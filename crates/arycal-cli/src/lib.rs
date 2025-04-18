pub mod input;
pub mod output;

#[cfg(feature = "mpi")]
use mpi::traits::*;

use anyhow::Result;
use log::info;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;
use std::time::Instant;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::alloc;
use cap::Cap;
use sysinfo::System;

use arycal_cloudpath::{
    tsv::load_precursor_ids_from_tsv,
    osw::{OswAccess, PrecursorIdData},
    sqmass::SqMassAccess,
};
use arycal_common::{logging::Progress, chromatogram::{Chromatogram, create_common_rt_space, AlignedChromatogram}, AlignedTransitionScores, error::ArycalError, PrecursorXics, AlignedTics, FullTraceAlignmentScores, PeakMapping, PrecursorAlignmentResult, PeakMappingScores};
use arycal_core::{alignment::{self, alignment::apply_post_alignment_to_trgrp, dynamic_time_warping::align_chromatograms, fast_fourier_lag::shift_chromatogram}, scoring::{compute_alignment_scores, compute_peak_mapping_scores, compute_peak_mapping_transitions_scores}};
use arycal_core::{
    alignment::alignment::map_peaks_across_runs,
    alignment::dynamic_time_warping::{star_align_tics, mst_align_tics, progressive_align_tics},
    alignment::fast_fourier_lag::{star_align_tics_fft, mst_align_tics_fft, progressive_align_tics_fft},
    alignment::fast_fourier_lag_dtw::star_align_tics_fft_with_local_refinement,
    scoring::{create_decoy_peaks_by_random_regions, create_decoy_peaks_by_shuffling},
};
use input::Input; 

pub struct Runner {
    precursor_map: Vec<PrecursorIdData>,
    parameters: input::Input,
    feature_access: Vec<OswAccess>,
    xic_access: Vec<SqMassAccess>,
    start: Instant,
    progress_num: Option<Arc<Mutex<f32>>>, 
}

impl Runner {
    pub fn new(parameters: Input, progress_num: Option<Arc<Mutex<f32>>>) -> anyhow::Result<Self> {
        let start = Instant::now();

        // TODO: Currently only supports a single OSW file
        let start_io = Instant::now();
        let osw_access = OswAccess::new(&parameters.features.file_paths[0].to_str().unwrap())?;

        // Check if precursor_ids tsv file is provided
        let mut precursor_ids: Option<Vec<u32>> = None;
        if let Some(precursor_ids_file) = &parameters.filters.precursor_ids {
            precursor_ids = Some(load_precursor_ids_from_tsv(precursor_ids_file)?);
        }

        let precursor_map: Vec<PrecursorIdData> = osw_access.fetch_transition_ids(parameters.filters.decoy, parameters.filters.include_identifying_transitions.unwrap_or_default(), precursor_ids)?;
        let run_time = (Instant::now() - start_io).as_millis();

        info!(
            "Loaded {} target precursors and {} decoy precursors identifiers - took {}ms",
            precursor_map.iter().filter(|v| !v.decoy).count(),
            precursor_map.iter().filter(|v| v.decoy).count(),
            run_time
        );

        let xic_accessors: Result<Vec<SqMassAccess>, anyhow::Error> = parameters
            .xic
            .file_paths
            .iter()
            .map(|path| SqMassAccess::new(path.to_str().unwrap()).map_err(anyhow::Error::from))
            .collect();

        Ok(Self {
            precursor_map: precursor_map.clone(),
            parameters,
            feature_access: vec![osw_access],
            xic_access: xic_accessors?,
            start,
            progress_num: Some(progress_num.expect("Progress number is not set")),
        })
    }

    #[cfg(not(feature = "mpi"))]
    pub fn run(&mut self) -> anyhow::Result<()> {
        #[global_allocator]
        static ALLOCATOR: Cap<alloc::System> = Cap::new(alloc::System, usize::max_value());

        let mut system = sysinfo::System::new_all();
        system.refresh_all();
        let total_memory = system.total_memory();
        let starting_memory = system.used_memory();

        log::debug!("{}", self.parameters.alignment);
    
        let precursor_map = &self.precursor_map;
        let total_count = precursor_map.len();
        let precursor_threads = self.parameters.alignment.precursor_threads.unwrap_or(rayon::max_num_threads() - 4);
        let batch_size = self.parameters.alignment.batch_size.unwrap_or(500);
        let global_start = Instant::now();
    
        // log::info!("Will align {} precursors in parallel at a time", precursor_threads);
        log::info!("Starting alignment for {} precursors", total_count);
    
        let feature_access = if let Some(scores_output_file) = &self.parameters.alignment.scores_output_file {
            let scores_output_file = scores_output_file.clone();
            let osw_access = OswAccess::new(&scores_output_file)?;
            vec![osw_access]
        } else {
            self.feature_access.clone()
        };

        // Initialize writers if they don't exist or drop them if they do
        if self.parameters.alignment.compute_scores.unwrap_or_default() {
            for osw_access in &feature_access {
                osw_access.create_feature_alignment_table()?;
            }
        }

        for osw_access in &feature_access {
            osw_access.create_feature_ms2_alignment_table()?;
        }

        if self.parameters.filters.include_identifying_transitions.unwrap_or_default() && self.parameters.alignment.compute_scores.unwrap_or_default() {
            for osw_access in &feature_access {
                osw_access.create_feature_transition_alignment_table()?;
            }
        }
    
        let mut start_idx = 0;
        while start_idx < total_count {
            let end_idx = (start_idx + batch_size).min(total_count);
            let batch_start_time = Instant::now();
    
            // Slice the batch
            let batch = &precursor_map[start_idx..end_idx];
    
            // let results: Vec<_> = rayon::ThreadPoolBuilder::new()
            //     .num_threads(precursor_threads)
            //     .build()?
            //     .install(|| {
            //         batch.par_iter()
            //             .map(|precursor| {
            //                 self.process_precursor(precursor)
            //                     .map_err(|e| ArycalError::Custom(e.to_string()))
            //             })
            //             .collect()
            //     });

            // Step 1: Extract all XICs for the batch
            let start_time = Instant::now();
            let xics_batch = self.prepare_xics_batch(batch)?;
            log::debug!("XIC extraction for batch of {} precursors took: {:?}", batch.len(), start_time.elapsed());

            // Step 2: Align all TICs for the batch
            let start_time = Instant::now();
            let aligned_batch = self.align_tics_batch(&xics_batch)?;
            log::debug!("TIC alignment for batch of {} precursors took: {:?}", batch.len(), start_time.elapsed());

            // Step 3: Process all peak mappings for the batch
            let start_time = Instant::now();
            let results: HashMap<i32, PrecursorAlignmentResult> = self.process_peak_mappings_batch(&aligned_batch, batch)?;
            log::debug!("Peak mapping and scoring for batch of {} precursors took: {:?}", batch.len(), start_time.elapsed());
    
            // Write results for this batch
            if self.parameters.alignment.compute_scores.unwrap_or_default() {
                self.write_aligned_score_results_to_db(&feature_access, &results)?;
            }
    
            self.write_ms2_alignment_results_to_db(&feature_access, &results)?;
    
            if self.parameters.filters.include_identifying_transitions.unwrap_or_default()
                && self.parameters.alignment.compute_scores.unwrap_or_default() {
                self.write_transition_alignment_results_to_db(&feature_access, &results)?;
            }
    
            let elapsed = batch_start_time.elapsed();
            log::info!(
                "Batch {}-{} processed in {:.2?} ({:.2}/min) - {} MiB / {} GiB",
                start_idx,
                end_idx,
                elapsed,
                ((end_idx - start_idx) as f64 / (elapsed.as_secs_f64() / 60.0)).floor(),
                ALLOCATOR.allocated() / 1024 / 1024,
                total_memory / 1024 / 1024 / 1024
            );
    
            start_idx += batch_size;
        }
    
        let total_elapsed = global_start.elapsed();
        log::info!(
            "Aligned and scored {} precursors in {:?} ({:.2}/sec)",
            total_count,
            total_elapsed,
            total_count as f64 / total_elapsed.as_secs_f64()
        );
    
        let run_time = (Instant::now() - self.start).as_secs();
        info!("finished in {}s", run_time);
        Ok(())
    }
    

    #[cfg(feature = "mpi")]
    pub fn run(&self) -> anyhow::Result<()> {
        // Initialize MPI
        let universe = mpi::initialize().unwrap();
        let world = universe.world();
        let rank = world.rank();  // Current process rank
        let size = world.size();  // Total number of processes

        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string());
        log::info!("MPI initialized: rank = {}, size = {}, hostname = {}", rank, size, hostname);

        // Determine chunking for distributing precursors
        let total_precursors = self.precursor_map.len();
        let chunk_size = (total_precursors + size as usize - 1) / size as usize; // Ensure even distribution
        let start = rank as usize * chunk_size;
        let end = std::cmp::min(start + chunk_size, total_precursors);

        let local_chunk = &self.precursor_map[start..end];
        log::info!(
            "Process {}: Processing chunk {} to {} ({} precursors)",
            rank, start, end, local_chunk.len()
        );

        // Parallel processing using Rayon within each node
        let local_results: Vec<Result<HashMap<i32, PrecursorAlignmentResult>, ArycalError>> = local_chunk
            .par_chunks(self.parameters.alignment.batch_size.unwrap_or_default())
            .map(|batch| {
                let mut batch_results = Vec::new();
                let mut progress = None;
                if !self.parameters.disable_progress_bar {
                    progress = Some(Progress::new(
                        batch.len(),
                        format!(
                            "[arycal] Node {} - Rank {} - Thread {:?} - Aligning precursors",
                            hostname,
                            rank,
                            rayon::current_thread_index().unwrap()
                        )
                        .as_str(),
                    ));
                }
                let start_time = Instant::now();
                for precursor in batch {
                    let result = self.process_precursor(precursor).map_err(|e| ArycalError::Custom(e.to_string()));
                    batch_results.push(result);
                    if !self.parameters.disable_progress_bar {
                        progress.as_ref().expect("The Progess tqdm logger is not enabled").inc();
                    }
                }
                let end_time = Instant::now();
                if self.parameters.disable_progress_bar {
                    log::info!("Node {} - Rank {} - Thread {:?} - Batch of {} precursors aligned in {:?}", hostname, rank, rayon::current_thread_index().unwrap_or(0), batch.len(), end_time.duration_since(start_time));
                }
                Ok(batch_results)
            })
            .collect::<Result<Vec<_>, ArycalError>>()?
            .into_iter()
            .flatten()
            .collect();

        // Serialize results
        let serialized_results = bincode::serialize(&local_results)?;

        // Gather results at root (rank 0)
        let gathered_results = if rank == 0 {
            let mut all_results = Vec::new();
            all_results.extend(local_results);

            for process_rank in 1..size {
                log::info!("Root process receiving results from process {}", process_rank);
                let (received_bytes, _status) = world.process_at_rank(process_rank).receive_vec::<u8>();
                let received_results: Vec<Result<HashMap<i32, PrecursorAlignmentResult>, ArycalError>> =
                    bincode::deserialize(&received_bytes)?;
                all_results.extend(received_results);
            }

            all_results
        } else {
            log::info!("Process {} sending results to root process", rank);
            world.process_at_rank(0).send(&serialized_results);
            Vec::new() // Non-root processes do not keep results
        };

        // Only root process writes results to the database
        if rank == 0 {

            // Write out to separate file if scores_output_file is provided
            let feature_access = if let Some(scores_output_file) = &self.parameters.alignment.scores_output_file {
                let scores_output_file = scores_output_file.clone();
                let osw_access = OswAccess::new(&scores_output_file)?;
                vec![osw_access]
            } else {
                self.feature_access.clone()
            };

            if self.parameters.alignment.compute_scores.unwrap_or_default() {
                // Write FEATURE_ALIGNMENT results to the database
                self.write_aligned_score_results_to_db(&feature_access, &gathered_results)?;
            }
            self.write_ms2_alignment_results_to_db(&feature_access, &gathered_results)?;

            if self.parameters.filters.include_identifying_transitions.unwrap_or_default() && self.parameters.alignment.compute_scores.unwrap_or_default() {
                self.write_transition_alignment_results_to_db(&feature_access, &gathered_results)?;   
            }

            let run_time = (Instant::now() - self.start).as_secs();
            info!("Alignment completed in {}s", run_time);
        }

        Ok(())
    }


    pub fn process_precursor(
        &self,
        precursor: &PrecursorIdData,
    ) -> Result<HashMap<i32, PrecursorAlignmentResult>, anyhow::Error> {
        let native_ids: Vec<String> = precursor.clone().extract_native_ids_for_sqmass(
            self.parameters.xic.include_precursor,
            self.parameters.xic.num_isotopes,
        );
        let native_ids_str: Vec<&str> = native_ids.iter().map(|s| s.as_str()).collect();

        log::trace!("modified_sequence: {:?}, precursor_charge: {:?}, detecting transitions: {:?}, identifying transitions: {:?}", precursor.modified_sequence, precursor.precursor_charge, precursor.n_transitions(), precursor.n_identifying_transitions());

        // log::trace!("native_ids: {:?}", native_ids);

        let group_id =
            precursor.modified_sequence.clone() + "_" + &precursor.precursor_charge.to_string();
    
        /* ------------------------------------------------------------------ */
        /* Step 1. Extract and transform XICs                                 */
        /* ------------------------------------------------------------------ */

        // Extract chromatograms from the XIC files
        let chromatograms: Vec<_> = self
            .xic_access
            .iter()
            .map(|access| {
                access.read_chromatograms("NATIVE_ID", native_ids_str.clone(), group_id.clone())
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Check length of the first chromatogram, should be at least more than 10 points
        if chromatograms[0]
            .chromatograms
            .iter()
            .map(|chromatogram| chromatogram.1.intensities.len())
            .sum::<usize>()
            < 10
        {
            log::trace!("The first chromatogram has less than 10 points, skipping precursor: {:?}", precursor.precursor_id);
            return Ok(HashMap::new());
        }

        // Check that there are no NaN values in the chromatograms
        for chrom in chromatograms.iter() {
            for (_, chrom_data) in chrom.chromatograms.iter() {
                if chrom_data.intensities.iter().any(|&x| x.is_nan()) || chrom_data.retention_times.iter().any(|&x| x.is_nan()) {
                    log::trace!("NaN values detected in chromatograms, skipping precursor: {:?}", precursor.precursor_id);
                    return Ok(HashMap::new());
                }
            }
        }

        // Compute TICs
        let tics: Vec<_> = chromatograms
            .iter()
            .map(|chromatogram| chromatogram.calculate_tic())
            .collect();

        // Create common retention time space
        let common_rt_space = create_common_rt_space(tics);
        // let common_rt_space = tics.clone();

        // Smooth and normalize TICs
        let smoothed_tics: Vec<_> = common_rt_space
            .iter()
            .map(|tic| {
                tic.smooth_sgolay(
                    self.parameters.alignment.smoothing.sgolay_window,
                    self.parameters.alignment.smoothing.sgolay_order,
                )?
                .normalize()
            })
            .collect::<Result<Vec<_>, _>>()?;

        /* ------------------------------------------------------------------ */
        /* Step 2. Pair-wise Alignment of TICs                                */
        /* ------------------------------------------------------------------ */

        log::debug!("Aligning TICs using {:?} using reference type: {:?}", self.parameters.alignment.method.as_str(), self.parameters.alignment.reference_type);
        let start_time = Instant::now();
        let aligned_chromatograms = match self.parameters.alignment.method.to_lowercase().as_str() {
            "dtw" => {
                match self.parameters.alignment.reference_type.as_str() {
                    "star" => star_align_tics(smoothed_tics.clone(), &self.parameters.alignment)?,
                    "mst" => mst_align_tics(smoothed_tics.clone())?,
                    "progressive" => progressive_align_tics(smoothed_tics.clone())?,
                    _ => star_align_tics(smoothed_tics.clone(), &self.parameters.alignment)?,
                }
            },
            "fft" => {
                match self.parameters.alignment.reference_type.as_str() {
                    "star" => star_align_tics_fft(smoothed_tics.clone(), &self.parameters.alignment)?,
                    "mst" => mst_align_tics_fft(smoothed_tics.clone())?,
                    "progressive" => progressive_align_tics_fft(smoothed_tics.clone())?,
                    _ => star_align_tics_fft(smoothed_tics.clone(), &self.parameters.alignment)?,
                }
            },
            "fftdtw" => star_align_tics_fft_with_local_refinement(smoothed_tics.clone(), &self.parameters.alignment)?,
            _ => star_align_tics(smoothed_tics.clone(), &self.parameters.alignment)?,
        };
        log::debug!("Alignment took: {:?}", start_time.elapsed());

        /* ------------------------------------------------------------------ */
        /* Step 3. Score Algined TICs                                         */
        /* ------------------------------------------------------------------ */
        let alignment_scores = if self.parameters.alignment.compute_scores.unwrap_or_default() {
            log::debug!("Computing full trace alignment scores");
            let start_time = Instant::now();
            let alignment_scores = compute_alignment_scores(aligned_chromatograms.clone());
            log::debug!("Scoring took: {:?}", start_time.elapsed());
            alignment_scores
        } else {
            HashMap::new()
        };

        // let output_path = "aligned_chromatograms.parquet";
        // println!("Writing aligned chromatograms to: {:?}", output_path);
        // output::write_aligned_chromatograms_to_parquet(&aligned_chromatograms.clone(), output_path)?;

        /* ------------------------------------------------------------------ */
        /* Step 4. Aligned Peak Mapping                                       */
        /* ------------------------------------------------------------------ */
        let start_time = Instant::now();
        // fetch feature data from the database
        // TODO: Currently only supports a single merged OSW file
        let prec_feat_data = self.feature_access[0]
            .fetch_full_precursor_feature_data_for_runs(
                precursor.precursor_id,
                common_rt_space
                    .clone()
                    .iter()
                    .map(|tic| tic.metadata.get("basename").unwrap().to_string())
                    .collect(),
            )?;

        // First collect all the mapping results in parallel
        let peak_mapping_results: Vec<(String, Vec<arycal_common::PeakMapping>)> = aligned_chromatograms
        .par_iter()
        .filter_map(|chrom| {
            let current_run = chrom.chromatogram.metadata.get("basename").unwrap();
            log::trace!(
                "Mapping peaks from reference run: {} to current run: {}",
                chrom.rt_mapping[0].get("run1").unwrap(),
                current_run
            );

            // Filter prec_feat_data for current run
            let current_run_feat_data: Vec<_> = prec_feat_data
                .iter()
                .filter(|f| &f.basename == current_run)
                .cloned()
                .collect();

            // Get reference run feature data
            let ref_run_feat_data: Vec<_> = prec_feat_data
                .iter()
                .filter(|f| &f.basename == chrom.rt_mapping[0].get("run1").unwrap())
                .cloned()
                .collect();

            if current_run_feat_data.is_empty() || ref_run_feat_data.is_empty() {
                log::trace!(
                    "Current run feature data or reference run feature data is empty, skipping peak mapping for run: {}",
                    current_run
                );
                return None;
            }

            let mapped_peaks = map_peaks_across_runs(
                chrom,
                ref_run_feat_data,
                current_run_feat_data,
                self.parameters.alignment.rt_mapping_tolerance.unwrap_or_default(),
                &self.parameters.alignment,
            );

            Some((
                chrom.chromatogram.metadata.get("basename").unwrap().to_string(),
                mapped_peaks,
            ))
        })
        .collect();

        // Then insert into HashMap serially
        let mut mapped_prec_peaks: HashMap<String, Vec<arycal_common::PeakMapping>> = HashMap::new();
        for (key, value) in peak_mapping_results {
            mapped_prec_peaks.insert(key, value);
        }

        log::debug!("Peak mapping took: {:?}", start_time.elapsed());

        /* ------------------------------------------------------------------ */
        /* Step 5. Score Aligned Peaks                                        */
        /* ------------------------------------------------------------------ */
        let (scored_peak_mappings, all_peak_mappings) = if self.parameters.alignment.compute_scores.unwrap_or_default() {
            log::debug!("Computing peak mapping scores");
            let start_time = Instant::now();
            let scored_peak_mappings =
                compute_peak_mapping_scores(aligned_chromatograms.clone(), mapped_prec_peaks.clone());

            // Create decoy aligned peaks based on the method specified in the parameters
            let mut decoy_peak_mappings: HashMap<String, Vec<PeakMapping>> = HashMap::new();
            if self.parameters.alignment.decoy_peak_mapping_method == "shuffle" {
                log::debug!("Creating decoy peaks by shuffling query peaks");
                decoy_peak_mappings = create_decoy_peaks_by_shuffling(&mapped_prec_peaks.clone());
            } else if self.parameters.alignment.decoy_peak_mapping_method == "random_regions" {
                log::debug!("Creating decoy peaks by picking random regions in the query XIC");
                decoy_peak_mappings = create_decoy_peaks_by_random_regions(&aligned_chromatograms.clone(), &mapped_prec_peaks.clone(), self.parameters.alignment.decoy_window_size.unwrap_or_default());
            }
            log::debug!("Computing peak mapping scores for decoy peaks");
            let scored_decoy_peak_mappings =
                compute_peak_mapping_scores(aligned_chromatograms.clone(), decoy_peak_mappings.clone());

            // Combine true and decoy peaks for analysis into HashMap<String, Vec<PeakMapping>>
            let all_peak_mappings: HashMap<String, Vec<PeakMapping>> = {
                let mut all_peak_mappings = HashMap::new();
                for (key, value) in scored_peak_mappings
                    .iter()
                    .chain(scored_decoy_peak_mappings.iter())
                {
                    all_peak_mappings
                        .entry(key.clone())
                        .or_insert_with(Vec::new)
                        .extend(value.clone());
                }
                all_peak_mappings
            };
            log::debug!("Peak mapping scoring took: {:?}", start_time.elapsed());
            (scored_peak_mappings, all_peak_mappings)
        } else {
            (HashMap::new(), mapped_prec_peaks)
        };

        /* ------------------------------------------------------------------ */
        /* Step 6. Optional Step: Align and Score Identifying Transitions     */
        /* ------------------------------------------------------------------ */
        let identifying_peak_mapping_scores: HashMap<String, Vec<AlignedTransitionScores>> = if self.parameters.filters.include_identifying_transitions.unwrap_or_default() && self.parameters.alignment.compute_scores.unwrap_or_default() {
            let start_time = Instant::now();
            log::debug!("Processing identifying transitions - aligning and scoring");
            let id_peak_scores = self.process_identifying_transitions(group_id.clone(), precursor, aligned_chromatograms.clone(), scored_peak_mappings.clone(), smoothed_tics[0].retention_times.clone());
            log::debug!("Identifying peak mapping scoring took: {:?}", start_time.elapsed());
            id_peak_scores
        } else {
            HashMap::new()
        };
        
        // output::write_mapped_peaks_to_parquet(all_peak_mappings, "mapped_peaks.parquet")?;

        let mut result = HashMap::new();
        result.insert(precursor.precursor_id.clone(), PrecursorAlignmentResult{
            alignment_scores,
            detecting_peak_mappings: all_peak_mappings,
            identifying_peak_mapping_scores,
        });

        Ok(result)
    }

    fn process_identifying_transitions(
        &self,
        group_id: String,
        precursor: &PrecursorIdData,
        aligned_chromatograms: Vec<AlignedChromatogram>,
        peak_mappings: HashMap<String, Vec<PeakMapping>>,
        common_rt_space: Vec<f64>
    ) -> HashMap<String, Vec<AlignedTransitionScores>> {
        // Extract identifying transition ids
        let identifying_transitions_ids: Vec<String> = precursor.clone().extract_identifying_native_ids_for_sqmass();
        let identifying_transitions_ids_str: Vec<&str> = identifying_transitions_ids.iter().map(|s| s.as_str()).collect();
        log::trace!("identifying_transitions_ids: {:?}", identifying_transitions_ids);

        // Extract chromatograms for identifying transitions
        let identifying_chromatograms: Vec<_> = self
            .xic_access
            .iter()
            .map(|access| {
                access.read_chromatograms("NATIVE_ID", identifying_transitions_ids_str.clone(), group_id.clone())
            })
            .collect::<Result<Vec<_>, _>>().unwrap_or(Vec::new());

        // Check if identifying_chromatograms is empty
        if identifying_chromatograms.is_empty() {
            log::trace!("Identifying chromatograms are empty");
            return HashMap::new();
        }
        
        // Score Identifying transitions
        let aligned_identifying_trgrps = apply_post_alignment_to_trgrp(identifying_chromatograms, aligned_chromatograms.clone(), common_rt_space, &self.parameters.alignment);

        let scored_aligned_identifying_transitions = compute_peak_mapping_transitions_scores(aligned_identifying_trgrps, aligned_chromatograms, peak_mappings);

        scored_aligned_identifying_transitions
    }

    fn align_precursor(
        &self,
        precursor: &PrecursorIdData,
    ) -> Result<Vec<AlignedChromatogram>, anyhow::Error> 
    {
        let native_ids: Vec<String> = precursor.clone().extract_native_ids_for_sqmass(
            self.parameters.xic.include_precursor,
            self.parameters.xic.num_isotopes,
        );
        let native_ids_str: Vec<&str> = native_ids.iter().map(|s| s.as_str()).collect();

        log::debug!("modified_sequence: {:?}, precursor_charge: {:?}, detecting transitions: {:?}, identifying transitions: {:?}", precursor.modified_sequence, precursor.precursor_charge, precursor.n_transitions(), precursor.n_identifying_transitions());

        log::trace!("native_ids: {:?}", native_ids);

        let group_id =
            precursor.modified_sequence.clone() + "_" + &precursor.precursor_charge.to_string();
    
        /* ------------------------------------------------------------------ */
        /* Step 1. Extract and transform XICs                                 */
        /* ------------------------------------------------------------------ */

        // Extract chromatograms from the XIC files
        let chromatograms: Vec<_> = self
            .xic_access
            .iter()
            .map(|access| {
                access.read_chromatograms("NATIVE_ID", native_ids_str.clone(), group_id.clone())
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Check length of the first chromatogram, should be at least more than 10 points
        if chromatograms[0]
            .chromatograms
            .iter()
            .map(|chromatogram| chromatogram.1.intensities.len())
            .sum::<usize>()
            < 10
        {
            return Ok(Vec::new());
        }

        // Check that there are no NaN values in the chromatograms
        for chrom in chromatograms.iter() {
            for (_, chrom_data) in chrom.chromatograms.iter() {
                if chrom_data.intensities.iter().any(|&x| x.is_nan()) || chrom_data.retention_times.iter().any(|&x| x.is_nan()) {
                    return Ok(Vec::new());
                }
            }
        }

        // Compute TICs
        let tics: Vec<_> = chromatograms
            .iter()
            .map(|chromatogram| chromatogram.calculate_tic())
            .collect();

        // Create common retention time space
        let common_rt_space = create_common_rt_space(tics);
        // let common_rt_space = tics.clone();

        // Smooth and normalize TICs
        let smoothed_tics: Vec<_> = common_rt_space
            .iter()
            .map(|tic| {
                tic.smooth_sgolay(
                    self.parameters.alignment.smoothing.sgolay_window,
                    self.parameters.alignment.smoothing.sgolay_order,
                )?
                .normalize()
            })
            .collect::<Result<Vec<_>, _>>()?;

        /* ------------------------------------------------------------------ */
        /* Step 2. Pair-wise Alignment of TICs                                */
        /* ------------------------------------------------------------------ */

        log::debug!("Aligning TICs using {:?} using reference type: {:?}", self.parameters.alignment.method.as_str(), self.parameters.alignment.reference_type);
        let aligned_chromatograms = match self.parameters.alignment.method.as_str() {
            "dtw" => {
                match self.parameters.alignment.reference_type.as_str() {
                    "star" => star_align_tics(smoothed_tics.clone(), &self.parameters.alignment)?,
                    "mst" => mst_align_tics(smoothed_tics.clone())?,
                    "progressive" => progressive_align_tics(smoothed_tics.clone())?,
                    _ => star_align_tics(smoothed_tics.clone(), &self.parameters.alignment)?,
                }
            },
            "fft" => {
                match self.parameters.alignment.reference_type.as_str() {
                    "star" => star_align_tics_fft(smoothed_tics.clone(), &self.parameters.alignment)?,
                    "mst" => mst_align_tics_fft(smoothed_tics.clone())?,
                    "progressive" => progressive_align_tics_fft(smoothed_tics.clone())?,
                    _ => star_align_tics_fft(smoothed_tics.clone(), &self.parameters.alignment)?,
                }
            },
            "fft_dtw" => star_align_tics_fft_with_local_refinement(smoothed_tics.clone(), &self.parameters.alignment)?,
            _ => star_align_tics(smoothed_tics.clone(), &self.parameters.alignment)?,
        };
        Ok(aligned_chromatograms)
    }


    // Extract XICs for a batch of precursors
    pub fn prepare_xics_batch(
        &self,
        precursors: &[PrecursorIdData],
    ) -> anyhow::Result<HashMap<i32, PrecursorXics>> {
        precursors
            .par_iter()
            .filter_map(|precursor| {
                match self.prepare_xics(precursor) {
                    Ok(xics) if xics != PrecursorXics::default() => {
                        Some(Ok((precursor.precursor_id, xics)))
                    }
                    Ok(_) => {
                        log::trace!("Skipping precursor {} due to empty/default XICs", precursor.precursor_id);
                        None
                    }
                    Err(e) => {
                        log::warn!("Error processing precursor {}: {}", precursor.precursor_id, e);
                        None
                    }
                }
            })
            .collect()
    }

    // Extract and process XICs for a single precursor
    fn prepare_xics(
        &self,
        precursor: &PrecursorIdData,
    ) -> anyhow::Result<PrecursorXics> {
        let native_ids = precursor.clone().extract_native_ids_for_sqmass(
            self.parameters.xic.include_precursor,
            self.parameters.xic.num_isotopes,
        );
        let native_ids_str: Vec<&str> = native_ids.iter().map(|s| s.as_str()).collect();
        let group_id = precursor.modified_sequence.clone() + "_" + &precursor.precursor_charge.to_string();

        // Extract chromatograms
        let start_time = Instant::now();
        let chromatograms: Vec<_> = self
            .xic_access
            .par_iter()
            .map(|access| {
                access.read_chromatograms("NATIVE_ID", native_ids_str.clone(), group_id.clone())
            })
            .collect::<Result<Vec<_>, _>>()?;
        log::trace!("XIC extraction took: {:?}", start_time.elapsed());

        // Validate chromatograms
        if chromatograms[0].chromatograms.iter().map(|c| c.1.intensities.len()).sum::<usize>() < 10 {
            log::trace!("The first chromatogram has less than 10 points, skipping precursor with: {:?}", precursor.precursor_id);
            return Ok(PrecursorXics::default());
        }

        for chrom in &chromatograms {
            for (_, data) in &chrom.chromatograms {
                if data.intensities.iter().any(|&x| x.is_nan()) || 
                   data.retention_times.iter().any(|&x| x.is_nan()) {
                    log::trace!("NaN values detected in chromatograms, skipping precursor with: {:?}", precursor.precursor_id);
                    return Ok(PrecursorXics::default());
                }
            }
        }

        // Process chromatograms
        let start_time = Instant::now();
        let tics: Vec<_> = chromatograms.par_iter().map(|c| c.calculate_tic()).collect();
        let common_rt_space = create_common_rt_space(tics);
        let smoothed_tics = common_rt_space
            .par_iter()
            .map(|tic| {
                tic.smooth_sgolay(
                    self.parameters.alignment.smoothing.sgolay_window,
                    self.parameters.alignment.smoothing.sgolay_order,
                )?
                .normalize()
            })
            .collect::<Result<Vec<_>, _>>()?;
        log::trace!("TIC processing took: {:?}", start_time.elapsed());

        Ok(PrecursorXics {
            precursor_id: precursor.precursor_id,
            smoothed_tics,
            common_rt_space: common_rt_space[0].retention_times.clone(),
            group_id,
            native_ids,
        })
    }

    // Align TICs for a batch of precursors
    pub fn align_tics_batch(
        &self,
        xics_batch: &HashMap<i32, PrecursorXics>,
    ) -> anyhow::Result<HashMap<i32, AlignedTics>> {
        xics_batch
            .par_iter()
            .map(|(precursor_id, xics)| {
                let aligned = self.align_tics(xics)?;
                Ok((*precursor_id, aligned))
            })
            .collect()
    }

    // Align TICs for a single precursor
    fn align_tics(
        &self,
        xics: &PrecursorXics,
    ) -> anyhow::Result<AlignedTics> {
        log::trace!("Aligning TICs using {:?} with reference type: {:?}", 
            self.parameters.alignment.method.as_str(), 
            self.parameters.alignment.reference_type);

        let aligned_chromatograms = match self.parameters.alignment.method.to_lowercase().as_str() {
            "dtw" => match self.parameters.alignment.reference_type.as_str() {
                "star" => star_align_tics(xics.smoothed_tics.clone(), &self.parameters.alignment)?,
                "mst" => mst_align_tics(xics.smoothed_tics.clone())?,
                "progressive" => progressive_align_tics(xics.smoothed_tics.clone())?,
                _ => star_align_tics(xics.smoothed_tics.clone(), &self.parameters.alignment)?,
            },
            "fft" => match self.parameters.alignment.reference_type.as_str() {
                "star" => star_align_tics_fft(xics.smoothed_tics.clone(), &self.parameters.alignment)?,
                "mst" => mst_align_tics_fft(xics.smoothed_tics.clone())?,
                "progressive" => progressive_align_tics_fft(xics.smoothed_tics.clone())?,
                _ => star_align_tics_fft(xics.smoothed_tics.clone(), &self.parameters.alignment)?,
            },
            "fftdtw" => star_align_tics_fft_with_local_refinement(xics.smoothed_tics.clone(), &self.parameters.alignment)?,
            _ => star_align_tics(xics.smoothed_tics.clone(), &self.parameters.alignment)?,
        };

        Ok(AlignedTics {
            precursor_id: xics.precursor_id,
            group_id: xics.group_id.clone(),
            common_rt_space: xics.common_rt_space.clone(),
            aligned_chromatograms,
        })
    }

    // Process peak mappings for a batch
    pub fn process_peak_mappings_batch(
        &self,
        aligned_batch: &HashMap<i32, AlignedTics>,
        precursors: &[PrecursorIdData],
    ) -> anyhow::Result<HashMap<i32, PrecursorAlignmentResult>> {
        // Create a lookup map from precursor_id to PrecursorIdData for efficient access
        let precursor_map: HashMap<i32, &PrecursorIdData> = precursors
            .iter()
            .map(|precursor| (precursor.precursor_id, precursor))
            .collect();
    
        aligned_batch
            .par_iter()
            .map(|(precursor_id, aligned)| {
                // Look up the precursor data
                let precursor = precursor_map.get(precursor_id)
                    .ok_or_else(|| anyhow::anyhow!("Precursor {} not found in batch", precursor_id))?;
    
                // Process with both aligned data and precursor info
                let mappings = self.process_peak_mappings(aligned, precursor)?;
                Ok((*precursor_id, mappings))
            })
            .collect()
    }

    // Process peak mappings for a single precursor
    fn process_peak_mappings(
        &self,
        aligned: &AlignedTics,
        precursor: &PrecursorIdData,
    ) -> anyhow::Result<PrecursorAlignmentResult> {
        // Fetch feature data
        let prec_feat_data = self.feature_access[0].fetch_full_precursor_feature_data_for_runs(
            aligned.precursor_id,
            aligned.aligned_chromatograms
                .clone()
                .iter()
                .map(|chrom| chrom.chromatogram.metadata.get("basename").unwrap().to_string())
                .collect(),
        )?;

        /* ------------------------------------------------------------------ */
        /* Aligned Peak Mapping                                       */
        /* ------------------------------------------------------------------ */
        let peak_mapping_results: Vec<_> = aligned.aligned_chromatograms
            .par_iter()
            .filter_map(|chrom| {
                let current_run = chrom.chromatogram.metadata.get("basename").unwrap();
                log::trace!(
                    "Mapping peaks from reference run: {} to current run: {}",
                    chrom.rt_mapping[0].get("run1").unwrap(),
                    current_run
                );

                // Filter prec_feat_data for current run
                let current_run_feat_data: Vec<_> = prec_feat_data
                    .iter()
                    .filter(|f| &f.basename == current_run)
                    .cloned()
                    .collect();

                // Get reference run feature data
                let ref_run_feat_data: Vec<_> = prec_feat_data
                    .iter()
                    .filter(|f| &f.basename == chrom.rt_mapping[0].get("run1").unwrap())
                    .cloned()
                    .collect();

                if current_run_feat_data.is_empty() || ref_run_feat_data.is_empty() {
                    log::trace!(
                        "Current run feature data or reference run feature data is empty, skipping peak mapping for run: {}",
                        current_run
                    );
                    return None;
                }

                let mapped_peaks = map_peaks_across_runs(
                    chrom,
                    ref_run_feat_data,
                    current_run_feat_data,
                    self.parameters.alignment.rt_mapping_tolerance.unwrap_or_default(),
                    &self.parameters.alignment,
                );

                Some((
                    chrom.chromatogram.metadata.get("basename").unwrap().to_string(),
                    mapped_peaks,
                ))
            })
            .collect();

        let mut mapped_prec_peaks = HashMap::new();
        for (key, value) in peak_mapping_results {
            mapped_prec_peaks.insert(key, value);
        }

        /* ------------------------------------------------------------------ */
        /* Score Algined TICs                                         */
        /* ------------------------------------------------------------------ */
        let alignment_scores = if self.parameters.alignment.compute_scores.unwrap_or_default() {
            compute_alignment_scores(aligned.aligned_chromatograms.clone())
        } else {
            HashMap::new()
        };

        /* ------------------------------------------------------------------ */
        /* Step 5. Score Aligned Peaks                                        */
        /* ------------------------------------------------------------------ */
        let (scored_peak_mappings, all_peak_mappings) = if self.parameters.alignment.compute_scores.unwrap_or_default() {
            log::trace!("Computing peak mapping scores");
            let start_time = Instant::now();
            let scored_peak_mappings =
                compute_peak_mapping_scores(aligned.aligned_chromatograms.clone(), mapped_prec_peaks.clone());

            // Create decoy aligned peaks based on the method specified in the parameters
            let mut decoy_peak_mappings: HashMap<String, Vec<PeakMapping>> = HashMap::new();
            if self.parameters.alignment.decoy_peak_mapping_method == "shuffle" {
                log::trace!("Creating decoy peaks by shuffling query peaks");
                decoy_peak_mappings = create_decoy_peaks_by_shuffling(&mapped_prec_peaks.clone());
            } else if self.parameters.alignment.decoy_peak_mapping_method == "random_regions" {
                log::trace!("Creating decoy peaks by picking random regions in the query XIC");
                decoy_peak_mappings = create_decoy_peaks_by_random_regions(&aligned.aligned_chromatograms.clone(), &mapped_prec_peaks.clone(), self.parameters.alignment.decoy_window_size.unwrap_or_default());
            }
            log::trace!("Computing peak mapping scores for decoy peaks");
            let scored_decoy_peak_mappings =
                compute_peak_mapping_scores(aligned.aligned_chromatograms.clone(), decoy_peak_mappings.clone());

            // Combine true and decoy peaks for analysis into HashMap<String, Vec<PeakMapping>>
            let all_peak_mappings: HashMap<String, Vec<PeakMapping>> = {
                let mut all_peak_mappings = HashMap::new();
                for (key, value) in scored_peak_mappings
                    .iter()
                    .chain(scored_decoy_peak_mappings.iter())
                {
                    all_peak_mappings
                        .entry(key.clone())
                        .or_insert_with(Vec::new)
                        .extend(value.clone());
                }
                all_peak_mappings
            };
            log::trace!("Peak mapping scoring took: {:?}", start_time.elapsed());
            (scored_peak_mappings, all_peak_mappings)
        } else {
            (HashMap::new(), mapped_prec_peaks)
        };

        /* ------------------------------------------------------------------ */
        /* Optional Step: Align and Score Identifying Transitions     */
        /* ------------------------------------------------------------------ */
        let identifying_transition_scores = if self.parameters.filters.include_identifying_transitions.unwrap_or_default() 
            && self.parameters.alignment.compute_scores.unwrap_or_default() {
                let start_time = Instant::now();
                log::trace!("Processing identifying transitions - aligning and scoring");
                let id_peak_scores = self.process_identifying_transitions(aligned.group_id.clone(), precursor, aligned.aligned_chromatograms.clone(), scored_peak_mappings.clone(), aligned.common_rt_space.clone());
                log::trace!("Identifying peak mapping scoring took: {:?}", start_time.elapsed());
                id_peak_scores
        } else {
            HashMap::new()
        };

        Ok(PrecursorAlignmentResult {
            alignment_scores: alignment_scores,
            detecting_peak_mappings: all_peak_mappings,
            identifying_peak_mapping_scores: identifying_transition_scores,
        })
    }

    fn write_aligned_score_results_to_db(
        &self,
        feature_access: &[OswAccess],
        results: &HashMap<i32, PrecursorAlignmentResult>,
    ) -> Result<()> {
        // Initialize progress bar if enabled
        let mut progress = if !self.parameters.disable_progress_bar {
            Some(Progress::new(
                results.len(),
                "[arycal] Writing FEATURE_ALIGNMENT table to the database",
            ))
        } else {
            None
        };
    
        // Collect all alignment scores from all precursors
        let mut all_scores = Vec::new();
        for alignment_result in results.values() {
            for run_scores in alignment_result.alignment_scores.values() {
                all_scores.push(run_scores);
            }
    
            // Update progress if enabled
            if let Some(pb) = &mut progress {
                pb.inc();
            }
        }
    
        // Write all scores to each database
        if !all_scores.is_empty() {
            for osw_access in feature_access {
                log::debug!(
                    "Inserting {} full trace aligned features and scores",
                    all_scores.len()
                );
                osw_access.insert_feature_alignment_batch(&all_scores)?;
            }
        }
    
        Ok(())
    }

    fn write_ms2_alignment_results_to_db(
        &self,
        feature_access: &[OswAccess],
        results: &HashMap<i32, PrecursorAlignmentResult>,
    ) -> Result<()> {
        // Initialize progress bar if enabled
        let mut progress = if !self.parameters.disable_progress_bar {
            Some(Progress::new(
                results.len(),
                "[arycal] Writing FEATURE_MS2_ALIGNMENT table to the database",
            ))
        } else {
            None
        };
    
        // Collect all MS2 alignment results
        let mut all_ms2_alignments = Vec::new();
        for alignment_result in results.values() {
            for run_alignments in alignment_result.detecting_peak_mappings.values() {
                all_ms2_alignments.extend(run_alignments.iter().cloned());
            }
    
            // Update progress if enabled
            if let Some(pb) = &mut progress {
                pb.inc();
            }
        }
    
        // Write all alignments to each database
        if !all_ms2_alignments.is_empty() {
            for osw_access in feature_access {
                log::debug!(
                    "Inserting {} MS2 aligned features",
                    all_ms2_alignments.len()
                );
                osw_access.insert_feature_ms2_alignment_batch(&all_ms2_alignments)?;
            }
        }
    
        Ok(())
    }

    fn write_transition_alignment_results_to_db(
        &self,
        feature_access: &[OswAccess],
        results: &HashMap<i32, PrecursorAlignmentResult>,
    ) -> Result<()> {
        // Initialize progress bar if enabled
        let mut progress = if !self.parameters.disable_progress_bar {
            Some(Progress::new(
                results.len(),
                "[arycal] Writing FEATURE_TRANSITION_ALIGNMENT table to the database",
            ))
        } else {
            None
        };
    
        // Collect all transition alignment results
        let mut all_transition_alignments = Vec::new();
        for alignment_result in results.values() {
            if let transition_scores = &alignment_result.identifying_peak_mapping_scores {
                for run_scores in transition_scores.values() {
                    all_transition_alignments.extend(run_scores.iter().cloned());
                }
            }
    
            // Update progress if enabled
            if let Some(pb) = &mut progress {
                pb.inc();
            }
        }
    
        // Write all transition alignments to each database
        if !all_transition_alignments.is_empty() {
            for osw_access in feature_access {
                log::debug!(
                    "Inserting {} transition aligned features",
                    all_transition_alignments.len()
                );
                osw_access.insert_feature_transition_alignment_batch(&all_transition_alignments)?;
            }
        }
    
        Ok(())
    }
    

}
