#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]

pub mod input;
pub mod output;

use arycal_cloudpath::sqmass::TransitionGroup;
#[cfg(feature = "mpi")]
use mpi::traits::*;

use anyhow::Result;
use log::info;
use rayon::prelude::*;
use std::collections::HashMap;
use std::time::Instant;
use sysinfo::System;
use deepsize::DeepSizeOf;

use arycal_cloudpath::{
    ChromatogramReader,
    tsv::load_precursor_ids_from_tsv,
    osw::{OswAccess, PrecursorIdData},
    oswpq::OswpqAccess,
    sqmass::SqMassAccess,
    xic_parquet::DuckDBParquetChromatogramReader
};
use arycal_common::{chromatogram::{create_common_rt_space, AlignedChromatogram}, AlignedTransitionScores, PrecursorXics, AlignedTics, PeakMapping, PrecursorAlignmentResult, config::FeaturesFileType};
use arycal_core::{alignment::alignment::apply_post_alignment_to_trgrp, scoring::{compute_alignment_scores, compute_peak_mapping_scores, compute_peak_mapping_transitions_scores}};
use arycal_core::{
    alignment::alignment::map_peaks_across_runs,
    alignment::dynamic_time_warping::{star_align_tics, mst_align_tics, progressive_align_tics},
    alignment::fast_fourier_lag::{star_align_tics_fft, mst_align_tics_fft, progressive_align_tics_fft},
    alignment::fast_fourier_lag_dtw::star_align_tics_fft_with_local_refinement,
    scoring::{create_decoy_peaks_by_random_regions, create_decoy_peaks_by_shuffling},
};
use arycal_cloudpath::osw::FeatureData;
use input::Input;

/// Enum to handle both OSW and OSWPQ feature accessors
pub enum FeatureAccessor {
    Osw(OswAccess),
    Oswpq(OswpqAccess),
}

impl FeatureAccessor {
    fn fetch_transition_ids(
        &self,
        filter_decoys: bool,
        include_identifying: bool,
        precursor_ids: Option<Vec<u32>>,
    ) -> anyhow::Result<Vec<PrecursorIdData>> {
        match self {
            FeatureAccessor::Osw(access) => {
                access.fetch_transition_ids(filter_decoys, include_identifying, precursor_ids)
                    .map_err(|e| anyhow::anyhow!(e))
            }
            FeatureAccessor::Oswpq(access) => {
                access.fetch_transition_ids(filter_decoys, include_identifying, precursor_ids)
                    .map_err(|e| anyhow::anyhow!(e))
            }
        }
    }

    fn fetch_feature_data_for_precursor_batch(
        &self,
        precursor_run_sets: &[(i32, Vec<String>)],
    ) -> anyhow::Result<HashMap<i32, Vec<FeatureData>>> {
        match self {
            FeatureAccessor::Osw(access) => {
                access.fetch_feature_data_for_precursor_batch(precursor_run_sets)
                    .map_err(|e| anyhow::anyhow!(e))
            }
            FeatureAccessor::Oswpq(access) => {
                access.fetch_feature_data_for_precursor_batch(precursor_run_sets)
                    .map_err(|e| anyhow::anyhow!(e))
            }
        }
    }

    fn fetch_full_precursor_feature_data_for_runs(
        &self,
        precursor_id: i32,
        runs: Vec<String>,
    ) -> anyhow::Result<Vec<FeatureData>> {
        match self {
            FeatureAccessor::Osw(access) => {
                access.fetch_full_precursor_feature_data_for_runs(precursor_id, runs)
                    .map_err(|e| anyhow::anyhow!(e))
            }
            FeatureAccessor::Oswpq(access) => {
                access.fetch_full_precursor_feature_data_for_runs(precursor_id, runs)
                    .map_err(|e| anyhow::anyhow!(e))
            }
        }
    }

    fn create_feature_ms2_alignment_table(&self) -> anyhow::Result<()> {
        match self {
            FeatureAccessor::Osw(access) => {
                access.create_feature_ms2_alignment_table()
                    .map_err(|e| anyhow::anyhow!(e))
            }
            FeatureAccessor::Oswpq(access) => {
                access.create_feature_ms2_alignment_table()
                    .map_err(|e| anyhow::anyhow!(e))
            }
        }
    }

    fn create_feature_transition_alignment_table(&self) -> anyhow::Result<()> {
        match self {
            FeatureAccessor::Osw(access) => {
                access.create_feature_transition_alignment_table()
                    .map_err(|e| anyhow::anyhow!(e))
            }
            FeatureAccessor::Oswpq(access) => {
                access.create_feature_transition_alignment_table()
                    .map_err(|e| anyhow::anyhow!(e))
            }
        }
    }

    fn insert_feature_ms2_alignment_batch(&self, peak_mappings: &[PeakMapping]) -> anyhow::Result<()> {
        match self {
            FeatureAccessor::Osw(access) => {
                access.insert_feature_ms2_alignment_batch(peak_mappings)
                    .map_err(|e| anyhow::anyhow!(e))
            }
            FeatureAccessor::Oswpq(access) => {
                access.write_ms2_alignment_batch(peak_mappings)
                    .map_err(|e| anyhow::anyhow!(e))
            }
        }
    }

    fn insert_feature_transition_alignment_batch(&self, transition_scores: &[AlignedTransitionScores]) -> anyhow::Result<()> {
        match self {
            FeatureAccessor::Osw(access) => {
                access.insert_feature_transition_alignment_batch(transition_scores)
                    .map_err(|e| anyhow::anyhow!(e))
            }
            FeatureAccessor::Oswpq(access) => {
                access.write_transition_alignment_batch(transition_scores)
                    .map_err(|e| anyhow::anyhow!(e))
            }
        }
    }
}

pub struct Runner {
    precursor_map: Vec<PrecursorIdData>,
    parameters: input::Input,
    feature_access: Vec<FeatureAccessor>,
    xic_access: Vec<Box<dyn ChromatogramReader>>,
    start: Instant
}

impl Runner {
    pub fn new(parameters: Input) -> anyhow::Result<Self> {
        let start = Instant::now();

        // Determine the feature file type
        let feature_file_type = parameters.features.file_type.clone()
            .unwrap_or(FeaturesFileType::OSW);
        
        log::info!("Using feature file type: {:?}", feature_file_type);

        // TODO: Currently only supports a single feature file
        let start_io = Instant::now();
        let feature_accessor = match feature_file_type {
            FeaturesFileType::OSW => {
                log::info!("Loading OSW file: {:?}", parameters.features.file_paths[0]);
                let osw_access = OswAccess::new(&parameters.features.file_paths[0].to_str().unwrap(), true)?;
                FeatureAccessor::Osw(osw_access)
            },
            FeaturesFileType::OSWPQ => {
                log::info!("Loading OSWPQ directory: {:?}", parameters.features.file_paths[0]);
                let oswpq_access = OswpqAccess::new(&parameters.features.file_paths[0])?;
                FeatureAccessor::Oswpq(oswpq_access)
            },
            FeaturesFileType::Unknown => {
                return Err(anyhow::anyhow!("Unknown feature file type"));
            }
        };

        // Check if precursor_ids tsv file is provided
        let mut precursor_ids: Option<Vec<u32>> = None;
        if let Some(precursor_ids_file) = &parameters.filters.precursor_ids {
            precursor_ids = Some(load_precursor_ids_from_tsv(precursor_ids_file)?);
        }

        // filter_decoys parameter in fetch_transition_ids: when true, EXCLUDES decoys (only processes targets)
        // include_decoys config field: when true, INCLUDES decoys (processes both targets and decoys)
        // So: filter_decoys = !include_decoys
        let filter_decoys = !parameters.filters.include_decoys;
        
        let precursor_map: Vec<PrecursorIdData> = feature_accessor.fetch_transition_ids(
            filter_decoys, 
            parameters.filters.include_identifying_transitions.unwrap_or_default(), 
            precursor_ids
        )?;
        let run_time = (Instant::now() - start_io).as_millis();

        info!(
            "Loaded {} target precursors and {} decoy precursors identifiers - took {}ms ({} MiB)",
            precursor_map.iter().filter(|v| !v.decoy).count(),
            precursor_map.iter().filter(|v| v.decoy).count(),
            run_time,
            precursor_map.iter().map(|v| v.deep_size_of()).sum::<usize>() / 1024 / 1024
        );

        let start_io = Instant::now();
        let xic_accessors: Vec<Box<dyn ChromatogramReader>> = parameters
            .xic
            .file_paths
            .par_iter()
            .with_max_len(15)
            .map(|path| {
                match parameters.xic.file_type.clone().unwrap().as_str().to_lowercase().as_str() {
                    "sqmass" => SqMassAccess::new(path.to_str().unwrap())
                        .map(|r| Box::new(r) as Box<dyn ChromatogramReader>)
                        .map_err(|e| anyhow::anyhow!(e)),
                    "parquet" => DuckDBParquetChromatogramReader::new(path.to_str().unwrap())
                        .map(|r| Box::new(r) as Box<dyn ChromatogramReader>)
                        .map_err(|e| anyhow::anyhow!(e)),
                    _ => Err(anyhow::anyhow!("Unsupported XIC file type: {:?}", parameters.xic.file_type.clone().unwrap().as_str())),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        // log::trace!("Creating {:?} XIC file accessors took: {:?}", &parameters.xic.file_paths.len(), start_io.elapsed());

        Ok(Self {
            precursor_map: precursor_map.clone(),
            parameters,
            feature_access: vec![feature_accessor],
            xic_access: xic_accessors,
            start
        })
    }

    #[cfg(not(feature = "mpi"))]
    pub fn run(&mut self) -> anyhow::Result<()> {
        let mut system = sysinfo::System::new_all();
        system.refresh_all();
        let total_memory = system.total_memory();
        let starting_memory = system.used_memory();

        log::debug!("System name:             {:?}", System::name());
        log::debug!("System kernel version:   {:?}", System::kernel_version());
        log::debug!("System OS version:       {:?}", System::os_version());
        log::debug!("System host name:        {:?}", System::host_name());

        log::info!("Total memory: {} GiB", total_memory / 1024 / 1024 / 1024);
        log::info!("Used memory: {} GiB", starting_memory / 1024 / 1024 / 1024);
        log::debug!("Total swap: {} GiB", system.total_swap() / 1024 / 1024 / 1024);
        log::debug!("Used swap: {} GiB", system.used_swap() / 1024 / 1024 / 1024);
        log::info!("System CPU count: {}", system.cpus().len());
    
        let precursor_map = &self.precursor_map;
        let total_count = precursor_map.len();
        let batch_size = self.parameters.alignment.batch_size.unwrap_or(500);
        let global_start = Instant::now();
    
        log::info!("Starting alignment for {} precursors", total_count);
    
        let _separate_output_accessor: Option<FeatureAccessor>;
        let feature_access: &[FeatureAccessor] = if let Some(scores_output_file) = &self.parameters.alignment.scores_output_file {
            let scores_output_file = scores_output_file.clone();
            // Check file extension to determine type
            let accessor = if scores_output_file.ends_with(".oswpqd") || scores_output_file.ends_with(".oswpq") {
                log::info!("Creating separate OSWPQ output: {}", scores_output_file);
                let oswpq_access = OswpqAccess::new(&scores_output_file)?;
                FeatureAccessor::Oswpq(oswpq_access)
            } else {
                log::info!("Creating separate OSW output: {}", scores_output_file);
                let osw_access = OswAccess::new(&scores_output_file, false)?;
                FeatureAccessor::Osw(osw_access)
            };
            _separate_output_accessor = Some(accessor);
            std::slice::from_ref(_separate_output_accessor.as_ref().unwrap())
        } else {
            &self.feature_access
        };

        // Initialize writers if they don't exist or drop them if they do
        // if self.parameters.alignment.compute_scores.unwrap_or_default() {
        //     for osw_access in &feature_access {
        //         osw_access.create_feature_alignment_table()?;
        //     }
        // }

        for accessor in feature_access {
            accessor.create_feature_ms2_alignment_table()?;
        }

        if self.parameters.filters.include_identifying_transitions.unwrap_or_default() && self.parameters.alignment.compute_scores.unwrap_or_default() {
            for accessor in feature_access {
                accessor.create_feature_transition_alignment_table()?;
            }
        }
    
        let mut start_idx = 0;
        while start_idx < total_count {
            let end_idx = (start_idx + batch_size).min(total_count);
            let batch_start_time = Instant::now();
    
            // Slice the batch
            let batch = &precursor_map[start_idx..end_idx];

            // Step 1: Extract all XICs for the batch
            let start_time = Instant::now();
            let xics_batch = self.prepare_xics_batch(batch)?;
            log::debug!("XIC extraction for batch of {} precursors took: {:?} ({} MiB)", batch.len(), start_time.elapsed(), xics_batch.deep_size_of() / 1024 / 1024);
            let xic_batch_size = xics_batch.deep_size_of();

            // Step 2: Align all TICs for the batch
            let start_time = Instant::now();
            let aligned_batch = self.align_tics_batch(xics_batch)?;
            log::debug!("TIC alignment for batch of {} precursors took: {:?} ({} MiB)", batch.len(), start_time.elapsed(), aligned_batch.deep_size_of() / 1024 / 1024);
            let aligned_batch_size = aligned_batch.deep_size_of();

            // Step 3: Process all peak mappings for the batch
            let start_time = Instant::now();
            let results: HashMap<i32, PrecursorAlignmentResult> = self.process_peak_mappings_batch(aligned_batch, batch)?;
            log::debug!("Peak mapping and scoring for batch of {} precursors took: {:?} ({} MiB)", batch.len(), start_time.elapsed(), results.deep_size_of() / 1024 / 1024);
    
            // Write results for this batch
            // if self.parameters.alignment.compute_scores.unwrap_or_default() {
            //     self.write_aligned_score_results_to_db(&feature_access, &results)?;
            // }
    
            self.write_ms2_alignment_results_to_db(&feature_access, &results)?;
    
            if self.parameters.filters.include_identifying_transitions.unwrap_or_default()
                && self.parameters.alignment.compute_scores.unwrap_or_default() {
                self.write_transition_alignment_results_to_db(&feature_access, &results)?;
            }
    
            let elapsed = batch_start_time.elapsed();
            log::info!(
                "Batch {}-{} processed in {:.2?} ({:.2}/min) - {} MiB ({}%)",
                start_idx,
                end_idx,
                elapsed,
                ((end_idx - start_idx) as f64 / (elapsed.as_secs_f64() / 60.0)).floor(),
                // Add up the size of xics_batch + aligned_batch + results
                ( xic_batch_size + aligned_batch_size + results.deep_size_of() ) / 1024 / 1024,
                ( (xic_batch_size + aligned_batch_size + results.deep_size_of()) as f64 / total_memory as f64 * 100.0
                ).floor()
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
    pub fn run(&mut self) -> anyhow::Result<()> {
        // Initialize MPI
        let universe = mpi::initialize().unwrap();
        let world = universe.world();
        let rank = world.rank();
        let size = world.size();
    
        // Only rank 0 logs system info
        if rank == 0 {
            let mut system = sysinfo::System::new_all();
            system.refresh_all();
            let total_memory = system.total_memory();
            let starting_memory = system.used_memory();
    
            log::debug!("System name:             {:?}", System::name());
            log::debug!("System kernel version:   {:?}", System::kernel_version());
            log::debug!("System OS version:       {:?}", System::os_version());
            log::debug!("System host name:        {:?}", System::host_name());
    
            log::info!("Total memory: {} GiB", total_memory / 1024 / 1024 / 1024);
            log::info!("Used memory: {} GiB", starting_memory / 1024 / 1024 / 1024);
            log::debug!("Total swap: {} GiB", system.total_swap() / 1024 / 1024 / 1024);
            log::debug!("Used swap: {} GiB", system.used_swap() / 1024 / 1024 / 1024);
            log::info!("System CPU count: {}", system.cpus().len());
        }
    
        let precursor_map = &self.precursor_map;
        let total_count = precursor_map.len();
        let batch_size = self.parameters.alignment.batch_size.unwrap_or(500);
        let global_start = Instant::now();
    
        if rank == 0 {
            log::info!("Starting alignment for {} precursors", total_count);
        }
    
        // Initialize feature access (only rank 0 creates tables)
        let feature_access = if rank == 0 {
            if let Some(scores_output_file) = &self.parameters.alignment.scores_output_file {
                let scores_output_file = scores_output_file.clone();
                let osw_access = OswAccess::new(&scores_output_file, false)?;
                vec![osw_access]
            } else {
                self.feature_access.clone()
            }
        } else {
            Vec::new() // Other ranks don't need this
        };
    
        // Only rank 0 creates tables
        if rank == 0 {
            for osw_access in &feature_access {
                osw_access.create_feature_ms2_alignment_table()?;
            }
    
            if self.parameters.filters.include_identifying_transitions.unwrap_or_default() 
                && self.parameters.alignment.compute_scores.unwrap_or_default() {
                for osw_access in &feature_access {
                    osw_access.create_feature_transition_alignment_table()?;
                }
            }
        }
    
        // Distribute work
        let mut start_idx = (rank as usize) * batch_size;
        while start_idx < total_count {
            let end_idx = (start_idx + batch_size).min(total_count);
            let batch_start_time = Instant::now();
            
            log::info!("Rank {} processing batch {}-{}", rank, start_idx, end_idx);
    
            // Slice the batch
            let batch = &precursor_map[start_idx..end_idx];
    
            // Process the batch
            let start_time = Instant::now();
            let xics_batch = self.prepare_xics_batch(batch)?;
            log::debug!("[Rank {}] XIC extraction for batch of {} precursors took: {:?} ({} MiB)", 
                rank, batch.len(), start_time.elapsed(), xics_batch.deep_size_of() / 1024 / 1024);
            let xic_batch_size = xics_batch.deep_size_of();
    
            let start_time = Instant::now();
            let aligned_batch = self.align_tics_batch(xics_batch)?;
            log::debug!("[Rank {}] TIC alignment for batch of {} precursors took: {:?} ({} MiB)", 
                rank, batch.len(), start_time.elapsed(), aligned_batch.deep_size_of() / 1024 / 1024);
            let aligned_batch_size = aligned_batch.deep_size_of();
    
            let start_time = Instant::now();
            let results: HashMap<i32, PrecursorAlignmentResult> = self.process_peak_mappings_batch(aligned_batch, batch)?;
            log::debug!("[Rank {}] Peak mapping and scoring for batch of {} precursors took: {:?} ({} MiB)", 
                rank, batch.len(), start_time.elapsed(), results.deep_size_of() / 1024 / 1024);
    
            // Write results (each rank writes its own part)
            // You might want to coordinate this to avoid conflicts
            self.write_ms2_alignment_results_to_db(&feature_access, &results)?;
    
            if self.parameters.filters.include_identifying_transitions.unwrap_or_default()
                && self.parameters.alignment.compute_scores.unwrap_or_default() {
                self.write_transition_alignment_results_to_db(&feature_access, &results)?;
            }
    
            let elapsed = batch_start_time.elapsed();
            log::info!(
                "[Rank {}] Batch {}-{} processed in {:.2?} ({:.2}/min) - {} MiB",
                rank,
                start_idx,
                end_idx,
                elapsed,
                ((end_idx - start_idx) as f64 / (elapsed.as_secs_f64() / 60.0)).floor(),
                (xic_batch_size + aligned_batch_size + results.deep_size_of()) / 1024 / 1024
            );
    
            start_idx += (size as usize) * batch_size; // Jump to next batch for this rank
        }
    
        // Synchronize and gather final stats if needed
        world.barrier();
    
        if rank == 0 {
            let total_elapsed = global_start.elapsed();
            log::info!(
                "Aligned and scored {} precursors in {:?} ({:.2}/sec)",
                total_count,
                total_elapsed,
                total_count as f64 / total_elapsed.as_secs_f64()
            );
    
            let run_time = (Instant::now() - self.start).as_secs();
            info!("finished in {}s", run_time);
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
        let chromatograms: Vec<TransitionGroup> = self
            .xic_access
            .iter()
            .map(|access| {
                access.read_chromatograms("NATIVE_ID", native_ids_str.clone(), group_id.clone())
                    .map_err(|e| anyhow::anyhow!(e))  
            })
            .collect::<anyhow::Result<Vec<_>>>()?;    

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
                    "star" => star_align_tics(&smoothed_tics, &self.parameters.alignment)?,
                    "mst" => mst_align_tics(&smoothed_tics)?,
                    "progressive" => progressive_align_tics(&smoothed_tics)?,
                    _ => star_align_tics(&smoothed_tics, &self.parameters.alignment)?,
                }
            },
            "fft" => {
                match self.parameters.alignment.reference_type.as_str() {
                    "star" => star_align_tics_fft(&smoothed_tics, &self.parameters.alignment)?,
                    "mst" => mst_align_tics_fft(&smoothed_tics)?,
                    "progressive" => progressive_align_tics_fft(&smoothed_tics.clone())?,
                    _ => star_align_tics_fft(&smoothed_tics, &self.parameters.alignment)?,
                }
            },
            "fftdtw" => star_align_tics_fft_with_local_refinement(&smoothed_tics, &self.parameters.alignment)?,
            _ => star_align_tics(&smoothed_tics, &self.parameters.alignment)?,
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
                chrom.reference_basename,
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
                .filter(|f| f.basename == chrom.reference_basename)
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
                compute_peak_mapping_scores(&aligned_chromatograms, &mapped_prec_peaks);

            // Create decoy aligned peaks based on the method specified in the parameters
            let mut decoy_peak_mappings: HashMap<String, Vec<PeakMapping>> = HashMap::new();
            if self.parameters.alignment.decoy_peak_mapping_method == "shuffle" {
                log::debug!("Creating decoy peaks by shuffling query peaks");
                decoy_peak_mappings = create_decoy_peaks_by_shuffling(&mapped_prec_peaks);
            } else if self.parameters.alignment.decoy_peak_mapping_method == "random_regions" {
                log::debug!("Creating decoy peaks by picking random regions in the query XIC");
                decoy_peak_mappings = create_decoy_peaks_by_random_regions(&aligned_chromatograms, &mapped_prec_peaks, self.parameters.alignment.decoy_window_size.unwrap_or_default());
            }
            log::debug!("Computing peak mapping scores for decoy peaks");
            let scored_decoy_peak_mappings =
                compute_peak_mapping_scores(&aligned_chromatograms, &decoy_peak_mappings);

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
            let id_peak_scores = self.process_identifying_transitions(group_id.clone(), precursor, &aligned_chromatograms, &scored_peak_mappings, &smoothed_tics[0].retention_times);
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
        aligned_chromatograms: &Vec<AlignedChromatogram>,
        peak_mappings: &HashMap<String, Vec<PeakMapping>>,
        common_rt_space: &Vec<f64>
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
        let aligned_identifying_trgrps = apply_post_alignment_to_trgrp(identifying_chromatograms, &aligned_chromatograms, common_rt_space, &self.parameters.alignment);

        let scored_aligned_identifying_transitions = compute_peak_mapping_transitions_scores(aligned_identifying_trgrps, &aligned_chromatograms, &peak_mappings);

        scored_aligned_identifying_transitions
    }

    pub fn prepare_xics_batch(
        &self,
        precursors: &[PrecursorIdData],
    ) -> anyhow::Result<HashMap<i32, PrecursorXics>> {
        // First read all chromatograms for all precursors across all files
        let start_time = Instant::now();
        
        // Process each SQLite file in parallel
        let all_precursor_groups: Vec<HashMap<i32, TransitionGroup>> = self.xic_access
            .par_iter()
            .map(|access| {
                access.read_chromatograms_for_precursors(
                    precursors,
                    self.parameters.xic.include_precursor,
                    self.parameters.xic.num_isotopes,
                )
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        
        log::trace!("XIC extraction from all files took: {:?}", start_time.elapsed());
    
        // Then process each precursor's chromatograms
        let start_time = Instant::now();
        let xic_batch: Result<HashMap<_, _>, _> = precursors
            .par_iter()
            .filter_map(|precursor| {
                // Collect chromatograms for this precursor from all files
                let mut chromatograms = Vec::new();
                for precursor_groups in &all_precursor_groups {
                    if let Some(group) = precursor_groups.get(&precursor.precursor_id) {
                        chromatograms.push(group.clone());
                    }
                }
    
                if chromatograms.is_empty() {
                    log::trace!("No chromatograms found for precursor {}", precursor.precursor_id);
                    return None;
                }
    
                // Validate chromatograms
                if chromatograms[0].chromatograms.values()
                    .map(|c| c.intensities.len())
                    .sum::<usize>() < 10 
                {
                    log::trace!("Skipping precursor {} - insufficient points (xic has less 10 data points)", precursor.precursor_id);
                    return None;
                }
                
                // Check for NaN values
                for group in &chromatograms {
                    for data in group.chromatograms.values() {
                        if data.intensities.iter().any(|&x| x.is_nan()) || 
                           data.retention_times.iter().any(|&x| x.is_nan()) {
                            log::trace!("NaN values detected in chromatograms, skipping precursor with: {:?}", precursor.precursor_id);
                            return None;
                        }
                    }
                }
        
                // Process chromatograms
                let tics: Vec<_> = chromatograms.par_iter().map(|c| c.calculate_tic()).collect();
                let common_rt_space = create_common_rt_space(tics);
                
                // Handle smoothing errors gracefully
                let smoothed_tics = match common_rt_space
                    .par_iter()
                    .map(|tic| {
                        tic.smooth_sgolay(
                            self.parameters.alignment.smoothing.sgolay_window,
                            self.parameters.alignment.smoothing.sgolay_order,
                        )
                        .and_then(|t| t.normalize())
                    })
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(tics) => tics,
                    Err(e) => {
                        log::trace!(
                            "Smoothing failed for precursor {}: {}. Skipping.",
                            precursor.precursor_id,
                            e
                        );
                        return None;
                    }
                };
                
                Some(Ok((precursor.precursor_id, PrecursorXics {
                    precursor_id: precursor.precursor_id,
                    smoothed_tics,
                    common_rt_space: common_rt_space[0].retention_times.clone(),
                    group_id: precursor.modified_sequence.clone() + "_" + &precursor.precursor_charge.to_string(),
                    native_ids: precursor.extract_native_ids_for_sqmass(
                        self.parameters.xic.include_precursor,
                        self.parameters.xic.num_isotopes,
                    ),
                })))
            })
            .collect();
        
        log::trace!("TIC processing took: {:?}", start_time.elapsed());
        xic_batch
    }


    // Align TICs for a batch of precursors
    pub fn align_tics_batch(
        &self,
        xics_batch: HashMap<i32, PrecursorXics>,
    ) -> anyhow::Result<HashMap<i32, AlignedTics>> {
        xics_batch
            .into_par_iter()
            .map(|(precursor_id, xics)| {
                let start_time = Instant::now();
                let aligned = self.align_tics(xics)?;
                log::trace!("Alignment for precursor {} took: {:?} ({:?} MiB)", precursor_id, start_time.elapsed(), aligned.deep_size_of() / 1024 / 1024);
                Ok((precursor_id, aligned))
            })
            .collect()
    }

    // Align TICs for a single precursor
    fn align_tics(
        &self,
        xics: PrecursorXics,
    ) -> anyhow::Result<AlignedTics> {
        log::trace!("Aligning TICs using {:?} with reference type: {:?}", 
            self.parameters.alignment.method.as_str(), 
            self.parameters.alignment.reference_type);

        let aligned_chromatograms = match self.parameters.alignment.method.to_lowercase().as_str() {
            "dtw" => match self.parameters.alignment.reference_type.as_str() {
                "star" => star_align_tics(&xics.smoothed_tics, &self.parameters.alignment)?,
                "mst" => mst_align_tics(&xics.smoothed_tics)?,
                "progressive" => progressive_align_tics(&xics.smoothed_tics)?,
                _ => star_align_tics(&xics.smoothed_tics, &self.parameters.alignment)?,
            },
            "fft" => match self.parameters.alignment.reference_type.as_str() {
                "star" => star_align_tics_fft(&xics.smoothed_tics, &self.parameters.alignment)?,
                "mst" => mst_align_tics_fft(&xics.smoothed_tics)?,
                "progressive" => progressive_align_tics_fft(&xics.smoothed_tics)?,
                _ => star_align_tics_fft(&xics.smoothed_tics, &self.parameters.alignment)?,
            },
            "fftdtw" => star_align_tics_fft_with_local_refinement(&xics.smoothed_tics, &self.parameters.alignment)?,
            _ => star_align_tics(&xics.smoothed_tics, &self.parameters.alignment)?,
        };

        if aligned_chromatograms.is_empty() {
            log::trace!("There was not alignment for precursor {} for {} xics", xics.precursor_id, xics.smoothed_tics.len());
            return Ok(AlignedTics {
                precursor_id: xics.precursor_id,
                group_id: xics.group_id.clone(),
                common_rt_space: xics.common_rt_space.clone(),
                aligned_chromatograms: Vec::new(),
            });
        }

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
        aligned_batch: HashMap<i32, AlignedTics>,
        precursors: &[PrecursorIdData],
    ) -> anyhow::Result<HashMap<i32, PrecursorAlignmentResult>> {
        // Get all precursor IDs and their run sets
        let precursor_run_sets: Vec<(i32, Vec<String>)> = aligned_batch.iter()
            .map(|(precursor_id, aligned)| {
                let runs = aligned.aligned_chromatograms.iter()
                    .map(|chrom| chrom.chromatogram.metadata.get("basename").unwrap().to_string())
                    .collect();
                (*precursor_id, runs)
            })
            .collect();

        // Fetch all feature data in one batch
        let start_time = Instant::now();
        let all_feature_data = self.feature_access[0]
            .fetch_feature_data_for_precursor_batch(&precursor_run_sets)?;
        log::debug!("Fetching feature data for {:?} precursors for {:?} runs took: {:?} ({:?} MiB)", precursor_run_sets.len(), precursor_run_sets.iter().map(|(_, runs)| runs.len()).sum::<usize>(), start_time.elapsed(), all_feature_data.deep_size_of() / 1024 / 1024);
    
        // Create a lookup map from precursor_id to PrecursorIdData
        let precursor_map: HashMap<i32, &PrecursorIdData> = precursors
            .iter()
            .map(|precursor| (precursor.precursor_id, precursor))
            .collect();
    
        // Process each precursor with cached feature data
        aligned_batch
        .par_iter()
        .filter_map(|(precursor_id, aligned)| {
            let precursor = match precursor_map.get(precursor_id) {
                Some(p) => p,
                None => {
                    log::trace!("Precursor {} not found in batch", precursor_id);
                    return None;
                }
            };

            let feature_data = match all_feature_data.get(precursor_id) {
                Some(f) => f,
                None => {
                    log::trace!("Feature data not found for precursor {}", precursor_id);
                    return None;
                }
            };

            match self.process_peak_mappings(aligned, precursor, feature_data) {
                Ok(mappings) => Some(Ok((*precursor_id, mappings))),
                Err(e) => {
                    log::warn!("Failed to process peak mappings for precursor {}: {}", precursor_id, e);
                    None
                }
            }
        })
        .collect()
    }

    // Process peak mappings for a single precursor
    fn process_peak_mappings(
        &self,
        aligned: &AlignedTics,
        precursor: &PrecursorIdData,
        prec_feat_data: &[FeatureData],
    ) -> anyhow::Result<PrecursorAlignmentResult> {
        // log::trace!("Fetching feature data for precursor: {:?}", precursor.precursor_id);
        // // Fetch feature data
        // let prec_feat_data = self.feature_access[0].fetch_full_precursor_feature_data_for_runs(
        //     aligned.precursor_id,
        //     aligned.aligned_chromatograms
        //         .clone()
        //         .iter()
        //         .map(|chrom| chrom.chromatogram.metadata.get("basename").unwrap().to_string())
        //         .collect(),
        // )?;

        /* ------------------------------------------------------------------ */
        /* Aligned Peak Mapping                                       */
        /* ------------------------------------------------------------------ */
        // log::trace!("Mapping peaks for precursor: {:?} for {:?} aligned chromatograms", precursor.precursor_id, aligned.aligned_chromatograms.len());
        let peak_mapping_results: Vec<_> = aligned.aligned_chromatograms
            .par_iter()
            .filter_map(|chrom| {
                let current_run = chrom.chromatogram.metadata.get("basename").unwrap();

                // Check if current run is the reference run, if it is, skip mapping
                if chrom.reference_basename == *current_run {
                    log::trace!("Current run is the reference run, skipping peak mapping for run: {}", current_run);
                    return None;
                }

                // log::trace!(
                //     "Mapping peaks from reference run: {} to current run: {}",
                //     chrom.reference_basename,
                //     current_run
                // );

                // Filter prec_feat_data for current run
                let current_run_feat_data: Vec<_> = prec_feat_data
                    .iter()
                    .filter(|f| &f.basename == current_run)
                    .cloned()
                    .collect();

                // Get reference run feature data
                let ref_run_feat_data: Vec<_> = prec_feat_data
                    .iter()
                    .filter(|f| f.basename == chrom.reference_basename)
                    .cloned()
                    .collect();

                if current_run_feat_data.is_empty() || ref_run_feat_data.is_empty() {
                    log::trace!(
                        "Current run feature data or reference run feature data is empty, skipping peak mapping for run: {}",
                        current_run
                    );
                    return None;
                }

                let start_time = Instant::now();
                let mapped_peaks = map_peaks_across_runs(
                    chrom,
                    ref_run_feat_data,
                    current_run_feat_data,
                    self.parameters.alignment.rt_mapping_tolerance.unwrap_or_default(),
                    &self.parameters.alignment,
                );
                log::trace!("Peak mapping took: {:?}", start_time.elapsed());

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
        // let alignment_scores = if self.parameters.alignment.compute_scores.unwrap_or_default() {
        //     compute_alignment_scores(aligned.aligned_chromatograms.clone())
        // } else {
        //     HashMap::new()
        // };
        let alignment_scores  = HashMap::new();

        /* ------------------------------------------------------------------ */
        /* Step 5. Score Aligned Peaks                                        */
        /* ------------------------------------------------------------------ */
        let (scored_peak_mappings, all_peak_mappings) = if self.parameters.alignment.compute_scores.unwrap_or_default() {
            log::trace!("Computing peak mapping scores");
            let start_time = Instant::now();
            let scored_peak_mappings =
                compute_peak_mapping_scores(&aligned.aligned_chromatograms, &mapped_prec_peaks);

            // Create decoy aligned peaks based on the method specified in the parameters
            let mut decoy_peak_mappings: HashMap<String, Vec<PeakMapping>> = HashMap::new();
            if self.parameters.alignment.decoy_peak_mapping_method == "shuffle" {
                log::trace!("Creating decoy peaks by shuffling query peaks");
                decoy_peak_mappings = create_decoy_peaks_by_shuffling(&mapped_prec_peaks);
            } else if self.parameters.alignment.decoy_peak_mapping_method == "random_regions" {
                log::trace!("Creating decoy peaks by picking random regions in the query XIC");
                decoy_peak_mappings = create_decoy_peaks_by_random_regions(&aligned.aligned_chromatograms, &mapped_prec_peaks, self.parameters.alignment.decoy_window_size.unwrap_or_default());
            }
            log::trace!("Computing peak mapping scores for decoy peaks");
            let scored_decoy_peak_mappings =
                compute_peak_mapping_scores(&aligned.aligned_chromatograms, &decoy_peak_mappings);

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
                let id_peak_scores = self.process_identifying_transitions(aligned.group_id.clone(), precursor, &aligned.aligned_chromatograms, &scored_peak_mappings, &aligned.common_rt_space);
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
        // Collect all alignment scores from all precursors
        let mut all_scores = Vec::new();
        for alignment_result in results.values() {
            for run_scores in alignment_result.alignment_scores.values() {
                all_scores.push(run_scores);
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
        feature_access: &[FeatureAccessor],
        results: &HashMap<i32, PrecursorAlignmentResult>,
    ) -> Result<()> {
        // Collect all MS2 alignment results
        let mut all_ms2_alignments = Vec::new();
        for alignment_result in results.values() {
            for run_alignments in alignment_result.detecting_peak_mappings.values() {
                all_ms2_alignments.extend(run_alignments.iter().cloned());
            }
        }
    
        // Write all alignments to each database
        if !all_ms2_alignments.is_empty() {
            for accessor in feature_access {
                log::debug!(
                    "Inserting {} MS2 aligned features",
                    all_ms2_alignments.len()
                );
                accessor.insert_feature_ms2_alignment_batch(&all_ms2_alignments)?;
            }
        } else {
            log::warn!("No MS2 aligned features to write to the database");
        }
    
        Ok(())
    }

    fn write_transition_alignment_results_to_db(
        &self,
        feature_access: &[FeatureAccessor],
        results: &HashMap<i32, PrecursorAlignmentResult>,
    ) -> Result<()> {
        // Collect all transition alignment results
        let mut all_transition_alignments = Vec::new();
        for alignment_result in results.values() {
            let transition_scores = &alignment_result.identifying_peak_mapping_scores;
            for run_scores in transition_scores.values() {
                all_transition_alignments.extend(run_scores.iter().cloned());
            }
        }
    
        // Write all transition alignments to each database
        if !all_transition_alignments.is_empty() {
            for accessor in feature_access {
                log::debug!(
                    "Inserting {} transition aligned features",
                    all_transition_alignments.len()
                );
                accessor.insert_feature_transition_alignment_batch(&all_transition_alignments)?;
            }
        }
    
        Ok(())
    }
    

}
