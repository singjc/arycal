use ndarray::{Array1, Array2};
use rand::Rng;
use rayon::prelude::*;
use std::{collections::HashMap, sync::Arc};

use crate::{
    alignment::alignment::validate_widths,
    stats::{
        calc_mi_score, calc_mi_to_many_score, calc_xcorr_coelution_score, calc_xcorr_shape_score,
        calc_xcorr_shape_to_many_score, calc_xcorr_to_many_score,
    },
};
use arycal_cloudpath::{
    osw::{FeatureData, ValueEntryType},
    sqmass::TransitionGroup,
};
use arycal_common::{
    chromatogram::{AlignedChromatogram, Chromatogram},
    AlignedTransitionScores, FullTraceAlignmentScores, PeakMapping,
};

const STRATIFIED_RT_BINS: usize = 4;
const STRATIFIED_WIDTH_BINS: usize = 3;

#[derive(Debug, Clone)]
struct PeakTuple {
    aligned_feature_id: i64,
    aligned_rt: f64,
    aligned_left_width: f64,
    aligned_right_width: f64,
}

#[derive(Debug, Clone)]
struct FeatureCandidate {
    feature_id: i64,
    rt: f64,
    left_width: f64,
    right_width: f64,
    rank: Option<i32>,
    qvalue: Option<f64>,
}

fn stratified_shuffle_peak_tuples<R: Rng + ?Sized>(
    peak_tuples: &[PeakTuple],
    rng: &mut R,
) -> Vec<PeakTuple> {
    if peak_tuples.is_empty() {
        return Vec::new();
    }

    if peak_tuples.len() == 1 {
        return vec![synthesize_offset_peak_tuple(&peak_tuples[0], rng)];
    }

    let mut rt_order: Vec<usize> = (0..peak_tuples.len()).collect();
    rt_order.sort_by(|&a, &b| {
        peak_tuples[a]
            .aligned_rt
            .partial_cmp(&peak_tuples[b].aligned_rt)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut width_order: Vec<usize> = (0..peak_tuples.len()).collect();
    width_order.sort_by(|&a, &b| {
        peak_width_from_bounds(
            peak_tuples[a].aligned_left_width,
            peak_tuples[a].aligned_right_width,
        )
        .partial_cmp(&peak_width_from_bounds(
            peak_tuples[b].aligned_left_width,
            peak_tuples[b].aligned_right_width,
        ))
        .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut rt_ranks = vec![0usize; peak_tuples.len()];
    for (rank, idx) in rt_order.into_iter().enumerate() {
        rt_ranks[idx] = rank;
    }

    let mut width_ranks = vec![0usize; peak_tuples.len()];
    for (rank, idx) in width_order.into_iter().enumerate() {
        width_ranks[idx] = rank;
    }

    let mut stratified_groups: HashMap<(usize, usize), Vec<usize>> = HashMap::new();
    for idx in 0..peak_tuples.len() {
        let rt_bin = bucket_for_rank(rt_ranks[idx], peak_tuples.len(), STRATIFIED_RT_BINS);
        let width_bin = bucket_for_rank(width_ranks[idx], peak_tuples.len(), STRATIFIED_WIDTH_BINS);
        stratified_groups
            .entry((rt_bin, width_bin))
            .or_default()
            .push(idx);
    }

    let mut shuffled = vec![None; peak_tuples.len()];
    let mut leftovers = Vec::new();

    for indices in stratified_groups.values() {
        if indices.len() >= 2 {
            assign_deranged_peak_tuples(indices, peak_tuples, &mut shuffled, rng);
        } else {
            leftovers.extend(indices.iter().copied());
        }
    }

    if leftovers.len() >= 2 {
        assign_deranged_peak_tuples(&leftovers, peak_tuples, &mut shuffled, rng);
    }

    shuffled
        .into_iter()
        .enumerate()
        .map(|(idx, maybe_peak)| {
            maybe_peak.unwrap_or_else(|| synthesize_offset_peak_tuple(&peak_tuples[idx], rng))
        })
        .collect()
}

fn assign_deranged_peak_tuples<R: Rng + ?Sized>(
    indices: &[usize],
    peak_tuples: &[PeakTuple],
    shuffled: &mut [Option<PeakTuple>],
    rng: &mut R,
) {
    let Some(permutation) = sample_derangement(indices.len(), rng) else {
        return;
    };

    for (dest_pos, src_pos) in permutation.into_iter().enumerate() {
        shuffled[indices[dest_pos]] = Some(peak_tuples[indices[src_pos]].clone());
    }
}

fn sample_derangement<R: Rng + ?Sized>(len: usize, rng: &mut R) -> Option<Vec<usize>> {
    if len < 2 {
        return None;
    }

    let mut permutation: Vec<usize> = (0..len).collect();
    for i in (1..len).rev() {
        let j = rng.random_range(0..i);
        permutation.swap(i, j);
    }

    Some(permutation)
}

fn bucket_for_rank(rank: usize, total: usize, bins: usize) -> usize {
    if total <= 1 || bins <= 1 {
        0
    } else {
        ((rank * bins) / total).min(bins - 1)
    }
}

fn synthesize_offset_peak_tuple<R: Rng + ?Sized>(peak: &PeakTuple, rng: &mut R) -> PeakTuple {
    let peak_width = peak_width_from_bounds(peak.aligned_left_width, peak.aligned_right_width);
    let direction = if rng.random::<bool>() { 1.0 } else { -1.0 };
    let shift = direction * rng.random_range(2.0..4.0) * peak_width.max(1.0);

    let (aligned_left_width, aligned_right_width) = validate_widths(
        peak.aligned_left_width + shift,
        peak.aligned_right_width + shift,
    );

    PeakTuple {
        aligned_feature_id: -1,
        aligned_rt: peak.aligned_rt + shift,
        aligned_left_width,
        aligned_right_width,
    }
}

fn flatten_feature_candidates(feature: &FeatureData) -> Vec<FeatureCandidate> {
    let candidate_count = [
        Some(value_entry_len(&feature.exp_rt)),
        feature.feature_id.as_ref().map(value_entry_len),
        feature.left_width.as_ref().map(value_entry_len),
        feature.right_width.as_ref().map(value_entry_len),
        feature.rank.as_ref().map(value_entry_len),
        feature.qvalue.as_ref().map(value_entry_len),
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(0);

    (0..candidate_count)
        .filter_map(|idx| {
            let feature_id = optional_value_entry_at(feature.feature_id.as_ref(), idx)?;
            let rt = value_entry_at(&feature.exp_rt, idx)?;

            let default_half_width = 0.5;
            let left_width = optional_value_entry_at(feature.left_width.as_ref(), idx)
                .unwrap_or(rt - default_half_width);
            let right_width = optional_value_entry_at(feature.right_width.as_ref(), idx)
                .unwrap_or(rt + default_half_width);
            let (left_width, right_width) = validate_widths(left_width, right_width);

            Some(FeatureCandidate {
                feature_id,
                rt,
                left_width,
                right_width,
                rank: optional_value_entry_at(feature.rank.as_ref(), idx),
                qvalue: optional_value_entry_at(feature.qvalue.as_ref(), idx),
            })
        })
        .collect()
}

fn peak_width_from_bounds(left_width: f64, right_width: f64) -> f64 {
    let (left_width, right_width) = validate_widths(left_width, right_width);
    let width = (right_width - left_width).abs();
    if width.is_finite() && width > 0.0 {
        width
    } else {
        1.0
    }
}

fn value_entry_len<T>(entry: &ValueEntryType<T>) -> usize {
    match entry {
        ValueEntryType::Single(_) => 1,
        ValueEntryType::Multiple(values) => values.len(),
    }
}

fn value_entry_at<T: Clone>(entry: &ValueEntryType<T>, idx: usize) -> Option<T> {
    match entry {
        ValueEntryType::Single(value) => (idx == 0).then(|| value.clone()),
        ValueEntryType::Multiple(values) => values.get(idx).cloned(),
    }
}

fn optional_value_entry_at<T: Clone>(entry: Option<&ValueEntryType<T>>, idx: usize) -> Option<T> {
    entry.and_then(|entry| value_entry_at(entry, idx))
}

pub fn compute_alignment_scores(
    aligned_chromatograms: Vec<AlignedChromatogram>,
) -> HashMap<String, FullTraceAlignmentScores> {
    // Pre-convert all intensities to Array1 once
    let chrom_arrays: Vec<Array1<f64>> = aligned_chromatograms
        .iter()
        .map(|chrom| Array1::from(chrom.chromatogram.intensities.clone()))
        .collect();

    // Create lookup maps
    let chrom_lookup: HashMap<_, _> = aligned_chromatograms
        .iter()
        .map(|chrom| {
            (
                chrom
                    .chromatogram
                    .metadata
                    .get("basename")
                    .unwrap_or(&chrom.chromatogram.native_id)
                    .as_str(),
                chrom,
            )
        })
        .collect();

    // Process in parallel
    aligned_chromatograms
        .par_iter()
        .map(|aligned_chrom| {
            let aligned_filename = aligned_chrom
                .chromatogram
                .metadata
                .get("basename")
                .unwrap_or(&aligned_chrom.chromatogram.native_id)
                .to_string();

            let reference_filename = aligned_chrom.reference_basename.clone();

            let reference_chrom = match chrom_lookup.get(reference_filename.as_str()) {
                Some(c) => c,
                None => return (aligned_filename, None),
            };

            let aligned_array = Array1::from(aligned_chrom.chromatogram.intensities.clone());
            let reference_array = Array1::from(reference_chrom.chromatogram.intensities.clone());

            // Compute all scores
            let xcorr_coelution_to_ref =
                calc_xcorr_coelution_score(&reference_array, &aligned_array);

            let xcorr_shape_to_ref = calc_xcorr_shape_score(&reference_array, &aligned_array);

            let mi_to_ref = calc_mi_score(&reference_array, &aligned_array);

            // Convert chrom_arrays to slice for "to_all" functions
            let all_arrays_slice = &chrom_arrays;

            let xcorr_coelution_to_all = calc_xcorr_to_many_score(&aligned_array, all_arrays_slice);

            let xcorr_shape_to_all =
                calc_xcorr_shape_to_many_score(&aligned_array, all_arrays_slice);

            let mi_to_all = calc_mi_to_many_score(&aligned_array, all_arrays_slice);

            let alignment_score = FullTraceAlignmentScores {
                reference_filename: reference_filename.to_string(),
                aligned_filename: aligned_filename.clone(),
                xcorr_coelution_to_ref,
                xcorr_shape_to_ref,
                mi_to_ref,
                xcorr_coelution_to_all,
                xcorr_shape_to_all,
                mi_to_all,
            };

            (aligned_filename, Some(alignment_score))
        })
        .filter_map(|(k, v)| v.map(|score| (k, score)))
        .collect()
}

pub fn compute_peak_mapping_scores(
    aligned_chromatograms: &Vec<AlignedChromatogram>,
    peak_mappings: &HashMap<String, Vec<PeakMapping>>,
) -> HashMap<String, Vec<PeakMapping>> {
    // Create Arc wrappers for shared access
    let chroms_arc = Arc::new(aligned_chromatograms);
    let peak_maps_arc = Arc::new(peak_mappings);

    // Create lookup structure (using Arc reference)
    let chrom_lookup: HashMap<_, _> = chroms_arc
        .iter()
        .map(|c| {
            (
                c.chromatogram
                    .metadata
                    .get("basename")
                    .unwrap_or(&c.chromatogram.native_id)
                    .as_str(),
                c,
            )
        })
        .collect();

    // Process in parallel using Arc references
    peak_maps_arc
        .par_iter()
        .map(|(aligned_filename, peak_mappings_for_run)| {
            // Clone the Vec to mutate it
            let mut peak_mappings_for_run = peak_mappings_for_run.clone();

            let aligned_chrom = match chrom_lookup.get(aligned_filename.as_str()) {
                Some(c) => c,
                None => return (aligned_filename.clone(), peak_mappings_for_run),
            };

            // let reference_filename = match aligned_chrom.rt_mapping[0].get("run1") {
            //     Some(name) => name,
            //     None => return (aligned_filename.clone(), peak_mappings_for_run),
            // };

            let reference_filename = aligned_chrom.reference_basename.clone();

            let reference_chrom = match chrom_lookup.get(reference_filename.as_str()) {
                Some(c) => c,
                None => return (aligned_filename.clone(), peak_mappings_for_run),
            };

            for peak_mapping in &mut peak_mappings_for_run {
                // Get intensities once and reuse
                let reference_intensities = Array1::from(get_peak_intensities(
                    &reference_chrom.chromatogram,
                    peak_mapping.reference_left_width,
                    peak_mapping.reference_right_width,
                ));

                let aligned_intensities = Array1::from(get_peak_intensities(
                    &aligned_chrom.chromatogram,
                    peak_mapping.aligned_left_width,
                    peak_mapping.aligned_right_width,
                ));

                // Compute reference scores
                peak_mapping.xcorr_coelution_to_ref = Some(calc_xcorr_coelution_score(
                    &reference_intensities,
                    &aligned_intensities,
                ));

                peak_mapping.xcorr_shape_to_ref = Some(calc_xcorr_shape_score(
                    &reference_intensities,
                    &aligned_intensities,
                ));

                peak_mapping.mi_to_ref =
                    Some(calc_mi_score(&reference_intensities, &aligned_intensities));

                // Compute "to_all" scores using Arc references
                if let Some(all_intensities) = get_all_intensities_for_alignment(
                    &chroms_arc,
                    &peak_maps_arc,
                    peak_mapping.alignment_id.try_into().unwrap(),
                ) {
                    peak_mapping.xcorr_coelution_to_all = Some(calc_xcorr_to_many_score(
                        &aligned_intensities,
                        &all_intensities,
                    ));

                    peak_mapping.xcorr_shape_to_all = Some(calc_xcorr_shape_to_many_score(
                        &aligned_intensities,
                        &all_intensities,
                    ));

                    peak_mapping.mi_to_all = Some(calc_mi_to_many_score(
                        &aligned_intensities,
                        &all_intensities,
                    ));
                }

                peak_mapping.rt_deviation = Some(
                    (peak_mapping.aligned_rt
                        - peak_mapping
                            .mapped_target_rt
                            .unwrap_or(peak_mapping.reference_rt))
                    .abs(),
                );
                peak_mapping.intensity_ratio = Some(compute_peak_intensity_ratio(
                    &reference_intensities,
                    &aligned_intensities,
                ));
            }

            (aligned_filename.clone(), peak_mappings_for_run)
        })
        .collect()
}

pub fn compute_peak_mapping_transitions_scores(
    aligned_identifying_trgrps: Vec<TransitionGroup>,
    aligned_chromatograms: &Vec<AlignedChromatogram>,
    peak_mappings: &HashMap<String, Vec<PeakMapping>>,
) -> HashMap<String, Vec<AlignedTransitionScores>> {
    // Wrap data in Arc for shared access
    let chroms_arc = Arc::new(aligned_chromatograms);
    let peak_maps_arc = Arc::new(peak_mappings);

    // Create lookup maps
    let chrom_lookup: HashMap<_, _> = chroms_arc
        .iter()
        .map(|c| {
            (
                c.chromatogram
                    .metadata
                    .get("basename")
                    .unwrap_or(&c.chromatogram.native_id)
                    .as_str(),
                c,
            )
        })
        .collect();

    // Process transition groups in parallel
    aligned_identifying_trgrps
        .par_iter()
        .flat_map(|identifying_trgrp| {
            let current_filename = identifying_trgrp.metadata.get("basename").unwrap();

            // Get reference chromatogram once per group
            let (reference_chrom, peak_mappings_for_run) = {
                let current_chrom = match chrom_lookup.get(current_filename.as_str()) {
                    Some(c) => c,
                    None => return Vec::new(),
                };

                // let reference_filename = match current_chrom.rt_mapping[0].get("run1") {
                //     Some(name) => name,
                //     None => return Vec::new(),
                // };
                let reference_filename = current_chrom.reference_basename.clone();

                let reference_chrom = match chrom_lookup.get(reference_filename.as_str()) {
                    Some(c) => c,
                    None => return Vec::new(),
                };

                let peak_mappings_for_run = match peak_maps_arc.get(current_filename) {
                    Some(mappings) => mappings,
                    None => return Vec::new(),
                };

                (reference_chrom, peak_mappings_for_run)
            };

            // Process transitions in parallel
            identifying_trgrp
                .chromatograms
                .par_iter()
                .flat_map(|(transition_id, transition_chrom)| {
                    peak_mappings_for_run
                        .par_iter()
                        .map(|peak_mapping| {
                            // Get intensities once
                            let reference_intensities = Array1::from(get_peak_intensities(
                                &reference_chrom.chromatogram,
                                peak_mapping.reference_left_width,
                                peak_mapping.reference_right_width,
                            ));

                            let aligned_intensities = Array1::from(get_peak_intensities(
                                transition_chrom,
                                peak_mapping.aligned_left_width,
                                peak_mapping.aligned_right_width,
                            ));

                            // Compute reference scores
                            let xcorr_coelution_to_ref = calc_xcorr_coelution_score(
                                &reference_intensities,
                                &aligned_intensities,
                            );

                            let xcorr_shape_to_ref = calc_xcorr_shape_score(
                                &reference_intensities,
                                &aligned_intensities,
                            );

                            let mi_to_ref =
                                calc_mi_score(&reference_intensities, &aligned_intensities);

                            // Compute "to_all" scores
                            let all_intensities = get_all_intensities_for_alignment(
                                &chroms_arc,
                                &peak_maps_arc,
                                peak_mapping.alignment_id.clone(),
                            );

                            let xcorr_coelution_to_all = all_intensities
                                .as_ref()
                                .map(|ints| calc_xcorr_to_many_score(&aligned_intensities, ints))
                                .unwrap_or(0.0);

                            let xcorr_shape_to_all = all_intensities
                                .as_ref()
                                .map(|ints| {
                                    calc_xcorr_shape_to_many_score(&aligned_intensities, ints)
                                })
                                .unwrap_or(0.0);

                            let mi_to_all = all_intensities
                                .as_ref()
                                .map(|ints| calc_mi_to_many_score(&aligned_intensities, ints))
                                .unwrap_or(0.0);

                            // Create scores struct
                            AlignedTransitionScores {
                                feature_id: peak_mapping.aligned_feature_id,
                                transition_id: transition_id.parse().unwrap_or(0),
                                run_id: peak_mapping.run_id,
                                aligned_filename: current_filename.clone(),
                                label: peak_mapping.label,
                                xcorr_coelution_to_ref: Some(xcorr_coelution_to_ref),
                                xcorr_shape_to_ref: Some(xcorr_shape_to_ref),
                                mi_to_ref: Some(mi_to_ref),
                                xcorr_coelution_to_all: Some(xcorr_coelution_to_all),
                                xcorr_shape_to_all: Some(xcorr_shape_to_all),
                                mi_to_all: Some(mi_to_all),
                                rt_deviation: Some(
                                    (peak_mapping.aligned_rt - peak_mapping.reference_rt).abs(),
                                ),
                                intensity_ratio: Some(compute_peak_intensity_ratio(
                                    &reference_intensities,
                                    &aligned_intensities,
                                )),
                            }
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        })
        .fold(
            || HashMap::new(),
            |mut acc, score| {
                let filename = score.aligned_filename.clone(); // Adjust based on your actual data
                acc.entry(filename).or_insert_with(Vec::new).push(score);
                acc
            },
        )
        .reduce(
            || HashMap::new(),
            |mut a, b| {
                for (k, v) in b {
                    a.entry(k).or_insert_with(Vec::new).extend(v);
                }
                a
            },
        )
}

/// Computes the peak intensity ratio between two intensity arrays around a given retention time.
///
/// # Parameters
/// - `intensities1`: Intensity array of the first chromatogram.
/// - `intensities2`: Intensity array of the second chromatogram.
///
/// # Returns
/// The peak intensity ratio between the two intensity arrays.
fn compute_peak_intensity_ratio(intensities1: &Array1<f64>, intensities2: &Array1<f64>) -> f64 {
    // Compute peak intensity ratio
    let intensity1: f64 = intensities1.iter().sum();
    let intensity2: f64 = intensities2.iter().sum();

    // Account for NaN values
    if intensity1 == 0.0 {
        0.0
    } else {
        intensity2 / intensity1
    }
}

/// Get peak intensities given the left and right width.
///
/// # Parameters
/// - `aligned_chrom`: An aligned chromatogram.
/// - `left_width`: The left width.
/// - `right_width`: The right width.
///
/// # Returns
/// A vector of peak intensities.
fn get_peak_intensities(chrom: &Chromatogram, left_width: f64, right_width: f64) -> Vec<f64> {
    let left_boundary_idx = find_closest_index(&chrom.retention_times, left_width).unwrap_or(0);
    let right_boundary_idx = find_closest_index(&chrom.retention_times, right_width)
        .unwrap_or(chrom.retention_times.len() - 1);
    let peak_intensities = trim_vector(&chrom.intensities, left_boundary_idx, right_boundary_idx);
    peak_intensities
}

// /// Get peak intensities for all aligned chromatograms given the aligned left and right width mappings
// ///
// /// # Parameters
// /// - `aligned_chromatograms`: A list of aligned chromatograms.
// /// - `peak_mappings`: A dictionary of peak mappings for each run.
// ///
// /// # Returns
// /// A list of peak intensities for each aligned peak.
// fn get_array_peak_intensities(
//     aligned_chromatograms: Vec<AlignedChromatogram>,
//     peak_mappings: Vec<PeakMapping>,
// ) -> Vec<Array1<f64>> {

//     let mut peak_intensities_array = Vec::new();

//     for chrom in aligned_chromatograms {
//         let filename = chrom.chromatogram.metadata.get("basename").unwrap_or(&chrom.chromatogram.native_id).to_string();
//         if let Some(peak_mapping_for_run) = peak_mappings.iter().find(|m| m.aligned_filename == filename) {
//             let peak_intensities = get_peak_intensities(&chrom.chromatogram, peak_mapping_for_run.aligned_left_width, peak_mapping_for_run.aligned_right_width);
//             peak_intensities_array.push(Array1::from(peak_intensities));
//         }
//     }

//     peak_intensities_array
// }

/// Optimized version of get_array_peak_intensities
fn get_array_peak_intensities(
    aligned_chromatograms: &[AlignedChromatogram],
    peak_mappings: &[&PeakMapping], // Changed to accept references
) -> Vec<Array1<f64>> {
    // Create lookup map for faster filename searching
    let peak_map_lookup: HashMap<_, _> = peak_mappings
        .iter()
        .map(|m| (&m.aligned_filename, *m)) // Dereference here
        .collect();

    aligned_chromatograms
        .iter()
        .filter_map(|chrom| {
            let filename = chrom
                .chromatogram
                .metadata
                .get("basename")
                .unwrap_or(&chrom.chromatogram.native_id);

            peak_map_lookup.get(filename).map(|mapping| {
                Array1::from(get_peak_intensities(
                    &chrom.chromatogram,
                    mapping.aligned_left_width,
                    mapping.aligned_right_width,
                ))
            })
        })
        .collect()
}

/// Helper function to get all intensities for an alignment ID
fn get_all_intensities_for_alignment(
    chromatograms: &Arc<&Vec<AlignedChromatogram>>,
    peak_mappings: &Arc<&HashMap<String, Vec<PeakMapping>>>,
    alignment_id: i64,
) -> Option<Vec<Array1<f64>>> {
    let relevant_mappings: Vec<_> = peak_mappings
        .values()
        .flat_map(|mappings| mappings.iter())
        .filter(|m| m.alignment_id == alignment_id)
        .collect();

    if relevant_mappings.is_empty() {
        return None;
    }

    Some(get_array_peak_intensities(
        chromatograms.as_ref(),
        &relevant_mappings,
    ))
}

/// Trim vector given the start and end indices.
///
/// # Parameters
/// - `x`: A vector of values.
/// - `start_idx`: The start index.
/// - `end_idx`: The end index.
///
/// # Returns
/// A trimmed vector.
fn trim_vector(x: &Vec<f64>, start_idx: usize, end_idx: usize) -> Vec<f64> {
    // // Ensure the indices are valid
    // if start_idx >= x.len() || end_idx >= x.len() {
    //     panic!("Invalid indices, start_idx: {}, end_idx: {}, vector length: {}", start_idx, end_idx, x.len());
    // }

    // // Check if the start index is greater than the end index
    // if start_idx > end_idx {
    //     panic!("Start index is greater than end index");
    // }

    let (valid_start_idx, valid_end_index) = validate_widths(start_idx as f64, end_idx as f64);
    let start_idx = valid_start_idx as usize;
    let end_idx = valid_end_index as usize;

    x[start_idx..end_idx].to_vec()
}

/// Finds the index of the closest value to the target value in an array.
///
/// # Parameters
/// - `x`: A vector of values.
/// - `target`: The target value.
///
/// # Returns
/// The index of the closest value to the target value.
fn find_closest_index(x: &Vec<f64>, target: f64) -> Option<usize> {
    if x.is_empty() {
        panic!("Empty array");
    }

    let mut min_diff = f64::MAX;
    let mut closest_index = 0;

    for (index, time) in x.iter().enumerate() {
        let diff = (time - target).abs();
        if diff < min_diff {
            min_diff = diff;
            closest_index = index;
        }
    }

    Some(closest_index)
}

/// Creates decoy peaks by stratified shuffling of aligned peak tuples within each run.
///
/// # Parameters
/// - `peak_mappings`: A dictionary of peak mappings for each run.
///
/// # Returns
/// A dictionary of peak mappings with shuffled aligned peaks.
pub fn create_decoy_peaks_by_stratified_shuffling(
    peak_mappings: &HashMap<String, Vec<PeakMapping>>,
) -> HashMap<String, Vec<PeakMapping>> {
    let mut decoy_peak_mappings = HashMap::new();
    let mut rng = rand::rng();

    for (run_id, peaks) in peak_mappings {
        if peaks.is_empty() {
            continue;
        }

        let peak_tuples: Vec<PeakTuple> = peaks
            .iter()
            .map(|peak| PeakTuple {
                aligned_feature_id: peak.aligned_feature_id,
                aligned_rt: peak.aligned_rt,
                aligned_left_width: peak.aligned_left_width,
                aligned_right_width: peak.aligned_right_width,
            })
            .collect();

        let shuffled_tuples = stratified_shuffle_peak_tuples(&peak_tuples, &mut rng);

        let decoys = peaks
            .iter()
            .zip(shuffled_tuples.iter())
            .map(|(peak, shuffled)| {
                let mut decoy = peak.clone();
                decoy.aligned_rt = shuffled.aligned_rt;
                decoy.aligned_left_width = shuffled.aligned_left_width;
                decoy.aligned_right_width = shuffled.aligned_right_width;
                decoy.aligned_feature_id = shuffled.aligned_feature_id;
                decoy.label = -1;
                decoy
            })
            .collect();

        decoy_peak_mappings.insert(run_id.clone(), decoys);
    }

    decoy_peak_mappings
}

/// Backward-compatible alias for the legacy "shuffle" decoy method.
pub fn create_decoy_peaks_by_shuffling(
    peak_mappings: &HashMap<String, Vec<PeakMapping>>,
) -> HashMap<String, Vec<PeakMapping>> {
    create_decoy_peaks_by_stratified_shuffling(peak_mappings)
}

/// Creates decoy peaks by selecting hard negatives from the real candidate features in the same run.
///
/// The selected decoys are the nearest wrong features to the mapped target peak, preferring
/// candidates that still fall within the configured RT tolerance.
pub fn create_decoy_peaks_by_candidate_hard_negative(
    peak_mappings: &HashMap<String, Vec<PeakMapping>>,
    feature_data: &[FeatureData],
    rt_tolerance: f64,
) -> HashMap<String, Vec<PeakMapping>> {
    let mut feature_candidates_by_run: HashMap<String, Vec<FeatureCandidate>> = HashMap::new();
    for feature in feature_data {
        let candidates = flatten_feature_candidates(feature);
        if !candidates.is_empty() {
            feature_candidates_by_run
                .entry(feature.basename.clone())
                .or_default()
                .extend(candidates);
        }
    }

    let mut decoy_peak_mappings = HashMap::new();
    let mut rng = rand::rng();

    for (run_id, peaks) in peak_mappings {
        let Some(candidates) = feature_candidates_by_run.get(run_id) else {
            continue;
        };

        let decoys: Vec<PeakMapping> = peaks
            .iter()
            .map(|peak| {
                let target_width =
                    peak_width_from_bounds(peak.aligned_left_width, peak.aligned_right_width);
                let decoy_candidate = candidates
                    .iter()
                    .filter(|candidate| candidate.feature_id != peak.aligned_feature_id)
                    .min_by(|candidate_a, candidate_b| {
                        let a_rt_diff = (candidate_a.rt - peak.aligned_rt).abs();
                        let b_rt_diff = (candidate_b.rt - peak.aligned_rt).abs();
                        let a_width_diff = (peak_width_from_bounds(
                            candidate_a.left_width,
                            candidate_a.right_width,
                        ) - target_width)
                            .abs();
                        let b_width_diff = (peak_width_from_bounds(
                            candidate_b.left_width,
                            candidate_b.right_width,
                        ) - target_width)
                            .abs();
                        let a_within_tolerance = a_rt_diff <= rt_tolerance;
                        let b_within_tolerance = b_rt_diff <= rt_tolerance;

                        (!a_within_tolerance)
                            .cmp(&!b_within_tolerance)
                            .then(
                                a_rt_diff
                                    .partial_cmp(&b_rt_diff)
                                    .unwrap_or(std::cmp::Ordering::Equal),
                            )
                            .then(
                                a_width_diff
                                    .partial_cmp(&b_width_diff)
                                    .unwrap_or(std::cmp::Ordering::Equal),
                            )
                            .then(
                                candidate_a
                                    .rank
                                    .unwrap_or(i32::MAX)
                                    .cmp(&candidate_b.rank.unwrap_or(i32::MAX)),
                            )
                            .then(
                                candidate_a
                                    .qvalue
                                    .unwrap_or(f64::INFINITY)
                                    .partial_cmp(&candidate_b.qvalue.unwrap_or(f64::INFINITY))
                                    .unwrap_or(std::cmp::Ordering::Equal),
                            )
                            .then(candidate_a.feature_id.cmp(&candidate_b.feature_id))
                    });

                let fallback_peak = PeakTuple {
                    aligned_feature_id: peak.aligned_feature_id,
                    aligned_rt: peak.aligned_rt,
                    aligned_left_width: peak.aligned_left_width,
                    aligned_right_width: peak.aligned_right_width,
                };

                let decoy_tuple = decoy_candidate
                    .map(|candidate| {
                        let (aligned_left_width, aligned_right_width) =
                            validate_widths(candidate.left_width, candidate.right_width);
                        PeakTuple {
                            aligned_feature_id: candidate.feature_id,
                            aligned_rt: candidate.rt,
                            aligned_left_width,
                            aligned_right_width,
                        }
                    })
                    .unwrap_or_else(|| synthesize_offset_peak_tuple(&fallback_peak, &mut rng));

                let mut decoy = peak.clone();
                decoy.aligned_rt = decoy_tuple.aligned_rt;
                decoy.aligned_left_width = decoy_tuple.aligned_left_width;
                decoy.aligned_right_width = decoy_tuple.aligned_right_width;
                decoy.aligned_feature_id = decoy_tuple.aligned_feature_id;
                decoy.label = -1;
                decoy
            })
            .collect();

        if !decoys.is_empty() {
            decoy_peak_mappings.insert(run_id.clone(), decoys);
        }
    }

    decoy_peak_mappings
}

/// Creates decoy peaks by randomly selecting regions in the aligned chromatogram for each peak.
///
/// # Parameters
/// - `aligned_chromatograms`: A list of aligned chromatograms.
/// - `peak_mappings`: A dictionary of peak mappings for each run.
/// - `window_size`: Size of the decoy peak in retention time points.
///
/// # Returns
/// A dictionary of peak mappings with randomly selected regions for the aligned peaks.
pub fn create_decoy_peaks_by_random_regions(
    aligned_chromatograms: &[AlignedChromatogram],
    peak_mappings: &HashMap<String, Vec<PeakMapping>>,
    window_size: usize,
) -> HashMap<String, Vec<PeakMapping>> {
    let mut decoy_peak_mappings = peak_mappings.clone();
    let mut rng = rand::rng();

    // Iterate over each run (filename) and create decoy peaks
    for (run_id, peaks) in decoy_peak_mappings.iter_mut() {
        // Find the corresponding chromatogram for this run
        let chromatogram = aligned_chromatograms
            .iter()
            .find(|chrom| chrom.chromatogram.metadata.get("basename").unwrap() == run_id)
            .expect("Chromatogram not found for run");

        let retention_times = &chromatogram.chromatogram.retention_times;

        // Ensure the window size is valid
        if window_size >= retention_times.len() {
            panic!("Window size is larger than the retention time array");
        }

        // Create decoy peaks by randomly selecting regions for the aligned peak
        for peak in peaks.iter_mut() {
            // Randomly select a start index for the decoy peak in the aligned chromatogram
            let start_idx = rng.random_range(0..retention_times.len() - window_size);
            let end_idx = start_idx + window_size;

            // Update only the aligned peak information
            peak.aligned_rt = (retention_times[start_idx] + retention_times[end_idx]) / 2.0;
            peak.aligned_left_width = retention_times[start_idx];
            peak.aligned_right_width = retention_times[end_idx];
            peak.aligned_feature_id = -1; // Mark as decoy
            peak.label = -1; // Mark as decoy
        }
    }

    decoy_peak_mappings
}

/// Creates a feature matrix and labels for all the peak mappings.
pub fn create_feature_matrix(
    peak_mappings: &HashMap<String, Vec<PeakMapping>>,
) -> (Array2<f64>, Array1<i32>) {
    // feature matrix should be of shape len(peak_mappings) + inner len of each peak_mapping by 7 features
    let nrows: usize = peak_mappings
        .iter()
        .map(|(_, mappings)| mappings.len())
        .sum::<usize>()
        + peak_mappings.len();
    let ncols = 8;
    let mut feature_matrix = Array2::zeros((nrows, ncols));
    let mut labels = Array1::zeros(nrows);

    let mut row_idx = 0;
    for (_filename, mappings) in peak_mappings.iter() {
        for mapping in mappings {
            feature_matrix.row_mut(row_idx).assign(&Array1::from(vec![
                mapping.xcorr_coelution_to_ref.unwrap_or(0.0),
                mapping.xcorr_shape_to_ref.unwrap_or(0.0),
                mapping.mi_to_ref.unwrap_or(0.0),
                mapping.xcorr_coelution_to_all.unwrap_or(0.0),
                mapping.xcorr_shape_to_all.unwrap_or(0.0),
                mapping.mi_to_all.unwrap_or(0.0),
                mapping.rt_deviation.unwrap_or(-1.0), // TODO: Should this be -1.0 or some large value?
                mapping.intensity_ratio.unwrap_or(0.0),
            ]));
            labels[row_idx] = mapping.label;
            row_idx += 1;
        }
    }

    (feature_matrix, labels)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_peak_mapping(run_id: i64, aligned_feature_id: i64, aligned_rt: f64) -> PeakMapping {
        PeakMapping {
            alignment_id: aligned_feature_id,
            precursor_id: 1,
            run_id,
            reference_feature_id: 100 + aligned_feature_id,
            aligned_feature_id,
            reference_rt: aligned_rt - 0.5,
            aligned_rt,
            reference_left_width: aligned_rt - 1.0,
            reference_right_width: aligned_rt + 1.0,
            aligned_left_width: aligned_rt - 1.0,
            aligned_right_width: aligned_rt + 1.0,
            reference_filename: "ref".to_string(),
            aligned_filename: format!("run_{run_id}"),
            label: 1,
            xcorr_coelution_to_ref: None,
            xcorr_shape_to_ref: None,
            mi_to_ref: None,
            xcorr_coelution_to_all: None,
            xcorr_shape_to_all: None,
            mi_to_all: None,
            rt_deviation: None,
            intensity_ratio: None,
            mapped_target_rt: None,
            roundtrip_error: None,
            candidate_total_count: None,
            candidate_within_tolerance_count: None,
            normalized_rt_error: None,
            mapping_score: None,
            mapping_confidence: None,
            score_margin_to_second_best: None,
            feature_rank: None,
            feature_qvalue: None,
        }
    }

    #[test]
    fn stratified_shuffle_preserves_peak_tuples() {
        let run_id = "run_1".to_string();
        let peaks = vec![
            make_peak_mapping(1, 10, 100.0),
            make_peak_mapping(1, 11, 102.0),
            make_peak_mapping(1, 12, 104.0),
            make_peak_mapping(1, 13, 106.0),
        ];
        let peak_mappings = HashMap::from([(run_id.clone(), peaks.clone())]);

        let decoys = create_decoy_peaks_by_stratified_shuffling(&peak_mappings);
        let run_decoys = decoys.get(&run_id).unwrap();

        assert_eq!(run_decoys.len(), peaks.len());
        for (original, decoy) in peaks.iter().zip(run_decoys.iter()) {
            assert_eq!(decoy.label, -1);
            assert_ne!(decoy.aligned_feature_id, original.aligned_feature_id);
        }

        let original_by_feature: HashMap<i64, (f64, f64, f64)> = peaks
            .iter()
            .map(|peak| {
                (
                    peak.aligned_feature_id,
                    (
                        peak.aligned_rt,
                        peak.aligned_left_width,
                        peak.aligned_right_width,
                    ),
                )
            })
            .collect();

        for decoy in run_decoys {
            let expected_tuple = original_by_feature.get(&decoy.aligned_feature_id).unwrap();
            assert_eq!(
                *expected_tuple,
                (
                    decoy.aligned_rt,
                    decoy.aligned_left_width,
                    decoy.aligned_right_width,
                )
            );
        }
    }

    #[test]
    fn candidate_hard_negative_prefers_nearest_wrong_feature() {
        let run_id = "run_1".to_string();
        let peak_mappings =
            HashMap::from([(run_id.clone(), vec![make_peak_mapping(1, 10, 100.0)])]);

        let feature_data = vec![FeatureData::new(
            "run_1.osw".to_string(),
            1,
            1,
            Some(ValueEntryType::Multiple(vec![10, 11, 12])),
            ValueEntryType::Multiple(vec![100.0, 102.0, 130.0]),
            Some(ValueEntryType::Multiple(vec![99.0, 101.0, 129.0])),
            Some(ValueEntryType::Multiple(vec![101.0, 103.0, 131.0])),
            None,
            Some(ValueEntryType::Multiple(vec![1, 2, 3])),
            Some(ValueEntryType::Multiple(vec![0.001, 0.01, 0.5])),
            None,
        )];

        let decoys =
            create_decoy_peaks_by_candidate_hard_negative(&peak_mappings, &feature_data, 5.0);
        let run_decoys = decoys.get(&run_id).unwrap();
        let decoy = run_decoys.first().unwrap();

        assert_eq!(decoy.label, -1);
        assert_eq!(decoy.aligned_feature_id, 11);
        assert_eq!(decoy.aligned_rt, 102.0);
        assert_eq!(decoy.aligned_left_width, 101.0);
        assert_eq!(decoy.aligned_right_width, 103.0);
    }
}
