use anyhow::Error as AnyHowError;
use arycal_common::chromatogram::AlignedRTPointPair;
use rand::prelude::IndexedRandom;
use rustfft::{num_complex::Complex, FftPlanner};
use std::f64;
use std::collections::HashSet;
use rayon::prelude::*;

use super::alignment::calculate_distance;
use super::alignment::construct_mst;
use arycal_common::chromatogram::{Chromatogram, AlignedChromatogram};
use arycal_cloudpath::sqmass::TransitionGroup;
use arycal_common::config::AlignmentConfig;
use arycal_cloudpath::util::extract_basename;

/// Finds the lag with the maximum correlation in a cross-correlation vector.
///
/// # Parameters
/// - `cross_corr`: A vector of cross-correlation values
///
/// # Returns
/// - The lag with the maximum correlation
pub fn find_lag_with_max_correlation(cross_corr: &[f64]) -> isize {
    let (max_idx, _) = cross_corr
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
        .unwrap();
    let center = cross_corr.len() / 2;
    max_idx as isize - center as isize
}

/// Computes a full real-valued cross-correlation using FFT.
///
/// This matches the lag convention expected by `find_lag_with_max_correlation` by
/// computing `conv(reference, reverse(query))`, which yields a vector of length
/// `reference.len() + query.len() - 1`.
pub(crate) fn fft_cross_correlate_full(reference: &[f64], query: &[f64]) -> Vec<f64> {
    if reference.is_empty() || query.is_empty() {
        return Vec::new();
    }

    let output_len = reference.len() + query.len() - 1;
    let fft_len = output_len.next_power_of_two();

    let mut planner = FftPlanner::<f64>::new();
    let forward = planner.plan_fft_forward(fft_len);
    let inverse = planner.plan_fft_inverse(fft_len);

    let mut reference_freq = vec![Complex::new(0.0, 0.0); fft_len];
    for (slot, &value) in reference_freq.iter_mut().zip(reference.iter()) {
        slot.re = value;
    }
    forward.process(&mut reference_freq);

    let mut reversed_query_freq = vec![Complex::new(0.0, 0.0); fft_len];
    for (slot, &value) in reversed_query_freq.iter_mut().zip(query.iter().rev()) {
        slot.re = value;
    }
    forward.process(&mut reversed_query_freq);

    let mut product: Vec<Complex<f64>> = reference_freq
        .into_iter()
        .zip(reversed_query_freq)
        .map(|(lhs, rhs)| lhs * rhs)
        .collect();
    inverse.process(&mut product);

    let scale = fft_len as f64;
    product
        .into_iter()
        .take(output_len)
        .map(|value| value.re / scale)
        .collect()
}

// /// Shifts the retention times of a chromatogram by a given lag.
// ///
// /// # Parameters
// /// - `chrom` A chromatogram to shift
// /// - `lag` The lag to apply to the retention times
// ///
// /// # Returns
// /// - A new chromatogram with shifted retention times
// pub fn shift_chromatogram(chrom: &Chromatogram, lag: isize) -> Chromatogram {
//     let mut shifted_chrom = chrom.clone();
//     shifted_chrom.retention_times = chrom
//         .retention_times
//         .iter()
//         .map(|&rt| rt + lag as f64) // Apply the lag
//         .collect();
//     shifted_chrom
// }

/// Shifts the retention times of a chromatogram by a given lag.
///
/// # Parameters
/// - `chrom` A chromatogram to shift
/// - `lag` The lag to apply to the retention times
///
/// # Returns
/// - A new chromatogram with shifted retention times
pub fn shift_chromatogram(chrom: &Chromatogram, lag: isize) -> Chromatogram {
    Chromatogram {
        retention_times: chrom.retention_times.iter()
            .map(|&rt| rt + lag as f64)
            .collect(),
        ..chrom.clone()
    }
}


// /// Creates a mapping between the original retention times (RT) of two chromatograms based on the lag.
// ///
// /// # Parameters
// /// - `lag`: The lag between the two chromatograms
// /// - `chrom1`: The first chromatogram
// /// - `chrom2`: The second chromatogram
// ///
// /// # Returns
// /// - A vector of hashmaps containing the RT mapping
// pub fn create_fft_rt_mapping(
//     _lag: isize,
//     chrom1: &Chromatogram,
//     chrom2: &Chromatogram,
// ) -> Vec<HashMap<String, f64>> {
//     // let run1_name = chrom1.metadata.get("basename").unwrap_or(&chrom1.native_id).to_string();
//     // let run2_name = chrom2.metadata.get("basename").unwrap_or(&chrom2.native_id).to_string();

//     chrom1.retention_times
//         .iter()
//         .zip(chrom2.retention_times.iter())
//         .map(|(&rt1, &rt2)| {
//             let mut entry = HashMap::with_capacity(0);
//             entry.insert("rt1".to_string(), rt1);
//             entry.insert("rt2".to_string(), rt2);
//             // entry.insert("alignment".to_string(), format!("({}, {})", rt1, rt2));
//             // entry.insert("run1".to_string(), run1_name.clone());
//             // entry.insert("run2".to_string(), run2_name.clone());
//             entry
//         })
//         .collect()
// }

/// Creates a retention time mapping between two chromatograms for FFT alignment.
///
/// # Parameters
/// - `_lag`: The lag between chromatograms (unused in basic FFT alignment)
/// - `chrom1`: Reference chromatogram
/// - `chrom2`: Aligned chromatogram
///
/// # Returns
/// Vector of aligned RT point pairs
pub fn create_fft_rt_mapping(
    _lag: isize,
    chrom1: &Chromatogram,
    chrom2: &Chromatogram,
) -> Vec<AlignedRTPointPair> {
    // Pre-allocate with known capacity for efficiency
    let capacity = std::cmp::min(chrom1.retention_times.len(), chrom2.retention_times.len());
    let mut mapping = Vec::with_capacity(capacity);
    
    // Create pairs by zipping retention time vectors
    for (&rt1, &rt2) in chrom1.retention_times.iter().zip(chrom2.retention_times.iter()) {
        mapping.push(AlignedRTPointPair {
            rt1: rt1 as f32,
            rt2: rt2 as f32,
        });
    }
    
    mapping
}

/// Aligns a series of chromatograms by picking a random run as the reference and aligning all others to it,
/// using FFT-based cross-correlation.
///
/// # Parameters
/// - `smoothed_tics`: A vector of smoothed chromatograms
/// - `params`: Extra alignment configuration parameters
///
/// # Returns
/// - A vector of aligned chromatograms with their alignment offsets
pub fn star_align_tics_fft(
    smoothed_tics: &Vec<Chromatogram>,
    params: &AlignmentConfig,
) -> Result<Vec<AlignedChromatogram>, AnyHowError> {
    if smoothed_tics.len() < 2 {
        // return Err(AnyHowError::msg("At least two chromatograms are required for alignment"));
        log::warn!("At least two chromatograms are required for alignment - returning empty result");
        return Ok(Vec::new());
    }

    // Random reference selection (keeping original method)
     let reference_chrom = if let Some(ref_chrom) = &params.reference_run {
        match smoothed_tics.iter()
            .find(|x| x.metadata.get("basename").unwrap_or(&x.native_id) == &extract_basename(ref_chrom)) 
        {
            Some(chrom) => chrom,
            None => {
                log::warn!("Specified reference chromatogram not found - returning empty result");
                return Ok(Vec::new());  // Return empty vector immediately
            }
        }
    } else {
        let mut rng = rand::rng();
        let binding = (0..smoothed_tics.len()).collect::<Vec<_>>();
        let reference_idx = binding.choose(&mut rng).unwrap();
        &smoothed_tics[*reference_idx]
    };

    let ref_intensities = &reference_chrom.intensities;
    let ref_name = reference_chrom.metadata.get("basename").unwrap_or(&reference_chrom.native_id);

    // Process chromatograms in parallel
    let aligned_chromatograms = smoothed_tics.par_iter()
        // .filter(|chrom| {
        //     let chrom_name = chrom.metadata.get("basename").unwrap_or(&chrom.native_id);
        //     chrom_name != ref_name
        // })
        .map(|chrom| {
            // FFT cross-correlation
            let cross_corr = fft_cross_correlate_full(ref_intensities, &chrom.intensities);

            // Find optimal lag
            let lag = find_lag_with_max_correlation(&cross_corr);

            // Create aligned chromatogram
            let aligned_chrom = Chromatogram {
                retention_times: chrom.retention_times.iter()
                    .map(|&rt| rt + lag as f64)
                    .collect(),
                ..chrom.clone()
            };

            AlignedChromatogram {
                chromatogram: aligned_chrom,
                alignment_path: Vec::new(), // No path for FFT
                lag: Some(lag),
                rt_mapping: create_fft_rt_mapping(lag, reference_chrom, chrom),
                reference_basename: ref_name.clone(),
            }
        })
        .collect();

    Ok(aligned_chromatograms)
}

/// Progressively aligns a series of chromatograms using FFT-based cross-correlation.
/// Instead of aligning all chromatograms to a single reference, it updates the reference progressively.
///
/// # Parameters
/// - smoothed_tics: A vector of smoothed chromatograms
///
/// # Returns
/// - A vector of aligned chromatograms with their alignment offsets
pub fn progressive_align_tics_fft(
    smoothed_tics: &Vec<Chromatogram>,
) -> Result<Vec<AlignedChromatogram>, AnyHowError> {
    // Ensure we have at least two chromatograms to align
    if smoothed_tics.len() < 2 {
        return Err(AnyHowError::msg(
            "At least two chromatograms are required for alignment",
        ));
    }

    // Initialize the first chromatogram as the reference
    let mut aligned_sum = smoothed_tics[0].clone();

    // println!(
    //     "Using run: {:?} as the initial reference",
    //     aligned_sum.metadata.get("basename").unwrap_or(&aligned_sum.native_id)
    // );

    let mut aligned_chromatograms = vec![];

    // Progressively align chromatograms
    for (_idx, current_tic) in smoothed_tics.iter().enumerate() {
        // println!(
        //     "Aligning run: {:?} to progressive reference",
        //     current_tic.metadata.get("basename").unwrap_or(&current_tic.native_id)
        // );

        // Step 1: Perform FFT-based cross-correlation
        let cross_corr = fft_cross_correlate_full(&aligned_sum.intensities, &current_tic.intensities);

        // Step 2: Find the lag with the maximum correlation
        let lag = find_lag_with_max_correlation(&cross_corr);

        // Step 3: Shift chromatogram retention times by the computed lag
        let aligned_chrom = shift_chromatogram(current_tic, lag);

        // Step 4: Update the progressive reference by averaging the aligned chromatograms
        for (j, intensity) in aligned_chrom.intensities.iter().enumerate() {
            aligned_sum.intensities[j] = (aligned_sum.intensities[j] + intensity) / 2.0;
        }

        let rt_mapping = create_fft_rt_mapping(lag, &aligned_sum, current_tic);

        // Store the aligned chromatogram
        aligned_chromatograms.push(AlignedChromatogram {
            chromatogram: aligned_chrom.clone(),
            alignment_path: vec![], // No path for FFT-based alignment
            lag: Some(lag),
            rt_mapping: rt_mapping, 
            reference_basename: aligned_sum
                .metadata
                .get("basename")
                .unwrap_or(&aligned_sum.native_id)
                .clone(),
        });
    }

    Ok(aligned_chromatograms)
}

/// Aligns a series of chromatograms using a Minimum Spanning Tree (MST) approach
/// with FFT-based cross-correlation.
///
/// # Parameters
/// - `smoothed_tics`: A vector of smoothed chromatograms
///
/// # Returns
/// - A vector of aligned chromatograms with their alignment paths
pub fn mst_align_tics_fft(
    smoothed_tics: &Vec<Chromatogram>,
) -> Result<Vec<AlignedChromatogram>, AnyHowError> {
    // Ensure we have at least two chromatograms to align
    if smoothed_tics.len() < 2 {
        return Err(AnyHowError::msg(
            "At least two chromatograms are required for alignment",
        ));
    }

    // Calculate the pairwise distance matrix between chromatograms
    let num_chromatograms = smoothed_tics.len();
    let mut distances = Vec::new();

    for i in 0..num_chromatograms {
        for j in (i + 1)..num_chromatograms {
            // Use a distance function (e.g., DTW or Euclidean distance) to compute dissimilarity
            let distance = calculate_distance(&smoothed_tics[i], &smoothed_tics[j]);
            distances.push((i, j, distance));
        }
    }

    // Generate an MST using Kruskal's algorithm based on the distance matrix
    let mst_edges = construct_mst(&distances, num_chromatograms);

    // Initialize result storage
    let mut aligned_chromatograms = vec![];
    let mut trgrp_aligned = TransitionGroup::new("Aligned MST (FFT)".to_string());

    // HashSet to track chromatograms that have been added to aligned_chromatograms
    let mut aligned_chromatogram_indices = HashSet::new();

    // Align chromatograms based on the edges in the MST
    for (chrom1_idx, chrom2_idx, _) in mst_edges {
        let chrom1 = &smoothed_tics[chrom1_idx];
        let chrom2 = &smoothed_tics[chrom2_idx];

        // println!("Aligning runs: {:?} and {:?}", chrom1.metadata.get("basename"), chrom2.metadata.get("basename"));

        // Step 1: Perform FFT-based cross-correlation
        let cross_corr = fft_cross_correlate_full(&chrom1.intensities, &chrom2.intensities);

        let lag = find_lag_with_max_correlation(&cross_corr);

        // Step 2: Shift chromatogram retention times by the computed lag
        let aligned_chrom2 = shift_chromatogram(chrom2, lag);
        let rt_mapping = create_fft_rt_mapping(lag, chrom1, chrom2);

        // Store aligned chromatograms of the first chromatogram if not already added
        if !aligned_chromatogram_indices.contains(&chrom1_idx) {
            aligned_chromatograms.push(AlignedChromatogram {
                chromatogram: chrom1.clone(),
                alignment_path: vec![],
                lag: Some(lag),
                rt_mapping: rt_mapping.clone(),
                reference_basename: chrom1.metadata.get("basename").unwrap_or(&chrom1.native_id).clone(),
            });
            aligned_chromatogram_indices.insert(chrom1_idx);
        }

        // Store aligned chromatograms of the second chromatogram
        aligned_chromatograms.push(AlignedChromatogram {
            chromatogram: aligned_chrom2.clone(),
            alignment_path: vec![],
            lag: Some(lag),
            rt_mapping,
            reference_basename: chrom1.metadata.get("basename").unwrap_or(&chrom1.native_id).clone(),
        });
        aligned_chromatogram_indices.insert(chrom2_idx);

        // Add to aligned transition group for visualization
        let mut tmp_chrom = aligned_chrom2.clone();
        tmp_chrom.native_id = tmp_chrom
            .metadata
            .get("basename")
            .unwrap_or(&tmp_chrom.native_id)
            .clone();
        trgrp_aligned.add_chromatogram(tmp_chrom);
    }

    Ok(aligned_chromatograms)
}

#[cfg(test)]
mod tests {
    use super::{fft_cross_correlate_full, find_lag_with_max_correlation};

    #[test]
    fn fft_cross_correlation_finds_positive_lag_when_query_is_early() {
        let reference = vec![0.0, 0.0, 1.0, 0.0, 0.0];
        let query = vec![0.0, 1.0, 0.0, 0.0, 0.0];

        let cross_corr = fft_cross_correlate_full(&reference, &query);
        let lag = find_lag_with_max_correlation(&cross_corr);

        assert_eq!(lag, 1);
    }

    #[test]
    fn fft_cross_correlation_finds_negative_lag_when_query_is_late() {
        let reference = vec![0.0, 1.0, 0.0, 0.0, 0.0];
        let query = vec![0.0, 0.0, 1.0, 0.0, 0.0];

        let cross_corr = fft_cross_correlate_full(&reference, &query);
        let lag = find_lag_with_max_correlation(&cross_corr);

        assert_eq!(lag, -1);
    }
}
