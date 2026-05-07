use anyhow::Error as AnyHowError;
use ndarray::Array1;
use rand::prelude::IndexedRandom;
use rayon::prelude::*;
use dtw_rs::{Algorithm, DynamicTimeWarping};

use crate::alignment::fast_fourier_lag::{find_lag_with_max_correlation, shift_chromatogram};
use arycal_common::chromatogram::{AlignedChromatogram, AlignedRTPointPair, Chromatogram};
use arycal_common::config::AlignmentConfig;
use arycal_cloudpath::util::extract_basename;

// /// Creates a mapping between the original retention times (RT) of two chromatograms based on the lag and DTW alignment.
// ///
// /// The mapping is stored as a list of dictionaries, where each dictionary contains the following keys:
// ///
// /// # Parameters
// /// - `lag`: The lag between the two chromatograms.
// /// - `chrom1`: The reference chromatogram.
// /// - `chrom2`: The chromatogram to align.
// ///
// /// # Returns
// /// A list of dictionaries containing the following
// pub fn create_fft_dtw_rt_mapping(
//     lag: isize,
//     chrom1: &Chromatogram,
//     chrom2: &Chromatogram,
// ) -> Vec<HashMap<String, f64>> {
//     // let run1_name = chrom1.metadata.get("basename").unwrap_or(&chrom1.native_id).to_string();
//     // let run2_name = chrom2.metadata.get("basename").unwrap_or(&chrom2.native_id).to_string();

//     chrom1.retention_times
//         .iter()
//         .enumerate()
//         .filter_map(|(i, &rt1)| {
//             let j = (i as isize + lag) as usize;
//             chrom2.retention_times.get(j).map(|&rt2| {
//                 let mut entry = HashMap::with_capacity(0);
//                 entry.insert("rt1".to_string(), rt1);
//                 entry.insert("rt2".to_string(), rt2);
//                 // entry.insert("alignment".to_string(), format!("({}, {})", rt1, rt2));
//                 // entry.insert("run1".to_string(), run1_name.clone());
//                 // entry.insert("run2".to_string(), run2_name.clone());
//                 entry
//             })
//         })
//         .collect()
// }

/// Creates a mapping between the reference RT space and the original query RT space
/// from the local-refinement DTW path.
///
/// # Parameters
/// - `optimal_path`: The DTW path between the reference TIC and lag-shifted query TIC.
/// - `chrom1`: The reference chromatogram.
/// - `chrom2`: The original query chromatogram.
///
/// # Returns
/// A vector of aligned RT point pairs
pub fn create_fft_dtw_rt_mapping(
    optimal_path: &[(usize, usize)],
    chrom1: &Chromatogram,
    chrom2: &Chromatogram,
) -> Vec<AlignedRTPointPair> {
    optimal_path
        .iter()
        .filter(|&&(i, j)| i > 0 && j > 0)
        .map(|&(i, j)| AlignedRTPointPair {
            rt1: chrom1.retention_times[i - 1] as f32,
            rt2: chrom2.retention_times[j - 1] as f32,
        })
        .collect()
}

/// Aligns a series of chromatograms using FFT-based cross-correlation with local refinement via DTW.
///
/// This function aligns a series of chromatograms to a randomly selected reference chromatogram.
/// The alignment is performed in two steps:
///
/// 1. FFT-based cross-correlation to find the lag between the chromatograms.
/// 2. Local refinement using DTW to align the chromatograms based on the computed lag.
///
/// # Parameters
/// - `smoothed_tics`: A vector of chromatograms to align.
/// - `params`: Extra alignment parameters.
///
/// # Returns
/// A vector of aligned chromatograms.
pub fn star_align_tics_fft_with_local_refinement(
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



    let ref_intensities = Array1::from(reference_chrom.intensities.clone());
    let ref_rt = &reference_chrom.retention_times;
    let ref_name = reference_chrom.metadata.get("basename").unwrap_or(&reference_chrom.native_id);

    // Process chromatograms in parallel
    let aligned_chromatograms = smoothed_tics.par_iter()
        // .filter(|chrom| {
        //     let chrom_name = chrom.metadata.get("basename").unwrap_or(&chrom.native_id);
        //     chrom_name != ref_name
        // })
        .map(|chrom| {
            // Step 1: FFT cross-correlation
            let query_intensities = Array1::from(chrom.intensities.clone());
            let cross_corr = fftconvolve::fftcorrelate(&ref_intensities, &query_intensities, fftconvolve::Mode::Full)
                .unwrap()
                .to_vec();

            // Step 2: Find optimal lag
            let lag = find_lag_with_max_correlation(&cross_corr);

            // Step 3: Shift chromatogram
            let mut aligned_chrom = shift_chromatogram(chrom, lag);

            // Step 4: DTW refinement
            let query_intensities_slice = aligned_chrom.intensities.as_slice();
            let dtw = DynamicTimeWarping::between(
                ref_intensities.as_slice().unwrap(),  // Convert Array1 to slice
                query_intensities_slice      // Use slice directly
            );
            let path = dtw.path();

            // Apply DTW alignment
            let (refined_rt, refined_intensities): (Vec<_>, Vec<_>) = path.iter()
                .map(|&(_, j)| (aligned_chrom.retention_times[j], aligned_chrom.intensities[j]))
                .unzip();

            // Check if we want to retain the alignment path
            let path_out = if params.retain_alignment_path {
                path.to_vec()
            } else {
                Vec::new()
            };

            aligned_chrom.retention_times = refined_rt;
            aligned_chrom.intensities = refined_intensities;

            // Peak mapping needs a reference->original-query RT map. The DTW path is the
            // reliable source of that relationship; the display-time shifted/refined trace is not.
            let mapping = create_fft_dtw_rt_mapping(&path, reference_chrom, chrom);

            AlignedChromatogram {
                chromatogram: aligned_chrom,
                alignment_path: path_out,
                lag: Some(lag),
                rt_mapping: mapping,
                reference_basename: ref_name.to_string(),
            }
        })
        .collect();

    Ok(aligned_chromatograms)
}
