use anyhow::{anyhow, Result as AnyHowResult};
use std::collections::HashMap;
use std::collections::BTreeSet;
use ordered_float::OrderedFloat;
use crate::savgol;
use serde::{Serialize, Deserialize};
use deepsize::DeepSizeOf;



/// Represents a single trace chromatogram with its associated data.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, DeepSizeOf)]
pub struct Chromatogram {
    /// Unique identifier for the chromatogram.
    pub id: i32,
    /// The native identifier (i.e. transitin identifier) for the chromatogram.
    pub native_id: String,
    /// The retention times associated with this chromatogram.
    pub retention_times: Vec<f64>,
    /// The intensity values associated with this chromatogram.
    pub intensities: Vec<f64>,
    /// Metadata for tracking data processing information.
    pub metadata: HashMap<String, String>,
}

impl Chromatogram {
    /// len() method to return the number of retention times or intensities.
    pub fn len(&self) -> usize {
        self.retention_times.len()
    }

    /// Applies Savitzky-Golay smoothing to the intensities and returns a new Chromatogram.
    ///
    /// # Arguments
    ///
    /// * `window_length` - The length of the filter window (must be a positive odd integer).
    /// * `poly_order` - The order of the polynomial used to fit the samples (must be less than `window_length`).
    ///
    /// # Returns
    ///
    /// A new `Chromatogram` with smoothed intensities.
    pub fn smooth_sgolay(
        &self,
        window_length: usize,
        poly_order: usize,
    ) -> AnyHowResult<Chromatogram> {
        // Use anyhow for error propagation
        let smoothed_intensities = self
            .calculate_smoothed_intensities(window_length, poly_order)
            .map_err(|e| anyhow!(e))?;

        // Set any negative intensities to zero
        let smoothed_intensities: Vec<f64> = smoothed_intensities
            .into_iter()
            .map(|intensity| intensity.max(0.0))
            .collect();

        let mut new_metadata = self.metadata.clone();
        new_metadata.insert("smoothing_method".to_string(), "Savitzky-Golay".to_string());
        new_metadata.insert("window_length".to_string(), window_length.to_string());
        new_metadata.insert("poly_order".to_string(), poly_order.to_string());

        Ok(Chromatogram {
            id: self.id,
            native_id: self.native_id.clone(),
            retention_times: self.retention_times.clone(),
            intensities: smoothed_intensities,
            metadata: new_metadata,
        })
    }

    /// Applies Savitzky-Golay smoothing to the intensities in-place.
    ///
    /// # Arguments
    ///
    /// * `window_length` - The length of the filter window (must be a positive odd integer).
    /// * `poly_order` - The order of the polynomial used to fit the samples (must be less than `window_length`).
    pub fn smooth_sgolay_inplace(
        &mut self,
        window_length: usize,
        poly_order: usize,
    ) -> AnyHowResult<()> {
        let smoothed_intensities = self
            .calculate_smoothed_intensities(window_length, poly_order)
            .map_err(|e| anyhow!(e))?; // Convert error to anyhow::Error
        self.intensities = smoothed_intensities;

        // Update metadata
        self.metadata
            .insert("smoothing_method".to_string(), "Savitzky-Golay".to_string());
        self.metadata
            .insert("window_length".to_string(), window_length.to_string());
        self.metadata
            .insert("poly_order".to_string(), poly_order.to_string());

        Ok(())
    }

    /// Helper method to calculate smoothed intensities using Savitzky-Golay filter.
    /// 
    /// # Parameters
    /// - `window_length`: The length of the filter window (must be a positive odd integer).
    /// - `poly_order`: The order of the polynomial used to fit the samples (must be less than `window_length`).
    /// 
    /// # Returns
    /// A vector of smoothed intensities.
    fn calculate_smoothed_intensities(
        &self,
        window_length: usize,
        poly_order: usize,
    ) -> AnyHowResult<Vec<f64>> {
        if window_length % 2 == 0 {
            return Err(anyhow!("Window length must be odd"));
        }
        if poly_order >= window_length {
            return Err(anyhow!("Polynomial order must be less than window length"));
        }

        savgol::savgol_filter(&self.intensities, window_length, poly_order)
            .map_err(|e| anyhow!("Savitzky-Golay smoothing failed: {}", e))
    }

    /// Normalizes the intensities using min-max normalization.
    ///
    /// If all intensity values are the same or if the range of intensities is zero (which would result in division by zero),
    /// the original chromatogram is returned without modification.
    ///
    /// # Returns
    ///
    /// A new `Chromatogram` with normalized intensities if possible, otherwise the original `Chromatogram`.
    pub fn normalize(&self) -> AnyHowResult<Chromatogram> {
        let min_intensity = self
            .intensities
            .iter()
            .cloned()
            .fold(f64::INFINITY, f64::min);
        let max_intensity = self
            .intensities
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);

        if max_intensity - min_intensity == 0.0 {
            // If the range is zero, return the original chromatogram
            return Ok(self.clone());
        }

        let normalized_intensities: Vec<f64> = self
            .intensities
            .iter()
            .map(|&intensity| (intensity - min_intensity) / (max_intensity - min_intensity))
            .collect();

        let mut new_metadata = self.metadata.clone();
        new_metadata.insert("normalization_method".to_string(), "min-max".to_string());

        Ok(Chromatogram {
            id: self.id,
            native_id: self.native_id.clone(),
            retention_times: self.retention_times.clone(),
            intensities: normalized_intensities,
            metadata: new_metadata,
        })
    }

}

/// Crate a common retention time space for a vector of chromatograms.
/// 
/// This function creates a common retention time space by collecting all unique retention times from the input chromatograms.
/// It then creates new chromatograms with the common retention time space by interpolating intensities.
/// 
/// # Parameters
/// - `chromatograms`: A vector of `Chromatogram` instances.
/// 
/// # Returns
/// A vector of `Chromatogram` instances with a common retention time space.
pub fn create_common_rt_space(chromatograms: Vec<Chromatogram>) -> Vec<Chromatogram> {
    // Collect all unique retention times
    let all_rts: BTreeSet<OrderedFloat<f64>> = chromatograms
        .iter()
        .flat_map(|chrom| chrom.retention_times.iter().map(|&rt| OrderedFloat(rt)))
        .collect();

    let common_rts: Vec<f64> = all_rts.into_iter().map(|of| of.into_inner()).collect();

    // Create new chromatograms with common RT space
    chromatograms
        .into_iter()
        .map(|chrom| {
            let mut new_intensities = vec![0.0; common_rts.len()];
            let mut old_index = 0;

            for (new_index, &rt) in common_rts.iter().enumerate() {
                if old_index < chrom.retention_times.len()
                    && (chrom.retention_times[old_index] - rt).abs() < 1e-6
                {
                    new_intensities[new_index] = chrom.intensities[old_index];
                    old_index += 1;
                }
            }

            Chromatogram {
                id: chrom.id,
                native_id: chrom.native_id,
                retention_times: common_rts.clone(),
                intensities: new_intensities,
                metadata: chrom.metadata.clone(),
            }
        })
        .collect()
}

pub fn apply_common_rt_space_single(chrom: Chromatogram, common_rts: &Vec<f64>) -> Chromatogram {
    let mut new_intensities = vec![0.0; common_rts.len()];
    let mut old_index = 0;

    for (new_index, &rt) in common_rts.iter().enumerate() {
        if old_index < chrom.retention_times.len()
            && (chrom.retention_times[old_index] - rt).abs() < 1e-6
        {
            new_intensities[new_index] = chrom.intensities[old_index];
            old_index += 1;
        }
    }

    Chromatogram {
        id: chrom.id,
        native_id: chrom.native_id,
        retention_times: common_rts.clone(),
        intensities: new_intensities,
        metadata: chrom.metadata,
    }
}

/// Pad chromatograms
/// 
/// pad vector of chromatogram intensities with zeros and padding retention times with difference between last two retention times on both sides
/// 
/// # Parameters
/// - `chromatograms`: A vector of `Chromatogram` instances.
/// 
/// # Returns
pub fn pad_chromatograms(chromatograms: Vec<Chromatogram>) -> Vec<Chromatogram> {
    let mut padded_chromatograms = Vec::new();
    for chrom in chromatograms {
        let mut padded_intensities = vec![0.0; chrom.intensities.len() + 2];
        let mut padded_retention_times = vec![0.0; chrom.retention_times.len() + 2];
        let rt_diff = chrom.retention_times[chrom.retention_times.len() - 1]
            - chrom.retention_times[chrom.retention_times.len() - 2];

        let padded_rt_len = padded_retention_times.len();
        let padded_intensities_len = padded_intensities.len();
        padded_retention_times[0] = chrom.retention_times[0] - rt_diff;
        padded_retention_times[padded_rt_len - 1] =
            chrom.retention_times[chrom.retention_times.len() - 1] + rt_diff;
        padded_intensities[1..padded_intensities_len - 1].clone_from_slice(&chrom.intensities);
        padded_chromatograms.push(Chromatogram {
            id: chrom.id,
            native_id: chrom.native_id,
            retention_times: padded_retention_times,
            intensities: padded_intensities,
            metadata: chrom.metadata,
        });
    }
    padded_chromatograms
}


/// Struct for the aligned retention time pair.
#[derive(Debug, Clone, Default, Serialize, Deserialize, DeepSizeOf)]
pub struct AlignedRTPointPair {
    /// The retention time point of the first chromatogram (reference).
    pub rt1: f32,
    /// The retention time point of the second chromatogram (query aligned).
    pub rt2: f32
}

/// Represents the mapping of peaks across chromatograms.
#[derive(Debug, Clone, Serialize, Deserialize, DeepSizeOf)]
pub struct AlignedChromatogram {
    /// Aligned chromatogram
    pub chromatogram: Chromatogram,
    /// Optimal alignment path between the reference and query chromatograms (Only for DTW and FFT-DTW)
    pub alignment_path: Vec<(usize, usize)>,
    /// Lag between the reference and query chromatograms (Only for FFT and FFT-DTW)
    pub lag: Option<isize>,
    /// Mapping of retention times between the original and aligned chromatograms
    pub rt_mapping: Vec<AlignedRTPointPair>,
    /// basename of reference chromatogram
    pub reference_basename: String,
}

