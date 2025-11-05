#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]

use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use deepsize::DeepSizeOf;

pub mod config;
pub mod error;
pub mod chromatogram;


#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, DeepSizeOf)]
pub struct PrecursorXics {
    pub precursor_id: i32,
    pub smoothed_tics: Vec<chromatogram::Chromatogram>,
    pub common_rt_space: Vec<f64>,
    pub group_id: String,
    pub native_ids: Vec<String>,
}


#[derive(Debug, Clone, Default, Serialize, Deserialize, DeepSizeOf)]
pub struct AlignedTics {
    pub precursor_id: i32,
    pub group_id: String,
    pub common_rt_space: Vec<f64>,
    pub aligned_chromatograms: Vec<chromatogram::AlignedChromatogram>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, DeepSizeOf)]
pub struct PeakMappingScores {
    pub precursor_id: i32,
    pub mapped_peaks: HashMap<String, Vec<PeakMapping>>,
    pub alignment_scores: HashMap<String, Vec<FullTraceAlignmentScores>>,
    pub detecting_transition_scores: HashMap<String, Vec<PeakMapping>>,
    pub identifying_transition_scores: HashMap<String, Vec<AlignedTransitionScores>>,
}

/// Represents the result of the precursor alignment.
#[derive(Debug, Clone, Default, Serialize, Deserialize, DeepSizeOf)]
pub struct PrecursorAlignmentResult {
    pub alignment_scores: HashMap<String, FullTraceAlignmentScores>,
    pub detecting_peak_mappings: HashMap<String, Vec<PeakMapping>>,
    pub identifying_peak_mapping_scores: HashMap<String, Vec<AlignedTransitionScores>>,
}


/// Struct for the scores of the alignment of the full chromatogram.
#[derive(Debug, Clone, Default, Serialize, Deserialize, DeepSizeOf)]
pub struct FullTraceAlignmentScores {
    /// reference filename
    pub reference_filename: String,
    /// aligned filename
    pub aligned_filename: String,
    /// Scores for the alignment.
    pub xcorr_coelution_to_ref: f64,
    pub xcorr_shape_to_ref: f64,
    pub mi_to_ref: f64,
    pub xcorr_coelution_to_all: f64,
    pub xcorr_shape_to_all: f64,
    pub mi_to_all: f64,
}

/// Represents the mapping of peaks across chromatograms.
#[derive(Debug, Clone, Default, Serialize, Deserialize, DeepSizeOf)]
pub struct PeakMapping {
    /// Alignment ID to group the same peaks across runs.
    pub alignment_id: i64,
    /// Precursor ID
    pub precursor_id: i32,
    /// Run ID
    pub run_id: i64,
    /// Feature ID in the reference chromatogram.
    pub reference_feature_id: i64,
    /// Feature ID in the aligned chromatogram.
    pub aligned_feature_id: i64,
    /// Retention time in the reference chromatogram.
    pub reference_rt: f64,
    /// Retention time in the aligned chromatogram.
    pub aligned_rt: f64,
    /// Left width in the reference chromatogram.
    pub reference_left_width: f64,
    /// Right width in the reference chromatogram.
    pub reference_right_width: f64,
    /// Left width in the aligned chromatogram.
    pub aligned_left_width: f64,
    /// Right width in the aligned chromatogram.
    pub aligned_right_width: f64,
    /// Filename of the run that was used as the reference.
    pub reference_filename: String,
    /// Filename of the run that was aligned.
    pub aligned_filename: String,

    /// Label to group peaks based on target/decoy aligned peak
    pub label: i32,
    /// Scores for the peak matching.
    pub xcorr_coelution_to_ref: Option<f64>,
    pub xcorr_shape_to_ref: Option<f64>,
    pub mi_to_ref: Option<f64>,
    pub xcorr_coelution_to_all: Option<f64>,
    pub xcorr_shape_to_all: Option<f64>,
    pub mi_to_all: Option<f64>,
    pub rt_deviation: Option<f64>,
    pub intensity_ratio: Option<f64>,
}


/// Represents the scores for aligning peaks on the transition level.
#[derive(Debug, Clone, Default, Serialize, Deserialize, DeepSizeOf)]
pub struct AlignedTransitionScores {
    /// Feature ID
    pub feature_id: i64,
    /// Transition ID
    pub transition_id: i64,
    /// Run ID
    pub run_id: i64,
    /// Filename of the run that was aligned.
    pub aligned_filename: String,
    /// Label to group peaks based on target/decoy aligned peak
    pub label: i32,
    /// Scores for the peak matching.
    pub xcorr_coelution_to_ref: Option<f64>,
    pub xcorr_shape_to_ref: Option<f64>,
    pub mi_to_ref: Option<f64>,
    pub xcorr_coelution_to_all: Option<f64>,
    pub xcorr_shape_to_all: Option<f64>,
    pub mi_to_all: Option<f64>,
    pub rt_deviation: Option<f64>,
    pub intensity_ratio: Option<f64>,
}