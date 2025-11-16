use anyhow::Result;
use serde::{Deserialize, Deserializer, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum XicFileType {
    SqMass,
    Parquet,
    Unknown,
}

impl Default for XicFileType {
    fn default() -> Self {
        XicFileType::SqMass
    }
}

impl<'de> Deserialize<'de> for XicFileType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.to_lowercase().as_str() {
            "sqmass" => Ok(XicFileType::SqMass),
            "parquet" => Ok(XicFileType::Parquet),
            _ => Ok(XicFileType::Unknown),
        }
    }
}

impl XicFileType {
    pub fn as_str(&self) -> &str {
        match self {
            XicFileType::SqMass => "sqMass",
            XicFileType::Parquet => "parquet",
            XicFileType::Unknown => "Unknown",
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum FeaturesFileType {
    OSW,
    OSWPQ,
    Unknown,
}

impl Default for FeaturesFileType {
    fn default() -> Self {
        FeaturesFileType::OSW
    }
}

impl<'de> Deserialize<'de> for FeaturesFileType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.to_lowercase().as_str() {
            "osw" => Ok(FeaturesFileType::OSW),
            "oswpq" | "parquet" => Ok(FeaturesFileType::OSWPQ),
            _ => Ok(FeaturesFileType::Unknown),
        }
    }
}

impl FeaturesFileType {
    pub fn as_str(&self) -> &str {
        match self {
            FeaturesFileType::OSW => "osw",
            FeaturesFileType::OSWPQ => "oswpq",
            FeaturesFileType::Unknown => "Unknown",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct XicConfig {
    #[serde(rename = "include-precursor")]
    pub include_precursor: bool,
    #[serde(rename = "num-isotopes")]
    pub num_isotopes: usize,
    #[serde(rename = "file-type")]
    pub file_type: Option<XicFileType>,
    #[serde(rename = "file-paths")]
    pub file_paths: Vec<PathBuf>,
}

impl XicConfig {
    /// Get the number of file paths.
    pub fn len(&self) -> usize {
        self.file_paths.len()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FeaturesConfig {
    #[serde(rename = "file-type")]
    pub file_type: Option<FeaturesFileType>,
    #[serde(rename = "file-paths")]
    pub file_paths: Vec<PathBuf>,
}

impl FeaturesConfig {
    /// Get the number of file paths.
    pub fn len(&self) -> usize {
        self.file_paths.len()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FiltersConfig {
    pub decoy: bool,
    pub include_identifying_transitions: Option<bool>,
    pub max_score_ms2_qvalue: Option<f64>,
    /// TSV file containing the list of precursors to filter for.
    pub precursor_ids: Option<String>
}

impl Default for FiltersConfig {
    fn default() -> Self {
        FiltersConfig {
            decoy: true,
            include_identifying_transitions: Some(false),
            max_score_ms2_qvalue: Some(1.0),
            precursor_ids: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SmoothingConfig {
    #[serde(rename = "sgolay_window")]
    pub sgolay_window: usize,
    #[serde(rename = "sgolay_order")]
    pub sgolay_order: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlignmentConfig {
    /// Batch size of precursors to process before writing results to disk.
    pub batch_size: Option<usize>,
    /// Method to use for alignment. Current options are "FFT", "DTW", "FFTDTW"
    pub method: String,
    /// Type of reference to use for alignment. Current options are "star", "mst", "progressive"
    pub reference_type: String,
    /// Name of the run to use as the reference. If not provided, a run will be used. Only used when the reference type is "star".
    pub reference_run: Option<String>,
    /// Whether to use TIC for alignment. Currently not used. We always use TIC.
    #[serde(rename = "use_tic")]
    pub use_tic: bool,
    /// Smoothing configuration for the chromatograms.
    pub smoothing: SmoothingConfig,
    /// Retention time mapping tolerance in seconds for mapping aligned query peak to reference peak.
    pub rt_mapping_tolerance: Option<f64>,
    /// Method to use for mapping decoy peaks. Current options are "shuffle" and "random_region".
    #[serde(rename = "decoy_peak_mapping_method")]
    pub decoy_peak_mapping_method: String,
    /// Size of the window to use for the decoy peak mapping. Only used when the method is "random_region".
    pub decoy_window_size: Option<usize>,
    /// Optionally compute alignment scores for the full trace alignment and peak mapping. Default is true.
    pub compute_scores: Option<bool>,
    /// Optionally output the scores to a separate OSW file. Otherwise, the scores are added to the input OSW file.
    pub scores_output_file: Option<String>,
    /// Retain alignment path. Default is false. If include_identifying_transitions, this will be true, since we need to use the alignment path to apply the same alignment to the identifying transitions.
    #[serde(default)]
    pub retain_alignment_path: bool,
}

impl Default for AlignmentConfig {
    fn default() -> Self {
        AlignmentConfig {
            batch_size: Some(1000),
            method: "fftdtw".to_string(),
            reference_type: "star".to_string(),
            reference_run: None,
            use_tic: true,
            smoothing: SmoothingConfig {
                sgolay_window: 11,
                sgolay_order: 3,
            },
            rt_mapping_tolerance: Some(10.0),
            decoy_peak_mapping_method: "shuffle".to_string(),
            decoy_window_size: Some(30),
            compute_scores: Some(true),
            scores_output_file: None,
            retain_alignment_path: false,
        }
    }
}

impl std::fmt::Display for AlignmentConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "\n---- Alignment Config ----\n\
            batch_size: {}\n\
            method: {}\n\
            reference_type: {}\n\
            reference_run: {:?}\n\
            use_tic: {}\n\
            sgolay_window: {}\n\
            sgolay_order: {}\n\
            rt_mapping_tolerance: {}\n\
            decoy_peak_mapping_method: {}\n\
            decoy_window_size: {:?}\n\
            compute_scores: {:?}\n\
            scores_output_file: {:?}\n\
            -------------------------",
            self.batch_size.unwrap_or_default(),
            self.method,
            self.reference_type,
            self.reference_run,
            self.use_tic,
            self.smoothing.sgolay_window,
            self.smoothing.sgolay_order,
            self.rt_mapping_tolerance.unwrap_or_default(),
            self.decoy_peak_mapping_method,
            self.decoy_window_size.unwrap_or_default(),
            self.compute_scores.unwrap_or_default(),
            self.scores_output_file
        )
    }
}


// ****************************
// GUI Specific Configurations

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum PlotMode {
    Floating,
    EmbeddedGrid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VisualizationConfig {
    /// What the user typed to filter the peptide list
    pub peptide_filter: String,
    /// the user’s search text for the peptide dropdown
    pub peptide_search: String,
    /// the list of full peptide names (populated at load time)
    pub peptide_list: Vec<String>,
    /// charge states available for the selected peptide
    pub charge_list: Vec<usize>,
    /// what the user has selected
    pub selected_peptide: Option<String>,
    pub selected_charge: Option<usize>,
    /// Feature boundaries
    pub peakgroup_qvalue: Option<f32>,
    pub alignment_qvalue: Option<f32>,
    
    /// smoothing parameters
    pub smoothing_enabled: bool,
    pub sgolay_window: usize,
    pub sgolay_order: usize,
    pub link_axis_x: bool,
    pub link_axis_y: bool,
    pub link_cursor: bool,

    /// Plotting configuration
    pub show_background: bool,
    pub show_grid: bool,
    pub show_legend: bool,
    pub show_axis_labels: bool,
    pub plot_mode:  PlotMode,
    pub grid_rows:  usize,
    pub grid_cols:  usize,
}

impl Default for VisualizationConfig {
    fn default() -> Self {
        VisualizationConfig {
            peptide_filter: String::new(),
            peptide_search: String::new(),
            peptide_list: Vec::new(),
            charge_list: Vec::new(),
            selected_peptide: None,
            selected_charge: None,
            peakgroup_qvalue: None,
            alignment_qvalue: None,
            smoothing_enabled: true,
            sgolay_window: 11,
            sgolay_order: 3,
            link_axis_x: true,
            link_axis_y: false,
            link_cursor: true,
            show_background: true,
            show_grid: false,
            show_legend: true,
            show_axis_labels: true,
            plot_mode:       PlotMode::EmbeddedGrid,
            grid_rows:       1,
            grid_cols:       1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PQPConfig {
    /// List of file paths to process
    pub file_paths: Vec<PathBuf>,
    pub output_path: PathBuf,
    pub pqp_out_type: String, // "pqp", "tsv", "TraML"

    /// Mode: Generate PQP for main data searching, or generate iRT PQP
    pub main_mode: bool,
    pub n_concurrent_processes: usize,

    // === TargetedFileConverter params ===
    pub tfc_binary_path: PathBuf,
    pub tfc_enabled: bool,
    pub tfc_input: PathBuf,
    pub tfc_input_type: String,     // "tsv", "mrm", "pqp", "TraML"
    pub tfc_output: PathBuf,
    pub tfc_output_type: String,    // "tsv", "pqp", "TraML"
    pub tfc_threads: usize,
    pub tfc_legacy_traml_id: bool,
    pub tfc_advanced: String,       // any extra flags

    // === OpenSwathAssayGenerator params ===
    pub osg_binary_path: PathBuf,
    pub osg_enabled: bool,
    pub osg_input: PathBuf,
    pub osg_input_type: String,     
    pub osg_output: PathBuf,
    pub osg_min_transitions: usize,
    pub osg_max_transitions: usize,
    pub osg_allowed_fragment_types: String,   // e.g. "b,y"
    pub osg_allowed_fragment_charges: String, // e.g. "1,2"
    pub osg_enable_detection_specific_losses: bool,
    pub osg_enable_detection_unspecific_losses: bool,
    pub osg_enable_ipf: bool,
    pub osg_unimod_file: PathBuf,
    pub osg_advanced: String,

    // === OpenSwathDecoyGenerator params ===
    pub odg_binary_path: PathBuf,
    pub odg_enabled: bool,
    pub odg_input: PathBuf,
    pub odg_input_type: String,     
    pub odg_output: PathBuf,
    pub odg_method: String,         // "reverse", "pseudo-reverse", "shuffle", "shift"
    pub odg_decoy_tag: String,      // e.g. "_DECOY"
    pub odg_min_decoy_fraction: f32,
    pub odg_aim_decoy_fraction: f32,
    pub odg_shuffle_max_attempts: usize,
    pub odg_shuffle_sequence_identity_threshold: f32,
    pub odg_shift_precursor_mz_shift: f32,
    pub odg_shift_product_mz_shift: f32,
    pub odg_advanced: String,

    // === EasyPQP iRTs ===
    pub easypqp_binary_path: PathBuf,
    pub irt_reduce_enabled: bool,
    pub irt_input: PathBuf,
    pub irt_bins: usize,
    pub irt_num_peptides: usize,

}

impl Default for PQPConfig {
    fn default() -> Self {
        PQPConfig {
            file_paths: Vec::new(),
            output_path: PathBuf::new(),
            pqp_out_type: "pqp".into(),

            main_mode: true,
            n_concurrent_processes: 1,

            tfc_binary_path: PathBuf::new(),
            tfc_enabled: false,
            tfc_input: PathBuf::new(),
            tfc_input_type: "tsv".into(),
            tfc_output: PathBuf::new(),
            tfc_output_type: "pqp".into(),
            tfc_threads: 1,
            tfc_legacy_traml_id: false,
            tfc_advanced: String::new(),

            osg_binary_path: PathBuf::new(),
            osg_enabled: true,
            osg_input: PathBuf::new(),
            osg_input_type: "pqp".into(),
            osg_output: PathBuf::new(),
            osg_min_transitions: 6,
            osg_max_transitions: 6,
            osg_allowed_fragment_types: "b,y".into(),
            osg_allowed_fragment_charges: "1,2,3,4".into(),
            osg_enable_detection_specific_losses: false,
            osg_enable_detection_unspecific_losses: false,
            osg_enable_ipf: false,
            osg_unimod_file: PathBuf::new(),
            osg_advanced: String::new(),

            odg_binary_path: PathBuf::new(),
            odg_enabled: true,
            odg_input: PathBuf::new(),
            odg_input_type: "pqp".into(),
            odg_output: PathBuf::new(),
            odg_method: "shuffle".into(),
            odg_decoy_tag: "DECOY_".into(),
            odg_min_decoy_fraction: 0.8,
            odg_aim_decoy_fraction: 1.0,
            odg_shuffle_max_attempts: 30,
            odg_shuffle_sequence_identity_threshold: 0.5,
            odg_shift_precursor_mz_shift: 0.0,
            odg_shift_product_mz_shift: 20.0,
            odg_advanced: String::new(),

            easypqp_binary_path: PathBuf::new(),
            irt_reduce_enabled: false,
            irt_input: PathBuf::new(),
            irt_bins: 10,
            irt_num_peptides: 10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RawFileType {
    MzML,
    MzXML,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpenSwathConfig {
    // OpenSwathWorkflow Binary path
    pub binary_path: PathBuf,

    // Inputs
    #[serde(rename = "file-type")]
    pub file_type: Option<RawFileType>,
    #[serde(rename = "file-paths")]
    pub file_paths: Vec<PathBuf>,
    spectral_library_type: Option<String>,
    pub spectral_library_paths: Vec<PathBuf>,
    pub linear_irt_library_paths: Vec<PathBuf>,
    pub include_non_linear_irt: bool,
    pub nonlinear_irt_library_paths: Vec<PathBuf>,

    // Outputs
    #[serde(rename = "output-path")]
    pub output_path: PathBuf,
    pub output_file_type: String, 
    pub output_debug_files: bool,

    // Parameters
    pub enable_ms1: bool,
    pub enable_ipf: bool,
    pub is_pasef: bool,

    pub rt_extraction_window: f32,
    pub ion_mobility_window: f32,
    pub ms2_mz_extraction_window: f32,
    pub ms2_mz_extraction_window_unit: String,
    pub ms1_mz_extraction_window: f32,
    pub ms1_mz_extraction_window_unit: String,
    pub ms1_ion_mobility_extraction_window: f32,
    pub irt_mz_extraction_window: f32,
    pub irt_mz_extraction_window_unit: String,
    pub irt_ion_mobility_extraction_window: f32,
    pub mz_correction_function: String,

    pub read_options: String,
    pub temp_directory: Option<PathBuf>,
    pub batch_size: usize,
    pub threads: usize,
    pub outer_loop_threads: isize,

    // Common Advanced Parameters
    pub rt_normalization_alignment_method: String,
    pub rt_normalization_outlier_method: String,
    pub rt_normalization_estimate_best_peptides: bool,
    pub rt_normalization_lowess_span: f32,

    /// Additional flags to pass directly to the OpenSwathWorkflow binary
    pub advanced_params: String,

}

impl Default for OpenSwathConfig {
    fn default() -> Self {
        OpenSwathConfig {
            binary_path: PathBuf::from(""), 
            file_type: None,
            file_paths: Vec::new(),
            spectral_library_type: None,
            spectral_library_paths: Vec::new(),
            linear_irt_library_paths: Vec::new(),
            include_non_linear_irt: false,
            nonlinear_irt_library_paths: Vec::new(),
            output_path: PathBuf::from("./"),
            output_file_type: "osw".to_string(),
            output_debug_files: false,
            enable_ms1: true,
            enable_ipf: false,
            is_pasef: false,
            rt_extraction_window: 600.0,
            ion_mobility_window: 0.06,
            ms2_mz_extraction_window: 50.0,
            ms2_mz_extraction_window_unit: "ppm".to_string(),
            ms1_mz_extraction_window: 50.0,
            ms1_mz_extraction_window_unit: "ppm".to_string(),
            ms1_ion_mobility_extraction_window: 0.06,
            irt_mz_extraction_window: 50.0,
            irt_mz_extraction_window_unit: "ppm".to_string(),
            irt_ion_mobility_extraction_window: 0.06,
            mz_correction_function: "quadratic_regression_delta_ppm".to_string(),
            read_options: "cacheWorkingInMemory".to_string(),
            temp_directory: Some(PathBuf::from("./")),
            batch_size: 1000,
            threads: std::thread::available_parallelism().unwrap().get().saturating_sub(2).max(1),
            outer_loop_threads: -1,
            rt_normalization_alignment_method: "lowess".to_string(),
            rt_normalization_outlier_method: "none".to_string(),
            rt_normalization_estimate_best_peptides: false,
            rt_normalization_lowess_span: 0.05,
            advanced_params: String::new(),
        }
    }
}

/// Configuration for PyProphet execution
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PyProphetConfig {
    /// Path to the `pyprophet` executable
    pub binary_path: PathBuf,

    /// Main I/O paths
    pub file_paths: Vec<PathBuf>,

    /// Which main commands to run
    pub run_merge: bool,
    pub run_score: bool,
    pub run_inference: bool,
    pub run_export: bool,

    /// Merge options
    pub merge_osw_output: PathBuf,

    /// Scoring options
    pub score_ms1: bool,
    pub score_ms2: bool,
    pub score_transition: bool,
    pub score_alignment: bool,

    /// Score ms1 options
    pub classifier_ms1: String,
    pub xeval_num_iter_ms1: usize,
    pub ss_num_iter_ms1: usize,
    pub ss_initial_fdr_ms1: f32,
    pub ss_iteration_fdr_ms1: f32,
    /// Additional flags to append to the pyprophet call
    pub advanced_params_ms1: String,

    /// Score ms2 options
    pub classifier_ms2: String,
    pub integrate_ms1: bool,
    pub xeval_num_iter_ms2: usize,
    pub ss_num_iter_ms2: usize,
    pub ss_initial_fdr_ms2: f32,
    pub ss_iteration_fdr_ms2: f32,
    /// Additional flags to append to the pyprophet call
    pub advanced_params_ms2: String,

    /// Score transition options
    pub classifier_transition: String,
    pub xeval_num_iter_transition: usize,
    pub ss_num_iter_transition: usize,
    pub ss_initial_fdr_transition: f32,
    pub ss_iteration_fdr_transition: f32,
    /// Additional flags to append to the pyprophet call
    pub advanced_params_transition: String,

    /// Score alignment options
    pub classifier_alignment: String,
    pub xeval_num_iter_alignment: usize,
    pub ss_num_iter_alignment: usize,
    pub ss_initial_fdr_alignment: f32,
    pub ss_iteration_fdr_alignment: f32,
    /// Additional flags to append to the pyprophet call
    pub advanced_params_alignment: String,

    /// Inference Options
    pub run_infer_peptide: bool,
    pub run_infer_protein: bool,
    pub run_infer_gene: bool,
    pub run_infer_peptidoform: bool,

    pub infer_peptide_global: bool,
    pub infer_peptide_experiment_wide: bool,
    pub infer_peptide_run_specific: bool,
    pub infer_protein_global: bool,
    pub infer_protein_experiment_wide: bool,    
    pub infer_protein_run_specific: bool,
    pub infer_gene_global: bool,
    pub infer_gene_experiment_wide: bool,
    pub infer_gene_run_specific: bool,
    pub infer_peptidoform_ms1_scoring: bool,
    pub infer_peptidoform_ms2_scoring: bool,
    pub infer_peptidoform_max_precursor_pep: f32,
    pub infer_peptidoform_max_peakgroup_pep: f32,
    pub infer_peptidoform_max_precursor_peakgroup_pep: f32,
    pub infer_peptidoform_max_transition_pep: f32,
    pub infer_peptidoform_propagate_signal: bool,
    pub infer_peptidoform_max_alignment_pep: f32, 
    pub infer_peptidoform_advanced_params: String,

    /// Export Options
    pub export_output_path: PathBuf,
    pub export_tsv: bool,
    pub export_precursor_matrix: bool,
    pub export_peptide_matrix: bool,
    pub export_protein_matrix: bool,
    pub export_parquet: bool,
    pub ipf_max_peptidoform_pep: f32,
    pub max_rs_peakgroup_qvalue: f32,
    pub max_global_peptide_qvalue: f32,
    pub max_global_protein_qvalue: f32,
    pub top_n: usize,
    pub consistent_top_n: bool,
    pub normalization_method: String,
    /// Parquet export options
    pub pqpfile: PathBuf,
    pub export_parquet_output_path: PathBuf,
    pub split_transition_data: bool,
    pub split_runs: bool,

}

impl Default for PyProphetConfig {
    fn default() -> Self {
        PyProphetConfig {
            binary_path: PathBuf::new(),
            file_paths: Vec::new(),
            run_merge: false,
            run_score: true,
            run_inference: true,
            run_export: true,
            merge_osw_output: PathBuf::from("./merged.osw"),
            score_ms1: false,
            score_ms2: true,
            score_transition: false,
            score_alignment: false,
            classifier_ms1: "XGBoost".to_string(),
            xeval_num_iter_ms1: 10,
            ss_num_iter_ms1: 10,
            ss_initial_fdr_ms1: 0.15,
            ss_iteration_fdr_ms1: 0.05,
            advanced_params_ms1: String::new(),
            classifier_ms2: "XGBoost".to_string(),
            integrate_ms1: true,
            xeval_num_iter_ms2: 10,
            ss_num_iter_ms2: 10,
            ss_initial_fdr_ms2: 0.15,
            ss_iteration_fdr_ms2: 0.05,
            advanced_params_ms2: String::new(),
            classifier_transition: "XGBoost".to_string(),
            xeval_num_iter_transition: 10,
            ss_num_iter_transition: 10,
            ss_initial_fdr_transition: 0.15,
            ss_iteration_fdr_transition: 0.05,
            advanced_params_transition: String::new(),
            classifier_alignment: "XGBoost".to_string(),
            xeval_num_iter_alignment: 10,
            ss_num_iter_alignment: 10,
            ss_initial_fdr_alignment: 0.15,
            ss_iteration_fdr_alignment: 0.05,
            advanced_params_alignment: String::new(),
            run_infer_peptide: true,
            run_infer_protein: true,
            run_infer_gene: false,
            run_infer_peptidoform: false,
            infer_peptide_global: true,
            infer_peptide_experiment_wide: false,
            infer_peptide_run_specific: false,
            infer_protein_global: true,
            infer_protein_experiment_wide: false,
            infer_protein_run_specific: false,
            infer_gene_global: true,
            infer_gene_experiment_wide: false,
            infer_gene_run_specific: false,
            infer_peptidoform_ms1_scoring: false,
            infer_peptidoform_ms2_scoring: false,
            infer_peptidoform_max_precursor_pep: 0.7,
            infer_peptidoform_max_peakgroup_pep: 0.7,
            infer_peptidoform_max_precursor_peakgroup_pep: 0.4,
            infer_peptidoform_max_transition_pep: 0.6,
            infer_peptidoform_propagate_signal: false,
            infer_peptidoform_max_alignment_pep: 0.4,
            infer_peptidoform_advanced_params: String::new(),
            export_output_path: PathBuf::new(),
            export_tsv: true,
            export_precursor_matrix: false,
            export_peptide_matrix: true,
            export_protein_matrix: true,
            export_parquet: false,
            ipf_max_peptidoform_pep: 0.4,
            max_rs_peakgroup_qvalue: 0.05,
            max_global_peptide_qvalue: 0.01,
            max_global_protein_qvalue: 0.01,
            top_n: 3,
            consistent_top_n: true,
            normalization_method: "none".to_string(),
            pqpfile: PathBuf::new(),
            export_parquet_output_path: PathBuf::new(),
            split_transition_data: false,
            split_runs: false,
        }
    }
}
