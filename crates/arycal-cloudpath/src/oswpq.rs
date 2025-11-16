//! PyProphet Split Parquet Format (OSWPQ/OSWPQD) Reader
//!
//! This module provides functionality to read PyProphet's split parquet format,
//! which stores features and scores in parquet files instead of SQLite.
//!
//! Structure:
//! ```
//! merged_runs.oswpqd/
//!   ├── run1.oswpq/
//!   │   ├── precursors_features.parquet
//!   │   └── transition_features.parquet
//!   ├── run2.oswpq/
//!   │   ├── precursors_features.parquet
//!   │   └── transition_features.parquet
//!   └── ...
//! ```

use duckdb::{Connection, Result as DuckDbResult};
use std::path::{Path, PathBuf};
use std::fs;
use std::fmt;
use std::error::Error;
use std::collections::HashMap;

// Import types from the osw module that we need
use crate::osw::{PrecursorIdData, FeatureData, ValueEntryType};
// Import from arycal_common for PeakMapping and AlignedTransitionScores
use arycal_common::{PeakMapping, AlignedTransitionScores};

/// Custom error type for OSWPQ operations
#[derive(Debug)]
pub enum OpenSwathParquetError {
    DatabaseError(String),
    FileNotFound(String),
    InvalidFormat(String),
    IoError(String),
}

impl fmt::Display for OpenSwathParquetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpenSwathParquetError::DatabaseError(msg) => write!(f, "[OpenSwathParquetError] Database Error: {}", msg),
            OpenSwathParquetError::FileNotFound(msg) => write!(f, "[OpenSwathParquetError] File Not Found: {}", msg),
            OpenSwathParquetError::InvalidFormat(msg) => write!(f, "[OpenSwathParquetError] Invalid Format: {}", msg),
            OpenSwathParquetError::IoError(msg) => write!(f, "[OpenSwathParquetError] IO Error: {}", msg),
        }
    }
}

impl Error for OpenSwathParquetError {}

impl From<duckdb::Error> for OpenSwathParquetError {
    fn from(err: duckdb::Error) -> OpenSwathParquetError {
        OpenSwathParquetError::DatabaseError(err.to_string())
    }
}

impl From<std::io::Error> for OpenSwathParquetError {
    fn from(err: std::io::Error) -> OpenSwathParquetError {
        OpenSwathParquetError::IoError(err.to_string())
    }
}

/// Represents a PyProphet split parquet directory structure
pub struct OswpqAccess {
    /// Path to the .oswpqd directory
    pub base_path: PathBuf,
}

/// Feature data from precursors_features.parquet
#[derive(Debug, Clone)]
pub struct PrecursorFeature {
    pub protein_id: Option<i64>,
    pub peptide_id: Option<i64>,
    pub ipf_peptide_id: Option<i64>,
    pub precursor_id: i64,
    pub run_id: i64,
    pub filename: String,
    pub feature_id: i64,
    pub exp_rt: f64,
    pub left_width: f64,
    pub right_width: f64,
    pub feature_ms2_area_intensity: Option<f64>,
    pub score_ms2_q_value: Option<f64>,
    pub score_ms2_pep: Option<f64>,
    pub precursor_decoy: Option<i64>,
}

/// Alignment feature data for writing to feature_alignment.parquet
#[derive(Debug, Clone)]
pub struct AlignmentFeature {
    pub alignment_id: i64,
    pub run_id: i64,
    pub precursor_id: i64,
    pub feature_id: i64,
    pub reference_feature_id: i64,
    pub aligned_rt: f64,
    pub reference_rt: f64,
    pub var_xcorr_coelution_to_reference: Option<f64>,
    pub var_xcorr_shape_to_reference: Option<f64>,
    pub var_mi_to_reference: Option<f64>,
    pub var_xcorr_coelution_to_all: Option<f64>,
    pub var_xcorr_shape: Option<f64>,
    pub var_mi_to_all: Option<f64>,
    pub var_retention_time_deviation: Option<f64>,
    pub var_peak_intensity_ratio: Option<f64>,
    pub decoy: i64,
}

impl OswpqAccess {
    /// Create a new OswpqAccess instance
    ///
    /// # Arguments
    /// * `base_path` - Path to the .oswpqd directory
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self, OpenSwathParquetError> {
        let base_path = base_path.as_ref().to_path_buf();
        
        // Verify the path exists and is a directory
        if !base_path.exists() {
            return Err(OpenSwathParquetError::FileNotFound(
                base_path.display().to_string(),
            ));
        }
        
        if !base_path.is_dir() {
            return Err(OpenSwathParquetError::InvalidFormat(
                "OSWPQD path must be a directory".to_string(),
            ));
        }

        Ok(Self {
            base_path,
        })
    }

    /// Create a DuckDB connection for querying
    fn create_connection(&self) -> Result<Connection, OpenSwathParquetError> {
        Connection::open_in_memory()
            .map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))
    }

    /// List all run directories (.oswpq) in the base directory
    pub fn list_runs(&self) -> Result<Vec<String>, OpenSwathParquetError> {
        let mut runs = Vec::new();
        
        let entries = fs::read_dir(&self.base_path)
            .map_err(|e| OpenSwathParquetError::IoError(e.to_string()))?;

        for entry in entries {
            let entry = entry.map_err(|e| OpenSwathParquetError::IoError(e.to_string()))?;
            let path = entry.path();
            
            if path.is_dir() {
                if let Some(name) = path.file_name() {
                    let name_str = name.to_string_lossy().to_string();
                    if name_str.ends_with(".oswpq") {
                        runs.push(name_str);
                    }
                }
            }
        }

        Ok(runs)
    }

    /// Get the path to a specific run's precursors_features.parquet file
    pub fn get_precursors_features_path(&self, run_name: &str) -> PathBuf {
        self.base_path
            .join(run_name)
            .join("precursors_features.parquet")
    }

    /// Get the path to a specific run's transition_features.parquet file
    pub fn get_transition_features_path(&self, run_name: &str) -> PathBuf {
        self.base_path
            .join(run_name)
            .join("transition_features.parquet")
    }

    /// Fetch precursor features for a specific precursor ID across all runs
    pub fn fetch_precursor_features_for_runs(
        &self,
        precursor_id: i32,
        runs: Vec<String>,
    ) -> Result<Vec<PrecursorFeature>, OpenSwathParquetError> {
        let mut all_features = Vec::new();
        let conn = self.create_connection()?;

        for run_name in runs {
            let precursor_path = self.get_precursors_features_path(&run_name);
            
            if !precursor_path.exists() {
                log::warn!(
                    "Precursors features file not found for run {}: {}",
                    run_name,
                    precursor_path.display()
                );
                continue;
            }

            let query = format!(
                r#"
                SELECT 
                    PROTEIN_ID,
                    PEPTIDE_ID,
                    IPF_PEPTIDE_ID,
                    PRECURSOR_ID,
                    RUN_ID,
                    FILENAME,
                    FEATURE_ID,
                    EXP_RT,
                    LEFT_WIDTH,
                    RIGHT_WIDTH,
                    FEATURE_MS2_AREA_INTENSITY,
                    SCORE_MS2_Q_VALUE,
                    SCORE_MS2_PEP,
                    PRECURSOR_DECOY
                FROM read_parquet('{}')
                WHERE PRECURSOR_ID = {}
                ORDER BY EXP_RT
                "#,
                precursor_path.display(),
                precursor_id
            );

            let mut stmt = conn
                .prepare(&query)
                .map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;

            let features: Vec<PrecursorFeature> = stmt
                .query_map([], |row| {
                    Ok(PrecursorFeature {
                        protein_id: row.get(0)?,
                        peptide_id: row.get(1)?,
                        ipf_peptide_id: row.get(2)?,
                        precursor_id: row.get(3)?,
                        run_id: row.get(4)?,
                        filename: row.get(5)?,
                        feature_id: row.get(6)?,
                        exp_rt: row.get(7)?,
                        left_width: row.get(8)?,
                        right_width: row.get(9)?,
                        feature_ms2_area_intensity: row.get(10)?,
                        score_ms2_q_value: row.get(11)?,
                        score_ms2_pep: row.get(12)?,
                        precursor_decoy: row.get(13)?,
                    })
                })
                .map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?
                .collect::<DuckDbResult<Vec<_>>>()
                .map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;

            all_features.extend(features);
        }

        Ok(all_features)
    }

    /// Fetch all unique precursor IDs from the parquet files
    pub fn fetch_all_precursor_ids(&self) -> Result<Vec<i32>, OpenSwathParquetError> {
        let runs = self.list_runs()?;
        
        if runs.is_empty() {
            return Ok(Vec::new());
        }

        let conn = self.create_connection()?;

        // Build a query that unions all precursor IDs from all runs
        let mut union_queries = Vec::new();
        
        for run_name in &runs {
            let precursor_path = self.get_precursors_features_path(run_name);
            
            if precursor_path.exists() {
                union_queries.push(format!(
                    "SELECT DISTINCT PRECURSOR_ID FROM read_parquet('{}')",
                    precursor_path.display()
                ));
            }
        }

        if union_queries.is_empty() {
            return Ok(Vec::new());
        }

        let query = format!(
            "SELECT DISTINCT PRECURSOR_ID FROM ({}) ORDER BY PRECURSOR_ID",
            union_queries.join(" UNION ALL ")
        );

        let mut stmt = conn
            .prepare(&query)
            .map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;

        let precursor_ids: Vec<i32> = stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?
            .collect::<DuckDbResult<Vec<_>>>()
            .map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;

        Ok(precursor_ids)
    }

    /// Write alignment features to feature_alignment.parquet
    pub fn write_alignment_features(
        &self,
        features: &[AlignmentFeature],
    ) -> Result<(), OpenSwathParquetError> {
        if features.is_empty() {
            log::warn!("No alignment features to write");
            return Ok(());
        }

        let output_path = self.base_path.join("feature_alignment.parquet");
        let conn = self.create_connection()?;

        // Create a temporary table with the alignment data
        conn
            .execute(
                r#"
                CREATE TEMPORARY TABLE alignment_temp (
                    ALIGNMENT_ID BIGINT,
                    RUN_ID BIGINT,
                    PRECURSOR_ID BIGINT,
                    FEATURE_ID BIGINT,
                    REFERENCE_FEATURE_ID BIGINT,
                    ALIGNED_RT DOUBLE,
                    REFERENCE_RT DOUBLE,
                    VAR_XCORR_COELUTION_TO_REFERENCE DOUBLE,
                    VAR_XCORR_SHAPE_TO_REFERENCE DOUBLE,
                    VAR_MI_TO_REFERENCE DOUBLE,
                    VAR_XCORR_COELUTION_TO_ALL DOUBLE,
                    VAR_XCORR_SHAPE DOUBLE,
                    VAR_MI_TO_ALL DOUBLE,
                    VAR_RETENTION_TIME_DEVIATION DOUBLE,
                    VAR_PEAK_INTENSITY_RATIO DOUBLE,
                    DECOY BIGINT
                )
                "#,
                [],
            )
            .map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;

        // Insert features in batches
        let batch_size = 1000;
        for chunk in features.chunks(batch_size) {
            let mut values = Vec::new();
            
            for feature in chunk {
                values.push(format!(
                    "({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                    feature.alignment_id,
                    feature.run_id,
                    feature.precursor_id,
                    feature.feature_id,
                    feature.reference_feature_id,
                    feature.aligned_rt,
                    feature.reference_rt,
                    feature.var_xcorr_coelution_to_reference.map_or("NULL".to_string(), |v| v.to_string()),
                    feature.var_xcorr_shape_to_reference.map_or("NULL".to_string(), |v| v.to_string()),
                    feature.var_mi_to_reference.map_or("NULL".to_string(), |v| v.to_string()),
                    feature.var_xcorr_coelution_to_all.map_or("NULL".to_string(), |v| v.to_string()),
                    feature.var_xcorr_shape.map_or("NULL".to_string(), |v| v.to_string()),
                    feature.var_mi_to_all.map_or("NULL".to_string(), |v| v.to_string()),
                    feature.var_retention_time_deviation.map_or("NULL".to_string(), |v| v.to_string()),
                    feature.var_peak_intensity_ratio.map_or("NULL".to_string(), |v| v.to_string()),
                    feature.decoy,
                ));
            }

            let insert_query = format!(
                "INSERT INTO alignment_temp VALUES {}",
                values.join(", ")
            );

            conn
                .execute(&insert_query, [])
                .map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;
        }

        // Export to parquet
        let export_query = format!(
            "COPY alignment_temp TO '{}' (FORMAT PARQUET)",
            output_path.display()
        );

        conn
            .execute(&export_query, [])
            .map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;

        log::info!(
            "Wrote {} alignment features to {}",
            features.len(),
            output_path.display()
        );

        Ok(())
    }

    /// Fetch transition IDs for precursors (equivalent to OSW's fetch_transition_ids)
    /// 
    /// This method queries the precursors_features.parquet files to extract precursor metadata
    /// Note: OSWPQ format doesn't have separate transition tables in the same way as OSW,
    /// so this implementation focuses on precursor-level data.
    pub fn fetch_transition_ids(
        &self,
        filter_decoys: bool,
        _include_identifying_transitions: bool,
        precursor_ids: Option<Vec<u32>>,
    ) -> Result<Vec<PrecursorIdData>, OpenSwathParquetError> {
        let runs = self.list_runs()?;
        
        if runs.is_empty() {
            log::warn!("No runs found in OSWPQ directory");
            return Ok(Vec::new());
        }

        let conn = self.create_connection()?;
        let mut precursor_map: HashMap<i32, PrecursorIdData> = HashMap::new();

        // Query each run's precursor features
        for run_name in &runs {
            let precursor_path = self.get_precursors_features_path(run_name);
            
            if !precursor_path.exists() {
                log::warn!("Precursors features file not found for run {}", run_name);
                continue;
            }

            // Build the query
            let mut query = format!(
                r#"
                SELECT DISTINCT
                    PRECURSOR_ID,
                    MODIFIED_SEQUENCE,
                    PRECURSOR_CHARGE,
                    PRECURSOR_DECOY
                FROM read_parquet('{}')
                WHERE 1=1
                "#,
                precursor_path.display()
            );

            // Add decoy filter if needed
            if filter_decoys {
                query.push_str(" AND (PRECURSOR_DECOY = 0 OR PRECURSOR_DECOY IS NULL)");
            }

            // Add precursor ID filter if provided
            if let Some(ref ids) = precursor_ids {
                let id_list = ids.iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                query.push_str(&format!(" AND PRECURSOR_ID IN ({})", id_list));
            }

            let mut stmt = conn.prepare(&query)
                .map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;

            let precursor_iter = stmt.query_map([], |row| {
                let precursor_id: i32 = row.get(0)?;
                let modified_sequence: String = row.get(1)?;
                let precursor_charge: i32 = row.get(2)?;
                let decoy: Option<i32> = row.get(3)?;
                
                Ok((precursor_id, modified_sequence, precursor_charge, decoy.unwrap_or(0) == 1))
            }).map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;

            for result in precursor_iter {
                let (precursor_id, modified_sequence, precursor_charge, decoy) = result
                    .map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;

                precursor_map.entry(precursor_id).or_insert_with(|| {
                    // OSWPQ doesn't have unmodified sequence readily available
                    let unmodified_sequence = modified_sequence.clone(); // TODO: Strip modifications
                    PrecursorIdData::new(
                        precursor_id,
                        unmodified_sequence,
                        modified_sequence,
                        precursor_charge,
                        decoy,
                    )
                });
            }
        }

        Ok(precursor_map.into_values().collect())
    }

    /// Fetch full precursor feature data for specific runs
    pub fn fetch_full_precursor_feature_data_for_runs(
        &self,
        precursor_id: i32,
        runs: Vec<String>,
    ) -> Result<Vec<FeatureData>, OpenSwathParquetError> {
        let conn = self.create_connection()?;
        let mut feature_data_map: HashMap<String, FeatureData> = HashMap::new();

        for run_name in runs {
            let precursor_path = self.get_precursors_features_path(&run_name);
            
            if !precursor_path.exists() {
                log::warn!("Precursors features file not found for run {}", run_name);
                continue;
            }

            let query = format!(
                r#"
                SELECT 
                    FILENAME,
                    RUN_ID,
                    PRECURSOR_ID,
                    FEATURE_ID,
                    EXP_RT,
                    LEFT_WIDTH,
                    RIGHT_WIDTH,
                    FEATURE_MS2_AREA_INTENSITY
                FROM read_parquet('{}')
                WHERE PRECURSOR_ID = ?
                ORDER BY EXP_RT
                "#,
                precursor_path.display()
            );

            let mut stmt = conn.prepare(&query)
                .map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;

            let feature_iter = stmt.query_map([precursor_id], |row| {
                let filename: String = row.get(0)?;
                let run_id: i64 = row.get(1)?;
                let precursor_id: i32 = row.get(2)?;
                let feature_id: i64 = row.get(3)?;
                let exp_rt: f64 = row.get(4)?;
                let left_width: f64 = row.get(5)?;
                let right_width: f64 = row.get(6)?;
                let intensity: Option<f64> = row.get(7)?;
                
                Ok((filename, run_id, precursor_id, feature_id, exp_rt, left_width, right_width, intensity))
            }).map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;

            for result in feature_iter {
                let (filename, run_id, precursor_id, feature_id, exp_rt, left_width, right_width, intensity) = result
                    .map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;

                let entry = feature_data_map.entry(filename.clone()).or_insert_with(|| {
                    FeatureData::new(
                        filename.clone(),
                        run_id,
                        precursor_id,
                        Some(ValueEntryType::Multiple(vec![])),
                        ValueEntryType::Multiple(vec![]),
                        Some(ValueEntryType::Multiple(vec![])),
                        Some(ValueEntryType::Multiple(vec![])),
                        Some(ValueEntryType::Multiple(vec![])),
                        None, // rank not available in OSWPQ
                        None, // qvalue not readily available
                        None, // normalized_summed_intensity
                    )
                });

                // Push values into their respective vectors
                if let Some(ValueEntryType::Multiple(ref mut ids)) = entry.feature_id {
                    ids.push(feature_id);
                }

                if let ValueEntryType::Multiple(ref mut exps) = entry.exp_rt {
                    exps.push(exp_rt);
                }

                if let Some(ValueEntryType::Multiple(ref mut widths)) = entry.left_width {
                    widths.push(left_width);
                }

                if let Some(ValueEntryType::Multiple(ref mut widths)) = entry.right_width {
                    widths.push(right_width);
                }

                if let Some(ValueEntryType::Multiple(ref mut intensities)) = entry.intensity {
                    if let Some(int) = intensity {
                        intensities.push(int);
                    }
                }
            }
        }

        Ok(feature_data_map.into_values().collect())
    }

    /// Fetch feature data for a batch of precursors
    pub fn fetch_feature_data_for_precursor_batch(
        &self,
        precursor_run_sets: &[(i32, Vec<String>)],
    ) -> Result<HashMap<i32, Vec<FeatureData>>, OpenSwathParquetError> {
        let conn = self.create_connection()?;
        let mut result_map: HashMap<i32, Vec<FeatureData>> = HashMap::new();

        // Process each precursor
        for (precursor_id, runs) in precursor_run_sets {
            let feature_data = self.fetch_full_precursor_feature_data_for_runs(*precursor_id, runs.clone())?;
            result_map.insert(*precursor_id, feature_data);
        }

        Ok(result_map)
    }

    /// Create MS2 alignment table (writes to parquet file)
    /// Note: OSWPQ format uses parquet files, so this just ensures the directory structure exists
    pub fn create_feature_ms2_alignment_table(&self) -> Result<(), OpenSwathParquetError> {
        // In OSWPQ format, we just need to ensure the base directory exists
        // The actual writing happens in write methods
        log::info!("OSWPQ: MS2 alignment will be written to parquet file");
        Ok(())
    }

    /// Create transition alignment table (writes to parquet file)
    pub fn create_feature_transition_alignment_table(&self) -> Result<(), OpenSwathParquetError> {
        log::info!("OSWPQ: Transition alignment will be written to parquet file");
        Ok(())
    }

    /// Write MS2 alignment batch to parquet file
    pub fn write_ms2_alignment_batch(
        &self,
        peak_mappings: &[PeakMapping],
    ) -> Result<(), OpenSwathParquetError> {
        if peak_mappings.is_empty() {
            return Ok(());
        }

        let output_path = self.base_path.join("feature_ms2_alignment.parquet");
        let conn = self.create_connection()?;

        // Create temporary table
        conn.execute(
            r#"
            CREATE TEMPORARY TABLE ms2_alignment_temp (
                ALIGNMENT_ID BIGINT,
                PRECURSOR_ID BIGINT,
                RUN_ID BIGINT,
                REFERENCE_FEATURE_ID BIGINT,
                ALIGNED_FEATURE_ID BIGINT,
                REFERENCE_RT DOUBLE,
                ALIGNED_RT DOUBLE,
                REFERENCE_LEFT_WIDTH DOUBLE,
                REFERENCE_RIGHT_WIDTH DOUBLE,
                ALIGNED_LEFT_WIDTH DOUBLE,
                ALIGNED_RIGHT_WIDTH DOUBLE,
                REFERENCE_FILENAME VARCHAR,
                ALIGNED_FILENAME VARCHAR,
                XCORR_COELUTION_TO_REFERENCE DOUBLE,
                XCORR_SHAPE_TO_REFERENCE DOUBLE,
                MI_TO_REFERENCE DOUBLE,
                XCORR_COELUTION_TO_ALL DOUBLE,
                XCORR_SHAPE_TO_ALL DOUBLE,
                MI_TO_ALL DOUBLE,
                RETENTION_TIME_DEVIATION DOUBLE,
                PEAK_INTENSITY_RATIO DOUBLE,
                LABEL BIGINT
            )
            "#,
            [],
        ).map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;

        // Insert data in batches
        let batch_size = 1000;
        for chunk in peak_mappings.chunks(batch_size) {
            let mut values = Vec::new();
            
            for pm in chunk {
                values.push(format!(
                    "({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, '{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, {})",
                    pm.alignment_id,
                    pm.precursor_id,
                    pm.run_id,
                    pm.reference_feature_id,
                    pm.aligned_feature_id,
                    pm.reference_rt,
                    pm.aligned_rt,
                    pm.reference_left_width,
                    pm.reference_right_width,
                    pm.aligned_left_width,
                    pm.aligned_right_width,
                    pm.reference_filename.replace("'", "''"),
                    pm.aligned_filename.replace("'", "''"),
                    pm.xcorr_coelution_to_ref.map_or("NULL".to_string(), |v| v.to_string()),
                    pm.xcorr_shape_to_ref.map_or("NULL".to_string(), |v| v.to_string()),
                    pm.mi_to_ref.map_or("NULL".to_string(), |v| v.to_string()),
                    pm.xcorr_coelution_to_all.map_or("NULL".to_string(), |v| v.to_string()),
                    pm.xcorr_shape_to_all.map_or("NULL".to_string(), |v| v.to_string()),
                    pm.mi_to_all.map_or("NULL".to_string(), |v| v.to_string()),
                    pm.rt_deviation.map_or("NULL".to_string(), |v| v.to_string()),
                    pm.intensity_ratio.map_or("NULL".to_string(), |v| v.to_string()),
                    pm.label,
                ));
            }

            let insert_query = format!(
                "INSERT INTO ms2_alignment_temp VALUES {}",
                values.join(", ")
            );

            conn.execute(&insert_query, [])
                .map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;
        }

        // Export to parquet (append mode if file exists)
        let export_query = if output_path.exists() {
            // Read existing data, union with new data, and overwrite
            format!(
                "COPY (SELECT * FROM read_parquet('{}') UNION ALL SELECT * FROM ms2_alignment_temp) TO '{}' (FORMAT PARQUET)",
                output_path.display(),
                output_path.display()
            )
        } else {
            format!(
                "COPY ms2_alignment_temp TO '{}' (FORMAT PARQUET)",
                output_path.display()
            )
        };

        conn.execute(&export_query, [])
            .map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;

        log::info!(
            "Wrote {} MS2 alignment records to {}",
            peak_mappings.len(),
            output_path.display()
        );

        Ok(())
    }

    /// Write transition alignment batch to parquet file
    pub fn write_transition_alignment_batch(
        &self,
        transition_scores: &[AlignedTransitionScores],
    ) -> Result<(), OpenSwathParquetError> {
        if transition_scores.is_empty() {
            return Ok(());
        }

        let output_path = self.base_path.join("feature_transition_alignment.parquet");
        let conn = self.create_connection()?;

        // Create temporary table
        conn.execute(
            r#"
            CREATE TEMPORARY TABLE transition_alignment_temp (
                FEATURE_ID BIGINT,
                TRANSITION_ID BIGINT,
                RUN_ID BIGINT,
                ALIGNED_FILENAME VARCHAR,
                LABEL BIGINT,
                XCORR_COELUTION_TO_REFERENCE DOUBLE,
                XCORR_SHAPE_TO_REFERENCE DOUBLE,
                MI_TO_REFERENCE DOUBLE,
                XCORR_COELUTION_TO_ALL DOUBLE,
                XCORR_SHAPE_TO_ALL DOUBLE,
                MI_TO_ALL DOUBLE,
                RETENTION_TIME_DEVIATION DOUBLE,
                PEAK_INTENSITY_RATIO DOUBLE
            )
            "#,
            [],
        ).map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;

        // Insert data in batches
        let batch_size = 1000;
        for chunk in transition_scores.chunks(batch_size) {
            let mut values = Vec::new();
            
            for ts in chunk {
                values.push(format!(
                    "({}, {}, {}, '{}', {}, {}, {}, {}, {}, {}, {}, {}, {})",
                    ts.feature_id,
                    ts.transition_id,
                    ts.run_id,
                    ts.aligned_filename.replace("'", "''"),
                    ts.label,
                    ts.xcorr_coelution_to_ref.map_or("NULL".to_string(), |v| v.to_string()),
                    ts.xcorr_shape_to_ref.map_or("NULL".to_string(), |v| v.to_string()),
                    ts.mi_to_ref.map_or("NULL".to_string(), |v| v.to_string()),
                    ts.xcorr_coelution_to_all.map_or("NULL".to_string(), |v| v.to_string()),
                    ts.xcorr_shape_to_all.map_or("NULL".to_string(), |v| v.to_string()),
                    ts.mi_to_all.map_or("NULL".to_string(), |v| v.to_string()),
                    ts.rt_deviation.map_or("NULL".to_string(), |v| v.to_string()),
                    ts.intensity_ratio.map_or("NULL".to_string(), |v| v.to_string()),
                ));
            }

            let insert_query = format!(
                "INSERT INTO transition_alignment_temp VALUES {}",
                values.join(", ")
            );

            conn.execute(&insert_query, [])
                .map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;
        }

        // Export to parquet (append mode if file exists)
        let export_query = if output_path.exists() {
            format!(
                "COPY (SELECT * FROM read_parquet('{}') UNION ALL SELECT * FROM transition_alignment_temp) TO '{}' (FORMAT PARQUET)",
                output_path.display(),
                output_path.display()
            )
        } else {
            format!(
                "COPY transition_alignment_temp TO '{}' (FORMAT PARQUET)",
                output_path.display()
            )
        };

        conn.execute(&export_query, [])
            .map_err(|e| OpenSwathParquetError::DatabaseError(e.to_string()))?;

        log::info!(
            "Wrote {} transition alignment records to {}",
            transition_scores.len(),
            output_path.display()
        );

        Ok(())
    }

    /// Get the base path of the OSWPQD directory
    pub fn get_base_path(&self) -> &Path {
        &self.base_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oswpq_access_creation() {
        // This test would need actual test data
        // For now, just test error handling
        let result = OswpqAccess::new("/nonexistent/path");
        assert!(result.is_err());
    }
}
