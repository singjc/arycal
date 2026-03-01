use duckdb::{Connection, Result};
use std::collections::HashMap;

use crate::osw::PrecursorIdData;
use crate::sqmass::{decompress_data, TransitionGroup};
use crate::util::extract_basename;
use crate::ChromatogramReader;
use arycal_common::chromatogram::Chromatogram;

use duckdb::{self, Error as DuckDBError};
use std::error::Error;
use std::fmt;

/// Define a custom error type for DuckDB Parquet access
#[derive(Debug)]
pub enum OpenMSParquetError {
    DatabaseError(String),
    GeneralError(String),
    DuckDBError(DuckDBError),
    DecompressionError(String),
}

impl fmt::Display for OpenMSParquetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpenMSParquetError::DatabaseError(msg) => {
                write!(f, "[OpenMSParquetError] Database Error: {}", msg)
            }
            OpenMSParquetError::GeneralError(msg) => {
                write!(f, "[OpenMSParquetError] Error: {}", msg)
            }
            OpenMSParquetError::DuckDBError(err) => {
                write!(f, "[OpenMSParquetError] DuckDB Error: {}", err)
            }
            OpenMSParquetError::DecompressionError(msg) => {
                write!(f, "[OpenMSParquetError] Decompression Error: {}", msg)
            }
        }
    }
}

impl From<DuckDBError> for OpenMSParquetError {
    fn from(err: DuckDBError) -> OpenMSParquetError {
        OpenMSParquetError::DuckDBError(err)
    }
}

impl Error for OpenMSParquetError {}
unsafe impl Send for OpenMSParquetError {}
unsafe impl Sync for OpenMSParquetError {}

/// Reader for OpenMS XIC Parquet files using DuckDB
pub struct OpenMSXicParquetChromatogramReader {
    file: String,
    conn: Connection,
}

unsafe impl Send for OpenMSXicParquetChromatogramReader {}
unsafe impl Sync for OpenMSXicParquetChromatogramReader {}

impl ChromatogramReader for OpenMSXicParquetChromatogramReader {
    fn new(db_path: &str) -> anyhow::Result<Self> {
        log::trace!(
            "Creating OpenMSXicParquetChromatogramReader instance with path: {}",
            db_path
        );

        let conn = Connection::open_in_memory().map_err(|e| {
            OpenMSParquetError::DatabaseError(format!("Failed to create DuckDB connection: {}", e))
        })?;

        Ok(OpenMSXicParquetChromatogramReader {
            file: db_path.to_string(),
            conn,
        })
    }

    /// Read chromatograms by TRANSITION_ID values supplied as strings in filter_values.
    /// This is a pragmatic minimal implementation compatible with the schema described for OpenMS XIC parquet
    fn read_chromatograms(
        &self,
        filter_type: &str,
        filter_values: Vec<&str>,
        group_id: String,
    ) -> anyhow::Result<TransitionGroup> {
        if filter_type != "TRANSITION_ID" {
            return Err(OpenMSParquetError::GeneralError(
                "Only TRANSITION_ID filter is supported by OpenMS XIC reader".to_string(),
            )
            .into());
        }

        if filter_values.is_empty() {
            return Ok(TransitionGroup::new(group_id));
        }

        let placeholders = filter_values
            .iter()
            .map(|id| format!("{}", id.replace("'", "''")))
            .map(|id| format!("{}", id))
            .collect::<Vec<_>>()
            .join(",");

        // Use TRANSITION_ID integer column to match transition ids
        let query = format!(
            "SELECT TRANSITION_ID, PRECURSOR_ID, RT_DATA, INTENSITY_DATA, RT_COMPRESSION, INTENSITY_COMPRESSION FROM read_parquet('{}') WHERE TRANSITION_ID IN ({})",
            self.file, placeholders
        );

        let mut stmt = self.conn.prepare(&query)?;

        let mut transition_group = TransitionGroup::new(group_id);
        transition_group.add_metadata("file".to_string(), self.file.to_string());
        transition_group.add_metadata("basename".to_string(), extract_basename(&self.file));

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, Vec<u8>>(3)?,
                row.get::<_, i32>(4)?,
                row.get::<_, i32>(5)?,
            ))
        })?;

        for r in rows {
            let (transition_id, precursor_id_opt, rt_data, intensity_data, rt_comp, int_comp) = r?;

            // Decompress
            let retention_times = decompress_data(&rt_data, rt_comp)
                .map_err(|e| OpenMSParquetError::DecompressionError(e.to_string()))?;
            let intensities = decompress_data(&intensity_data, int_comp)
                .map_err(|e| OpenMSParquetError::DecompressionError(e.to_string()))?;

            let native_id = format!("transition:{}", transition_id);

            let chromatogram = transition_group
                .chromatograms
                .entry(native_id.clone())
                .or_insert(Chromatogram {
                    id: transition_id as i32,
                    native_id: native_id.clone(),
                    retention_times: Vec::new(),
                    intensities: Vec::new(),
                    metadata: HashMap::new(),
                });

            chromatogram.retention_times = retention_times;
            chromatogram.intensities = intensities;

            if let Some(pid) = precursor_id_opt {
                chromatogram.metadata.insert("precursor_id".to_string(), pid.to_string());
            }
        }

        Ok(transition_group)
    }

    fn read_chromatograms_for_precursors(
        &self,
        precursors: &[PrecursorIdData],
        include_precursor: bool,
        num_isotopes: usize,
    ) -> anyhow::Result<HashMap<i32, TransitionGroup>> {
        // Build map of precursor_id -> list of transition ids we care about.
        let precursor_to_transitions: HashMap<i64, Vec<i64>> = precursors
            .iter()
            .map(|p| (p.precursor_id as i64, Vec::new()))
            .collect();

        // For now, request all rows and filter by PRECURSOR_ID that match our precursors.
        let precursor_ids = precursors
            .iter()
            .map(|p| p.precursor_id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        if precursor_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let query = format!(
            "SELECT PRECURSOR_ID, TRANSITION_ID, RT_DATA, INTENSITY_DATA, RT_COMPRESSION, INTENSITY_COMPRESSION FROM read_parquet('{}') WHERE PRECURSOR_ID IN ({})",
            self.file, precursor_ids
        );

        let mut stmt = self.conn.prepare(&query)?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, Option<i64>>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, Vec<u8>>(3)?,
                row.get::<_, i32>(4)?,
                row.get::<_, i32>(5)?,
            ))
        })?;

        let mut groups: HashMap<i32, TransitionGroup> = HashMap::new();

        for r in rows {
            let (precursor_id_opt, transition_id_opt, rt_data, intensity_data, rt_comp, int_comp) = r?;
            if let (Some(precursor_id), Some(transition_id)) = (precursor_id_opt, transition_id_opt) {
                let pid = precursor_id as i32;

                let retention_times = decompress_data(&rt_data, rt_comp)
                    .map_err(|e| OpenMSParquetError::DecompressionError(e.to_string()))?;
                let intensities = decompress_data(&intensity_data, int_comp)
                    .map_err(|e| OpenMSParquetError::DecompressionError(e.to_string()))?;

                let native_id = format!("transition:{}", transition_id);

                let group = groups.entry(pid).or_insert_with(|| TransitionGroup::new(format!("precursor_{}", pid)));

                let chrom = group.chromatograms.entry(native_id.clone()).or_insert(Chromatogram {
                    id: transition_id as i32,
                    native_id: native_id.clone(),
                    retention_times: Vec::new(),
                    intensities: Vec::new(),
                    metadata: HashMap::new(),
                });

                chrom.retention_times = retention_times;
                chrom.intensities = intensities;
            }
        }

        // Debug: report how many groups/chromatograms were loaded
        log::trace!("OpenMS parquet: loaded {} chromatograms across {} precursor groups from {}",
            groups.values().map(|g| g.chromatograms.len()).sum::<usize>(), groups.len(), self.file);

        // Add metadata file & basename
        let basename = extract_basename(&self.file);
        let file_str = self.file.to_string();
        for group in groups.values_mut() {
            group.metadata.insert("file".to_string(), file_str.clone());
            group.metadata.insert("basename".to_string(), basename.clone());
        }

        Ok(groups)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke() {
        // Minimal smoke test ensuring the module compiles.
        let _ = 1 + 1;
    }
}
