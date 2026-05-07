use anyhow::Result;
use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, ListArray, StringArray};
use arrow::datatypes::{DataType, Field, Float64Type, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use arycal_common::chromatogram::AlignedChromatogram;
use arycal_common::PeakMapping;

/// Writes the mapped peaks to a Parquet file.
///
/// # Parameters
/// - `mapped_prec_peaks`: A HashMap containing the mapped peaks.
/// - `output_path`: The path to the output Parquet file.
///
/// # Returns
/// A Result containing the success status of the operation.
pub fn write_mapped_peaks_to_parquet(
    mapped_prec_peaks: HashMap<String, Vec<PeakMapping>>,
    output_path: &str,
) -> Result<()> {
    // Define the schema
    let schema = Schema::new(vec![
        Field::new("alignment_id", DataType::Int64, false),
        Field::new("filename", DataType::Utf8, false),
        Field::new("reference_feature_id", DataType::Int64, false),
        Field::new("aligned_feature_id", DataType::Int64, false),
        Field::new("reference_rt", DataType::Float64, false),
        Field::new("aligned_rt", DataType::Float64, false),
        Field::new("reference_left_width", DataType::Float64, false),
        Field::new("reference_right_width", DataType::Float64, false),
        Field::new("aligned_left_width", DataType::Float64, false),
        Field::new("aligned_right_width", DataType::Float64, false),
        Field::new("label", DataType::Int32, true),
        Field::new("xcorr_coelution_to_ref", DataType::Float64, true),
        Field::new("xcorr_shape_to_ref", DataType::Float64, true),
        Field::new("mi_to_ref", DataType::Float64, true),
        Field::new("xcorr_coelution_to_all", DataType::Float64, true),
        Field::new("xcorr_shape_to_all", DataType::Float64, true),
        Field::new("mi_to_all", DataType::Float64, true),
        Field::new("retention_time_deviation", DataType::Float64, true),
        Field::new("intensity_ratio", DataType::Float64, true),
    ]);

    // Prepare arrays for each column
    let mut alignment_ids = Vec::new();
    let mut filenames = Vec::new();
    let mut reference_feature_ids = Vec::new();
    let mut aligned_feature_ids = Vec::new();
    let mut reference_rts = Vec::new();
    let mut aligned_rts = Vec::new();
    let mut reference_left_widths = Vec::new();
    let mut reference_right_widths = Vec::new();
    let mut aligned_left_widths = Vec::new();
    let mut aligned_right_widths = Vec::new();
    let mut labels = Vec::new();
    let mut xcorr_coelution_to_ref = Vec::new();
    let mut xcorr_shape_to_ref = Vec::new();
    let mut mi_to_ref = Vec::new();
    let mut xcorr_coelution_to_all = Vec::new();
    let mut xcorr_shape_to_all = Vec::new();
    let mut mi_to_all = Vec::new();
    let mut retention_time_deviation = Vec::new();
    let mut intensity_ratio = Vec::new();

    // Populate arrays from mapped_prec_peaks
    for (filename, peaks) in mapped_prec_peaks {
        for peak in peaks {
            alignment_ids.push(peak.alignment_id);
            filenames.push(filename.clone());
            reference_feature_ids.push(peak.reference_feature_id);
            aligned_feature_ids.push(peak.aligned_feature_id);
            reference_rts.push(peak.reference_rt);
            aligned_rts.push(peak.aligned_rt);
            reference_left_widths.push(peak.reference_left_width);
            reference_right_widths.push(peak.reference_right_width);
            aligned_left_widths.push(peak.aligned_left_width);
            aligned_right_widths.push(peak.aligned_right_width);
            labels.push(peak.label);
            xcorr_coelution_to_ref.push(peak.xcorr_coelution_to_ref.unwrap());
            xcorr_shape_to_ref.push(peak.xcorr_shape_to_ref.unwrap());
            mi_to_ref.push(peak.mi_to_ref.unwrap());
            xcorr_coelution_to_all.push(peak.xcorr_coelution_to_all.unwrap());
            xcorr_shape_to_all.push(peak.xcorr_shape_to_all.unwrap());
            mi_to_all.push(peak.mi_to_all.unwrap());
            retention_time_deviation.push(peak.rt_deviation.unwrap());
            intensity_ratio.push(peak.intensity_ratio.unwrap());
        }
    }

    // Create Arrow arrays
    let alignment_id_array = Int64Array::from(alignment_ids);
    let filename_array = StringArray::from(filenames);
    let reference_feature_id_array = Int64Array::from(reference_feature_ids);
    let aligned_feature_id_array = Int64Array::from(aligned_feature_ids);
    let reference_rt_array = Float64Array::from(reference_rts);
    let aligned_rt_array = Float64Array::from(aligned_rts);
    let reference_left_width_array = Float64Array::from(reference_left_widths);
    let reference_right_width_array = Float64Array::from(reference_right_widths);
    let aligned_left_width_array = Float64Array::from(aligned_left_widths);
    let aligned_right_width_array = Float64Array::from(aligned_right_widths);
    let label_array = Int32Array::from(labels);
    let cross_correlation_to_ref_array = Float64Array::from(xcorr_coelution_to_ref);
    let xcorr_shape_to_ref_array = Float64Array::from(xcorr_shape_to_ref);
    let mi_to_ref_array = Float64Array::from(mi_to_ref);
    let cross_correlation_to_all_array = Float64Array::from(xcorr_coelution_to_all);
    let xcorr_shape_to_all_array = Float64Array::from(xcorr_shape_to_all);
    let mi_to_all_array = Float64Array::from(mi_to_all);
    let retention_time_deviation_array = Float64Array::from(retention_time_deviation);
    let peak_intensity_ratio_array = Float64Array::from(intensity_ratio);

    // Create a RecordBatch
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(alignment_id_array),
            Arc::new(filename_array),
            Arc::new(reference_feature_id_array),
            Arc::new(aligned_feature_id_array),
            Arc::new(reference_rt_array),
            Arc::new(aligned_rt_array),
            Arc::new(reference_left_width_array),
            Arc::new(reference_right_width_array),
            Arc::new(aligned_left_width_array),
            Arc::new(aligned_right_width_array),
            Arc::new(label_array),
            Arc::new(cross_correlation_to_ref_array),
            Arc::new(xcorr_shape_to_ref_array),
            Arc::new(mi_to_ref_array),
            Arc::new(cross_correlation_to_all_array),
            Arc::new(xcorr_shape_to_all_array),
            Arc::new(mi_to_all_array),
            Arc::new(retention_time_deviation_array),
            Arc::new(peak_intensity_ratio_array),
        ],
    )?;

    // Create a file
    let file = File::create(output_path)?;

    // Create a Parquet writer
    let mut writer = ArrowWriter::try_new(
        file,
        Arc::new(schema),
        Some(WriterProperties::builder().build()),
    )?;

    // Write the batch
    writer.write(&batch)?;

    // Close the writer (this will flush and finalize the file)
    writer.close()?;

    Ok(())
}

/// Writes the aligned chromatograms to a Parquet file.
///
/// # Parameters
/// - `aligned_chromatograms`: A slice of AlignedChromatogram structs.
/// - `output_path`: The path to the output Parquet file.
///
/// # Returns
/// A Result containing the success status of the operation.
pub fn write_aligned_chromatograms_to_parquet(
    aligned_chromatograms: &[AlignedChromatogram],
    output_path: &str,
) -> Result<()> {
    // Define the schema
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("native_id", DataType::Utf8, false),
        Field::new("basename", DataType::Utf8, false),
        Field::new(
            "retention_times",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            false,
        ),
        Field::new(
            "intensities",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            false,
        ),
        Field::new("alignment_path", DataType::Utf8, false),
        Field::new("rt_mapping", DataType::Utf8, false),
    ]);

    // Prepare arrays for each column
    let mut ids = Vec::<i32>::new();
    let mut native_ids = Vec::<String>::new();
    let mut basenames = Vec::<String>::new();
    let mut retention_times_values = Vec::<f64>::new();
    let mut retention_times_offsets = Vec::<i32>::new();
    let mut intensities_values = Vec::<f64>::new();
    let mut intensities_offsets = Vec::<i32>::new();
    let mut alignment_paths = Vec::<String>::new();
    let mut rt_mappings = Vec::<String>::new();

    // Initialize offsets
    retention_times_offsets.push(0);
    intensities_offsets.push(0);

    // Populate arrays from aligned_chromatograms
    for ac in aligned_chromatograms {
        // println!("Processing chromatogram: {}", ac.chromatogram.native_id);

        ids.push(ac.chromatogram.id);
        native_ids.push(ac.chromatogram.native_id.clone());
        basenames.push(
            ac.chromatogram
                .metadata
                .get("basename")
                .cloned()
                .unwrap_or_default(),
        );

        // Flatten retention times and intensities into single vectors and track offsets
        // println!("Ret times: {:?}", ac.chromatogram.retention_times);
        retention_times_values.extend_from_slice(&ac.chromatogram.retention_times);
        // println!("Ret times values: {:?}", retention_times_values);
        retention_times_offsets.push(retention_times_values.len() as i32);

        intensities_values.extend_from_slice(&ac.chromatogram.intensities);
        intensities_offsets.push(intensities_values.len() as i32);

        alignment_paths.push(format!("{:?}", ac.alignment_path));
        rt_mappings.push(format!("{:?}", ac.rt_mapping));
    }

    // Create Arrow arrays
    let id_array = Int32Array::from(ids);
    let native_id_array = StringArray::from(native_ids);
    let basename_array = StringArray::from(basenames);

    // Create ListArrays using offsets and values
    let retention_times_array = ListArray::from_iter_primitive::<Float64Type, _, _>(
        retention_times_offsets.windows(2).map(|window| {
            let start = window[0] as usize;
            let end = window[1] as usize;
            Some(retention_times_values[start..end].iter().copied().map(Some))
        }),
    );

    let intensities_array = ListArray::from_iter_primitive::<Float64Type, _, _>(
        intensities_offsets.windows(2).map(|window| {
            let start = window[0] as usize;
            let end = window[1] as usize;
            Some(intensities_values[start..end].iter().copied().map(Some))
        }),
    );

    // Debugging output for array sizes
    // println!("id_array size: {}", id_array.len());
    // println!("native_id_array size: {}", native_id_array.len());
    // println!("basename_array size: {}", basename_array.len());
    // println!("retention_times_array size: {}", retention_times_array.len());
    // println!("intensities_array size: {}", intensities_array.len());

    // Create a RecordBatch
    // println!("Creating RecordBatch");

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(id_array) as ArrayRef,
            Arc::new(native_id_array) as ArrayRef,
            Arc::new(basename_array) as ArrayRef,
            Arc::new(retention_times_array) as ArrayRef,
            Arc::new(intensities_array) as ArrayRef,
            Arc::new(StringArray::from(alignment_paths)) as ArrayRef,
            Arc::new(StringArray::from(rt_mappings)) as ArrayRef,
        ],
    )?;

    // println!("RecordBatch created successfully");

    // Create a Parquet writer
    // println!("Creating Parquet writer");

    let file = File::create(output_path)?;

    // Use ArrowWriter to write the RecordBatch to a Parquet file
    let mut writer = ArrowWriter::try_new(
        file,
        Arc::<Schema>::clone(&Arc::<Schema>::from(schema)),
        None,
    )?;

    // Write the batch
    //  println!("Writing batch");
    if let Err(e) = writer.write(&batch) {
        eprintln!("Error writing batch: {:?}", e);
        return Err(e.into());
    }

    // Close the writer
    if let Err(e) = writer.close() {
        eprintln!("Error closing writer: {:?}", e);
        return Err(e.into());
    }

    println!(
        "Aligned chromatograms written to Parquet file: {}",
        output_path
    );

    Ok(())
}
