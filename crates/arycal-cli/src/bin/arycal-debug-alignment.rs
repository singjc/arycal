use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use arycal_cli::{input::Input, Runner};
use arycal_cloudpath::osw::{FeatureData, PrecursorIdData, ValueEntryType};
use arycal_cloudpath::sqmass::TransitionGroup;
use arycal_core::alignment::alignment::{inspect_peak_mapping_candidates, PeakMappingInspection};
use arycal_common::{chromatogram::Chromatogram, AlignedTics, PeakMapping, PrecursorXics};
use clap::{Arg, ArgAction, Command, ValueHint};
use serde::Serialize;

const SVG_WIDTH: usize = 1600;
const LEFT_MARGIN: f64 = 210.0;
const RIGHT_MARGIN: f64 = 40.0;
const TOP_MARGIN: f64 = 96.0;
const BOTTOM_MARGIN: f64 = 50.0;
const LANE_HEIGHT: f64 = 64.0;
const LANE_GAP: f64 = 20.0;
const MAX_POINTS_PER_TRACE: usize = 1500;
const MAX_LINK_PLOT_TRANSITIONS: usize = 6;

#[derive(Debug, Serialize)]
struct PrecursorDebugSummary {
    precursor_id: i32,
    modified_sequence: String,
    charge: i32,
    group_id: String,
    alignment_method: String,
    reference_type: String,
    reference_run: Option<String>,
    runs: Vec<RunDebugSummary>,
}

#[derive(Debug, Serialize)]
struct RunDebugSummary {
    basename: String,
    raw_transition_count: usize,
    raw_tic_points: usize,
    smoothed_tic_points: usize,
    aligned_points: usize,
    candidate_feature_count: usize,
    target_mappings: usize,
    decoy_mappings: usize,
    lag: Option<isize>,
    aligned_to: Option<String>,
}

#[derive(Debug, Clone)]
struct FeatureBoundary {
    left: f64,
    right: f64,
}

#[derive(Debug, Clone)]
struct FeaturePeak {
    rt: f64,
    left: f64,
    right: f64,
}

fn main() -> Result<()> {
    env_logger::Builder::default()
        .filter_level(log::LevelFilter::Error)
        .parse_env(env_logger::Env::default().filter_or("ARYCAL_LOG", "error,arycal=info"))
        .init();

    let matches = Command::new("arycal-debug-alignment")
        .version(clap::crate_version!())
        .about("Run ARYCAL alignment for a small precursor set and emit SVG debug plots")
        .arg(
            Arg::new("parameters")
                .required(true)
                .value_parser(clap::builder::NonEmptyStringValueParser::new())
                .help("Path to ARYCAL JSON configuration")
                .value_hint(ValueHint::FilePath),
        )
        .arg(
            Arg::new("output_dir")
                .long("output-dir")
                .required(true)
                .value_parser(clap::builder::NonEmptyStringValueParser::new())
                .help("Directory to write debug artifacts")
                .value_hint(ValueHint::DirPath),
        )
        .arg(
            Arg::new("precursor_ids")
                .long("precursor-id")
                .short('p')
                .required(true)
                .action(ArgAction::Append)
                .value_delimiter(',')
                .value_parser(clap::value_parser!(i32))
                .help("One or more precursor IDs to debug"),
        )
        .get_matches();

    let config_path = matches
        .get_one::<String>("parameters")
        .expect("required config path");
    let output_dir = PathBuf::from(
        matches
            .get_one::<String>("output_dir")
            .expect("required output dir"),
    );
    let precursor_ids: Vec<i32> = matches
        .get_many::<i32>("precursor_ids")
        .expect("required precursor ids")
        .copied()
        .collect();

    fs::create_dir_all(&output_dir)
        .with_context(|| format!("Failed to create output directory {}", output_dir.display()))?;

    let mut input = Input::from_config_path(config_path)?;
    input.alignment.retain_alignment_path = true;

    let runner = Runner::new(input.clone())?;
    let precursors = runner.get_precursors_by_ids(&precursor_ids)?;

    let raw_transition_groups = runner.read_transition_groups_batch(&precursors)?;
    let xics_batch = runner.prepare_xics_batch(&precursors)?;
    let aligned_batch = runner.align_tics_batch(xics_batch.clone())?;
    let feature_data = runner.fetch_feature_data_for_aligned_batch(&aligned_batch)?;
    let alignment_results = runner.process_peak_mappings_batch_with_feature_data(
        aligned_batch.clone(),
        &precursors,
        &feature_data,
    )?;

    for precursor in &precursors {
        let precursor_out = output_dir.join(format!(
            "precursor_{}_{}",
            precursor.precursor_id,
            sanitize_filename(&(precursor.modified_sequence.clone() + "_" + &precursor.precursor_charge.to_string()))
        ));
        fs::create_dir_all(&precursor_out)?;

        let raw_groups = raw_transition_groups
            .get(&precursor.precursor_id)
            .cloned()
            .unwrap_or_default();
        let xics = xics_batch
            .get(&precursor.precursor_id)
            .with_context(|| format!("Missing XIC batch for precursor {}", precursor.precursor_id))?;
        let aligned = aligned_batch
            .get(&precursor.precursor_id)
            .with_context(|| format!("Missing aligned batch for precursor {}", precursor.precursor_id))?;
        let features = feature_data
            .get(&precursor.precursor_id)
            .cloned()
            .unwrap_or_default();
        let result = alignment_results
            .get(&precursor.precursor_id)
            .cloned()
            .unwrap_or_default();

        let peak_mapping_inspections =
            build_peak_mapping_inspections(aligned, &features, &input);
        write_raw_xics_svg(&precursor_out.join("raw_xics.svg"), precursor, &raw_groups, xics)?;
        write_aligned_tics_svg(&precursor_out.join("aligned_tics.svg"), precursor, aligned)?;
        write_peak_mapping_svg(
            &precursor_out.join("peak_mappings.svg"),
            precursor,
            xics,
            aligned,
            &features,
            &result.detecting_peak_mappings,
        )?;
        write_peak_mapping_links_svg(
            &precursor_out.join("peak_mapping_links.svg"),
            precursor,
            &raw_groups,
            xics,
            aligned,
            &features,
            &result.detecting_peak_mappings,
            &peak_mapping_inspections,
            &input,
        )?;
        write_peak_mapping_tsv(
            &precursor_out.join("peak_mappings.tsv"),
            &result.detecting_peak_mappings,
        )?;
        write_peak_mapping_inspection_summary_tsv(
            &precursor_out.join("peak_mapping_inspection_summary.tsv"),
            &peak_mapping_inspections,
        )?;
        write_peak_mapping_candidates_tsv(
            &precursor_out.join("peak_mapping_candidates.tsv"),
            &peak_mapping_inspections,
        )?;

        let summary = build_summary(
            precursor,
            &input,
            &raw_groups,
            xics,
            aligned,
            &features,
            &result.detecting_peak_mappings,
        );
        fs::write(
            precursor_out.join("summary.json"),
            serde_json::to_string_pretty(&summary)?,
        )?;
    }

    Ok(())
}

fn build_summary(
    precursor: &PrecursorIdData,
    input: &Input,
    raw_groups: &[TransitionGroup],
    xics: &PrecursorXics,
    aligned: &AlignedTics,
    features: &[FeatureData],
    mappings: &HashMap<String, Vec<PeakMapping>>,
) -> PrecursorDebugSummary {
    let raw_by_run: HashMap<String, &TransitionGroup> = raw_groups
        .iter()
        .filter_map(|group| {
            group.metadata.get("basename").map(|basename| (basename.clone(), group))
        })
        .collect();
    let smoothed_by_run = smoothed_tic_map(xics);
    let aligned_by_run: HashMap<String, _> = aligned
        .aligned_chromatograms
        .iter()
        .map(|chrom| {
            (
                chrom.chromatogram.metadata.get("basename").unwrap().clone(),
                chrom,
            )
        })
        .collect();

    let mut run_names: Vec<String> = raw_by_run.keys().cloned().collect();
    run_names.sort();

    let runs = run_names
        .into_iter()
        .map(|run| {
            let raw_group = raw_by_run.get(&run);
            let smoothed = smoothed_by_run.get(&run);
            let aligned_chrom = aligned_by_run.get(&run);
            let candidate_features = extract_feature_boundaries(features, &run);
            let run_mappings = mappings.get(&run).cloned().unwrap_or_default();
            let target_mappings = run_mappings.iter().filter(|m| m.label == 1).count();
            let decoy_mappings = run_mappings.iter().filter(|m| m.label != 1).count();

            RunDebugSummary {
                basename: run,
                raw_transition_count: raw_group.map(|g| g.chromatograms.len()).unwrap_or(0),
                raw_tic_points: raw_group
                    .map(|g| g.calculate_tic().retention_times.len())
                    .unwrap_or(0),
                smoothed_tic_points: smoothed.map(|tic| tic.retention_times.len()).unwrap_or(0),
                aligned_points: aligned_chrom
                    .map(|chrom| chrom.chromatogram.retention_times.len())
                    .unwrap_or(0),
                candidate_feature_count: candidate_features.len(),
                target_mappings,
                decoy_mappings,
                lag: aligned_chrom.and_then(|chrom| chrom.lag),
                aligned_to: aligned_chrom.map(|chrom| chrom.reference_basename.clone()),
            }
        })
        .collect();

    PrecursorDebugSummary {
        precursor_id: precursor.precursor_id,
        modified_sequence: precursor.modified_sequence.clone(),
        charge: precursor.precursor_charge,
        group_id: xics.group_id.clone(),
        alignment_method: input.alignment.method.clone(),
        reference_type: input.alignment.reference_type.clone(),
        reference_run: aligned
            .aligned_chromatograms
            .first()
            .map(|chrom| chrom.reference_basename.clone()),
        runs,
    }
}

fn build_peak_mapping_inspections(
    aligned: &AlignedTics,
    features: &[FeatureData],
    input: &Input,
) -> HashMap<String, Vec<PeakMappingInspection>> {
    aligned
        .aligned_chromatograms
        .iter()
        .filter_map(|chrom| {
            let current_run = chrom.chromatogram.metadata.get("basename")?;
            if chrom.reference_basename == *current_run {
                return None;
            }

            let current_run_feat_data: Vec<_> = features
                .iter()
                .filter(|feature| &feature.basename == current_run)
                .cloned()
                .collect();
            let reference_run_feat_data: Vec<_> = features
                .iter()
                .filter(|feature| feature.basename == chrom.reference_basename)
                .cloned()
                .collect();

            if current_run_feat_data.is_empty() || reference_run_feat_data.is_empty() {
                return None;
            }

            let inspection_rows = inspect_peak_mapping_candidates(
                chrom,
                &reference_run_feat_data,
                &current_run_feat_data,
                input.alignment.rt_mapping_tolerance.unwrap_or_default(),
                &input.alignment,
            );

            Some((current_run.to_string(), inspection_rows))
        })
        .collect()
}

fn write_peak_mapping_tsv(path: &Path, mappings: &HashMap<String, Vec<PeakMapping>>) -> Result<()> {
    let mut rows = Vec::new();
    rows.push("run\tlabel\talignment_id\treference_feature_id\taligned_feature_id\treference_rt\taligned_rt\treference_left_width\treference_right_width\taligned_left_width\taligned_right_width\trt_deviation\tintensity_ratio".to_string());

    let mut run_names: Vec<_> = mappings.keys().cloned().collect();
    run_names.sort();

    for run in run_names {
        let mut run_rows = mappings.get(&run).cloned().unwrap_or_default();
        run_rows.sort_by(|a, b| a.alignment_id.cmp(&b.alignment_id));
        for mapping in run_rows {
            rows.push(format!(
                "{}\t{}\t{}\t{}\t{}\t{:.6}\t{:.6}\t{:.6}\t{:.6}\t{:.6}\t{:.6}\t{}\t{}",
                run,
                mapping.label,
                mapping.alignment_id,
                mapping.reference_feature_id,
                mapping.aligned_feature_id,
                mapping.reference_rt,
                mapping.aligned_rt,
                mapping.reference_left_width,
                mapping.reference_right_width,
                mapping.aligned_left_width,
                mapping.aligned_right_width,
                mapping
                    .rt_deviation
                    .map(|v| format!("{:.6}", v))
                    .unwrap_or_default(),
                mapping
                    .intensity_ratio
                    .map(|v| format!("{:.6}", v))
                    .unwrap_or_default(),
            ));
        }
    }

    fs::write(path, rows.join("\n"))?;
    Ok(())
}

fn write_peak_mapping_inspection_summary_tsv(
    path: &Path,
    inspections: &HashMap<String, Vec<PeakMappingInspection>>,
) -> Result<()> {
    let mut rows = Vec::new();
    rows.push("run\treference_feature_id\talignment_id\treference_rt\treference_left_width\treference_right_width\tmapped_target_rt\troundtrip_reference_rt\troundtrip_error\tlag\tcandidate_total_count\tcandidate_within_tolerance_count\tselected_feature_id\tselected_feature_rt\tselected_abs_rt_diff_to_target\tselected_abs_rt_diff_to_reference".to_string());

    let mut run_names: Vec<_> = inspections.keys().cloned().collect();
    run_names.sort();

    for run in run_names {
        let mut run_rows = inspections.get(&run).cloned().unwrap_or_default();
        run_rows.sort_by(|a, b| a.alignment_id.cmp(&b.alignment_id));

        for row in run_rows {
            rows.push(format!(
                "{}\t{}\t{}\t{:.6}\t{:.6}\t{:.6}\t{:.6}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                run,
                row.reference_feature_id,
                row.alignment_id,
                row.reference_rt,
                row.reference_left_width,
                row.reference_right_width,
                row.mapped_target_rt,
                format_optional_f64(row.roundtrip_reference_rt),
                format_optional_f64(row.roundtrip_error),
                format_optional_isize(row.lag),
                row.candidate_total_count,
                row.candidate_within_tolerance_count,
                format_optional_i64(row.selected_feature_id),
                format_optional_f64(row.selected_feature_rt),
                format_optional_f64(row.selected_abs_rt_diff_to_target),
                format_optional_f64(row.selected_abs_rt_diff_to_reference),
            ));
        }
    }

    fs::write(path, rows.join("\n"))?;
    Ok(())
}

fn write_peak_mapping_candidates_tsv(
    path: &Path,
    inspections: &HashMap<String, Vec<PeakMappingInspection>>,
) -> Result<()> {
    let mut rows = Vec::new();
    rows.push("run\treference_feature_id\talignment_id\treference_rt\tmapped_target_rt\troundtrip_reference_rt\troundtrip_error\tlag\tcandidate_total_count\tcandidate_within_tolerance_count\tcandidate_rank\tcandidate_feature_id\tcandidate_rt\tcandidate_left_width\tcandidate_right_width\tcandidate_abs_rt_diff_to_target\tcandidate_abs_rt_diff_to_reference\tcandidate_peakgroup_rank\tcandidate_qvalue\tcandidate_intensity\tcandidate_normalized_summed_intensity\tcandidate_within_tolerance\tcandidate_selected".to_string());

    let mut run_names: Vec<_> = inspections.keys().cloned().collect();
    run_names.sort();

    for run in run_names {
        let mut run_rows = inspections.get(&run).cloned().unwrap_or_default();
        run_rows.sort_by(|a, b| a.alignment_id.cmp(&b.alignment_id));

        for inspection in run_rows {
            if inspection.candidates.is_empty() {
                rows.push(format!(
                    "{}\t{}\t{}\t{:.6}\t{:.6}\t{}\t{}\t{}\t{}\t{}\t\t\t\t\t\t\t\t\t\t\t\t\t",
                    run,
                    inspection.reference_feature_id,
                    inspection.alignment_id,
                    inspection.reference_rt,
                    inspection.mapped_target_rt,
                    format_optional_f64(inspection.roundtrip_reference_rt),
                    format_optional_f64(inspection.roundtrip_error),
                    format_optional_isize(inspection.lag),
                    inspection.candidate_total_count,
                    inspection.candidate_within_tolerance_count,
                ));
                continue;
            }

            for candidate in inspection.candidates {
                rows.push(format!(
                    "{}\t{}\t{}\t{:.6}\t{:.6}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.6}\t{:.6}\t{:.6}\t{:.6}\t{:.6}\t{}\t{}\t{}\t{}\t{}\t{}",
                    run,
                    inspection.reference_feature_id,
                    inspection.alignment_id,
                    inspection.reference_rt,
                    inspection.mapped_target_rt,
                    format_optional_f64(inspection.roundtrip_reference_rt),
                    format_optional_f64(inspection.roundtrip_error),
                    format_optional_isize(inspection.lag),
                    inspection.candidate_total_count,
                    inspection.candidate_within_tolerance_count,
                    candidate.candidate_rank,
                    candidate.aligned_feature_id,
                    candidate.aligned_rt,
                    candidate.aligned_left_width,
                    candidate.aligned_right_width,
                    candidate.abs_rt_diff_to_target,
                    candidate.abs_rt_diff_to_reference,
                    format_optional_i32(candidate.peakgroup_rank),
                    format_optional_f64(candidate.qvalue),
                    format_optional_f64(candidate.intensity),
                    format_optional_f64(candidate.normalized_summed_intensity),
                    candidate.within_tolerance,
                    candidate.selected_by_current_logic,
                ));
            }
        }
    }

    fs::write(path, rows.join("\n"))?;
    Ok(())
}

fn write_raw_xics_svg(
    path: &Path,
    precursor: &PrecursorIdData,
    raw_groups: &[TransitionGroup],
    xics: &PrecursorXics,
) -> Result<()> {
    let mut groups = raw_groups.to_vec();
    groups.sort_by_key(|group| group.metadata.get("basename").cloned().unwrap_or_default());

    let smoothed = smoothed_tic_map(xics);

    let mut x_min = f64::INFINITY;
    let mut x_max = f64::NEG_INFINITY;
    for group in &groups {
        let raw_tic = group.calculate_tic();
        if let Some((min_rt, max_rt)) = range_from_slice(&raw_tic.retention_times) {
            x_min = x_min.min(min_rt);
            x_max = x_max.max(max_rt);
        }
    }

    if !x_min.is_finite() || !x_max.is_finite() {
        return Ok(());
    }

    let height = svg_height(groups.len());
    let mut body = String::new();
    body.push_str(&svg_title(&format!(
        "Raw extracted XICs for precursor {} ({})",
        precursor.precursor_id, xics.group_id
    )));

    for (idx, group) in groups.iter().enumerate() {
        let lane = lane_metrics(idx);
        let basename = group.metadata.get("basename").cloned().unwrap_or_default();
        let raw_tic = group.calculate_tic();
        let lane_max = raw_tic
            .intensities
            .iter()
            .copied()
            .fold(0.0_f64, f64::max)
            .max(1e-12);

        body.push_str(&lane_background(&lane));
        body.push_str(&lane_label(
            &lane,
            &format!("{} ({})", basename, group.chromatograms.len()),
        ));

        let mut traces: Vec<_> = group.chromatograms.values().collect();
        traces.sort_by_key(|chrom| chrom.native_id.clone());
        for chrom in traces {
            body.push_str(&series_polyline(
                &chrom.retention_times,
                &scale_values(&chrom.intensities, lane_max),
                x_min,
                x_max,
                &lane,
                "#b0b7c3",
                0.8,
                0.45,
            ));
        }

        body.push_str(&series_polyline(
            &raw_tic.retention_times,
            &scale_values(&raw_tic.intensities, lane_max),
            x_min,
            x_max,
            &lane,
            "#1f77b4",
            1.6,
            0.95,
        ));

        if let Some(smoothed_tic) = smoothed.get(&basename) {
            body.push_str(&series_polyline(
                &smoothed_tic.retention_times,
                &smoothed_tic.intensities,
                x_min,
                x_max,
                &lane,
                "#ff7f0e",
                1.4,
                0.9,
            ));
        }
    }

    body.push_str(&x_axis(x_min, x_max, groups.len()));
    write_svg(path, SVG_WIDTH, height, &body)
}

fn write_aligned_tics_svg(
    path: &Path,
    precursor: &PrecursorIdData,
    aligned: &AlignedTics,
) -> Result<()> {
    let mut chromatograms = aligned.aligned_chromatograms.clone();
    chromatograms.sort_by_key(|chrom| chrom.chromatogram.metadata.get("basename").cloned().unwrap_or_default());

    let mut x_min = f64::INFINITY;
    let mut x_max = f64::NEG_INFINITY;
    for chrom in &chromatograms {
        if let Some((min_rt, max_rt)) = range_from_slice(&chrom.chromatogram.retention_times) {
            x_min = x_min.min(min_rt);
            x_max = x_max.max(max_rt);
        }
    }

    if !x_min.is_finite() || !x_max.is_finite() {
        return Ok(());
    }

    let height = svg_height(chromatograms.len());
    let mut body = String::new();
    body.push_str(&svg_title(&format!(
        "Aligned TICs for precursor {} ({})",
        precursor.precursor_id, aligned.group_id
    )));

    for (idx, chrom) in chromatograms.iter().enumerate() {
        let lane = lane_metrics(idx);
        let basename = chrom.chromatogram.metadata.get("basename").cloned().unwrap_or_default();
        body.push_str(&lane_background(&lane));
        body.push_str(&lane_label(
            &lane,
            &format!(
                "{} -> {}{}",
                basename,
                chrom.reference_basename,
                chrom.lag.map(|lag| format!(" (lag={})", lag)).unwrap_or_default()
            ),
        ));
        body.push_str(&series_polyline(
            &chrom.chromatogram.retention_times,
            &normalize_series(&chrom.chromatogram.intensities),
            x_min,
            x_max,
            &lane,
            color_for_index(idx),
            1.6,
            0.95,
        ));
    }

    body.push_str(&x_axis(x_min, x_max, chromatograms.len()));
    write_svg(path, SVG_WIDTH, height, &body)
}

fn write_peak_mapping_svg(
    path: &Path,
    precursor: &PrecursorIdData,
    xics: &PrecursorXics,
    aligned: &AlignedTics,
    features: &[FeatureData],
    mappings: &HashMap<String, Vec<PeakMapping>>,
) -> Result<()> {
    let smoothed = smoothed_tic_map(xics);
    let reference_run = aligned
        .aligned_chromatograms
        .first()
        .map(|chrom| chrom.reference_basename.clone())
        .or_else(|| smoothed.keys().next().cloned());

    let Some(reference_run) = reference_run else {
        return Ok(());
    };

    let mut run_names: Vec<String> = smoothed.keys().cloned().collect();
    run_names.sort();

    let mut ordered_runs = Vec::with_capacity(run_names.len());
    ordered_runs.push(reference_run.clone());
    ordered_runs.extend(run_names.into_iter().filter(|run| run != &reference_run));

    let (x_min, x_max) = match range_from_slice(&xics.common_rt_space) {
        Some(range) => range,
        None => return Ok(()),
    };

    let height = svg_height(ordered_runs.len());
    let reference_tic = smoothed
        .get(&reference_run)
        .with_context(|| format!("Missing reference TIC for run {}", reference_run))?;
    let reference_boundaries = extract_feature_boundaries(features, &reference_run);

    let mut body = String::new();
    body.push_str(&svg_title(&format!(
        "Mapped peak boundaries for precursor {} ({})",
        precursor.precursor_id, xics.group_id
    )));

    for (idx, run) in ordered_runs.iter().enumerate() {
        let lane = lane_metrics(idx);
        body.push_str(&lane_background(&lane));

        if run == &reference_run {
            body.push_str(&lane_label(&lane, &format!("reference: {}", run)));
            body.push_str(&series_polyline(
                &reference_tic.retention_times,
                &reference_tic.intensities,
                x_min,
                x_max,
                &lane,
                "#333333",
                1.8,
                0.95,
            ));
            for boundary in &reference_boundaries {
                body.push_str(&boundary_rect(
                    boundary.left,
                    boundary.right,
                    x_min,
                    x_max,
                    &lane,
                    "#ffcc80",
                    "#d97706",
                    0.35,
                    0.62,
                    0.95,
                ));
            }
            continue;
        }

        body.push_str(&lane_label(&lane, run));
        body.push_str(&series_polyline(
            &reference_tic.retention_times,
            &reference_tic.intensities,
            x_min,
            x_max,
            &lane,
            "#b0b7c3",
            1.1,
            0.7,
        ));

        if let Some(query_tic) = smoothed.get(run) {
            body.push_str(&series_polyline(
                &query_tic.retention_times,
                &query_tic.intensities,
                x_min,
                x_max,
                &lane,
                "#1f77b4",
                1.6,
                0.95,
            ));
        }

        let candidate_boundaries = extract_feature_boundaries(features, run);
        for boundary in &candidate_boundaries {
            body.push_str(&boundary_rect(
                boundary.left,
                boundary.right,
                x_min,
                x_max,
                &lane,
                "#bfdbfe",
                "#60a5fa",
                0.18,
                0.08,
                0.34,
            ));
        }

        let target_mappings: Vec<_> = mappings
            .get(run)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|mapping| mapping.label == 1)
            .collect();

        for mapping in &target_mappings {
            body.push_str(&boundary_rect(
                mapping.reference_left_width,
                mapping.reference_right_width,
                x_min,
                x_max,
                &lane,
                "#fed7aa",
                "#ea580c",
                0.35,
                0.66,
                0.92,
            ));
            body.push_str(&boundary_rect(
                mapping.aligned_left_width,
                mapping.aligned_right_width,
                x_min,
                x_max,
                &lane,
                "#93c5fd",
                "#2563eb",
                0.45,
                0.36,
                0.62,
            ));
        }

        body.push_str(&lane_note(
            &lane,
            &format!("target mappings: {}", target_mappings.len()),
        ));
    }

    body.push_str(&x_axis(x_min, x_max, ordered_runs.len()));
    write_svg(path, SVG_WIDTH, height, &body)
}

fn write_peak_mapping_links_svg(
    path: &Path,
    precursor: &PrecursorIdData,
    raw_groups: &[TransitionGroup],
    xics: &PrecursorXics,
    aligned: &AlignedTics,
    features: &[FeatureData],
    mappings: &HashMap<String, Vec<PeakMapping>>,
    inspections: &HashMap<String, Vec<PeakMappingInspection>>,
    input: &Input,
) -> Result<()> {
    let reference_run = aligned
        .aligned_chromatograms
        .first()
        .map(|chrom| chrom.reference_basename.clone())
        .or_else(|| {
            raw_groups
                .iter()
                .find_map(|group| group.metadata.get("basename").cloned())
        });

    let Some(reference_run) = reference_run else {
        return Ok(());
    };

    let group_by_run: HashMap<String, &TransitionGroup> = raw_groups
        .iter()
        .filter_map(|group| {
            group.metadata
                .get("basename")
                .map(|basename| (basename.clone(), group))
        })
        .collect();
    if group_by_run.is_empty() {
        return Ok(());
    }

    let mut run_names: Vec<String> = group_by_run.keys().cloned().collect();
    run_names.sort();
    let mut ordered_runs = Vec::with_capacity(run_names.len());
    if group_by_run.contains_key(&reference_run) {
        ordered_runs.push(reference_run.clone());
        ordered_runs.extend(run_names.into_iter().filter(|run| run != &reference_run));
    } else {
        ordered_runs = run_names;
    }

    let smoothed_traces_by_run = smoothed_transition_trace_map(
        raw_groups,
        xics,
        &reference_run,
        input.alignment.smoothing.sgolay_window,
        input.alignment.smoothing.sgolay_order,
    );

    let mut x_min = f64::INFINITY;
    let mut x_max = f64::NEG_INFINITY;
    for traces in smoothed_traces_by_run.values() {
        for trace in traces {
            if let Some((min_rt, max_rt)) = range_from_slice(&trace.retention_times) {
                x_min = x_min.min(min_rt);
                x_max = x_max.max(max_rt);
            }
        }
    }
    if !x_min.is_finite() || !x_max.is_finite() {
        return Ok(());
    }

    let reference_peaks = extract_feature_peaks(features, &reference_run);
    let target_mappings_by_run: HashMap<String, HashMap<i64, PeakMapping>> = mappings
        .iter()
        .map(|(run, rows)| {
            let per_alignment = rows
                .iter()
                .filter(|mapping| mapping.label == 1)
                .cloned()
                .map(|mapping| (mapping.alignment_id, mapping))
                .collect();
            (run.clone(), per_alignment)
        })
        .collect();
    let inspections_by_run: HashMap<String, HashMap<i64, PeakMappingInspection>> = inspections
        .iter()
        .map(|(run, rows)| {
            let per_alignment = rows
                .iter()
                .cloned()
                .map(|inspection| (inspection.alignment_id, inspection))
                .collect();
            (run.clone(), per_alignment)
        })
        .collect();

    let height = svg_height(ordered_runs.len());
    let mut body = String::new();
    body.push_str(&svg_title(&format!(
        "Smoothed XICs with linked peak mappings for precursor {} ({})",
        precursor.precursor_id,
        aligned.group_id
    )));
    body.push_str(&plot_legend());

    for (idx, run) in ordered_runs.iter().enumerate() {
        let lane = lane_metrics(idx);
        body.push_str(&lane_background(&lane));
        if run == &reference_run {
            body.push_str(&reference_star(LEFT_MARGIN - 18.0, lane.top + 12.0, 8.0));
            body.push_str(&lane_label(&lane, &format!("{} [reference]", run)));
        } else {
            body.push_str(&lane_label(&lane, run));
        }

        if let Some(traces) = smoothed_traces_by_run.get(run) {
            for (trace_idx, trace) in traces.iter().enumerate() {
                body.push_str(&series_polyline(
                    &trace.retention_times,
                    &min_max_normalize_values(&trace.intensities),
                    x_min,
                    x_max,
                    &lane,
                    color_for_index(trace_idx),
                    1.2,
                    0.8,
                ));
            }
        }

        if run == &reference_run {
            for (alignment_idx, peak) in reference_peaks.iter().enumerate() {
                let color = color_for_index(alignment_idx);
                body.push_str(&boundary_pair_lines(
                    peak.left,
                    peak.right,
                    x_min,
                    x_max,
                    &lane,
                    color,
                    2.0,
                    None,
                ));
                body.push_str(&marker_circle(
                    peak.rt,
                    x_min,
                    x_max,
                    &lane,
                    "#111827",
                    3.0,
                    0.55,
                ));
            }
            body.push_str(&lane_note(
                &lane,
                &format!("reference peaks: {}", reference_peaks.len()),
            ));
        } else {
            let mapped_count = target_mappings_by_run.get(run).map(|rows| rows.len()).unwrap_or(0);
            if let Some(run_mappings) = target_mappings_by_run.get(run) {
                let mut alignment_ids: Vec<_> = run_mappings.keys().copied().collect();
                alignment_ids.sort_unstable();
                for alignment_id in alignment_ids {
                    if let Some(mapping) = run_mappings.get(&alignment_id) {
                        let color = color_for_index(alignment_id as usize);
                        body.push_str(&boundary_pair_lines(
                            mapping.aligned_left_width,
                            mapping.aligned_right_width,
                            x_min,
                            x_max,
                            &lane,
                            color,
                            2.0,
                            None,
                        ));
                    }
                }
            }

            if let Some(run_inspections) = inspections_by_run.get(run) {
                let mut unmapped_ids: Vec<_> = run_inspections
                    .values()
                    .filter(|inspection| inspection.selected_feature_id.is_none())
                    .map(|inspection| inspection.alignment_id)
                    .collect();
                unmapped_ids.sort_unstable();

                for alignment_id in unmapped_ids {
                    if let Some(inspection) = run_inspections.get(&alignment_id) {
                        body.push_str(&diamond_marker(
                            inspection.mapped_target_rt,
                            x_min,
                            x_max,
                            &lane,
                            "#dc2626",
                            7.0,
                            0.52,
                        ));
                    }
                }
            }

            body.push_str(&lane_note(&lane, &format!("mapped peaks: {}", mapped_count)));
        }
    }

    for (alignment_idx, peak) in reference_peaks.iter().enumerate() {
        let color = color_for_index(alignment_idx);
        let mut previous_segment = Some((
            0usize,
            peak.left,
            peak.right,
        ));

        for (run_idx, run) in ordered_runs.iter().enumerate().skip(1) {
            if let Some(mapping) = target_mappings_by_run
                .get(run)
                .and_then(|rows| rows.get(&(alignment_idx as i64)))
            {
                if let Some((prev_run_idx, prev_left, prev_right)) = previous_segment {
                    let prev_lane = lane_metrics(prev_run_idx);
                    let current_lane = lane_metrics(run_idx);
                    body.push_str(&connector_segment(
                        prev_left,
                        mapping.aligned_left_width,
                        x_min,
                        x_max,
                        &prev_lane,
                        &current_lane,
                        color,
                    ));
                    body.push_str(&connector_segment(
                        prev_right,
                        mapping.aligned_right_width,
                        x_min,
                        x_max,
                        &prev_lane,
                        &current_lane,
                        color,
                    ));
                }
                previous_segment = Some((run_idx, mapping.aligned_left_width, mapping.aligned_right_width));
            } else {
                previous_segment = None;
            }
        }
    }

    body.push_str(&x_axis(x_min, x_max, ordered_runs.len()));
    write_svg(path, SVG_WIDTH, height, &body)
}

fn smoothed_tic_map(xics: &PrecursorXics) -> HashMap<String, arycal_common::chromatogram::Chromatogram> {
    xics.smoothed_tics
        .iter()
        .filter_map(|chrom| chrom.metadata.get("basename").map(|basename| (basename.clone(), chrom.clone())))
        .collect()
}

fn smoothed_transition_trace_map(
    raw_groups: &[TransitionGroup],
    xics: &PrecursorXics,
    reference_run: &str,
    sgolay_window: usize,
    sgolay_order: usize,
) -> HashMap<String, Vec<Chromatogram>> {
    let selected_native_ids = select_link_plot_native_ids(raw_groups, xics, reference_run);

    raw_groups
        .iter()
        .filter_map(|group| {
            let basename = group.metadata.get("basename")?.clone();
            let mut traces: Vec<_> = group
                .chromatograms
                .iter()
                .filter(|(native_id, _)| selected_native_ids.contains(*native_id))
                .map(|(_, chrom)| chrom.clone())
                .collect();
            traces.sort_by_key(|chrom| chrom.native_id.clone());

            let smoothed_traces = traces
                .into_iter()
                .map(|trace| {
                    trace
                        .smooth_sgolay(sgolay_window, sgolay_order)
                        .unwrap_or(trace)
                })
                .collect();
            Some((basename, smoothed_traces))
        })
        .collect()
}

fn select_link_plot_native_ids(
    raw_groups: &[TransitionGroup],
    xics: &PrecursorXics,
    reference_run: &str,
) -> Vec<String> {
    let raw_fragment_ids: Vec<String> = xics
        .native_ids
        .iter()
        .filter(|native_id| !native_id.contains("_Precursor_"))
        .cloned()
        .collect();

    let Some(reference_group) = raw_groups
        .iter()
        .find(|group| group.metadata.get("basename").map(|run| run == reference_run).unwrap_or(false))
    else {
        return raw_fragment_ids
            .into_iter()
            .take(MAX_LINK_PLOT_TRANSITIONS)
            .collect();
    };

    let group_keys: HashSet<String> = reference_group.chromatograms.keys().cloned().collect();
    let fragment_ids: Vec<String> = raw_fragment_ids
        .into_iter()
        .filter_map(|native_id| {
            if group_keys.contains(&native_id) {
                Some(native_id)
            } else {
                let openms_native_id = format!("transition:{native_id}");
                if group_keys.contains(&openms_native_id) {
                    Some(openms_native_id)
                } else {
                    None
                }
            }
        })
        .collect();

    let mut ranked_ids: Vec<(String, f64)> = fragment_ids
        .into_iter()
        .filter_map(|native_id| {
            let chromatogram = reference_group.chromatograms.get(&native_id)?;
            let max_intensity = chromatogram
                .intensities
                .iter()
                .copied()
                .fold(0.0_f64, f64::max);
            Some((native_id, max_intensity))
        })
        .collect();

    ranked_ids.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.0.cmp(&b.0))
    });
    ranked_ids.truncate(MAX_LINK_PLOT_TRANSITIONS);
    ranked_ids.into_iter().map(|(native_id, _)| native_id).collect()
}

fn extract_feature_boundaries(features: &[FeatureData], basename: &str) -> Vec<FeatureBoundary> {
    features
        .iter()
        .filter(|feature| feature.basename == basename)
        .flat_map(|feature| {
            let feature_ids = feature
                .feature_id
                .as_ref()
                .map(expand_value_i64)
                .unwrap_or_default();
            let rts = expand_value_f64(&feature.exp_rt);
            let lefts = feature
                .left_width
                .as_ref()
                .map(expand_value_f64)
                .unwrap_or_default();
            let rights = feature
                .right_width
                .as_ref()
                .map(expand_value_f64)
                .unwrap_or_default();

            let len = rts
                .len()
                .min(feature_ids.len())
                .min(lefts.len())
                .min(rights.len());

            (0..len).map(move |idx| FeatureBoundary {
                left: lefts[idx],
                right: rights[idx],
            })
        })
        .collect()
}

fn extract_feature_peaks(features: &[FeatureData], basename: &str) -> Vec<FeaturePeak> {
    features
        .iter()
        .filter(|feature| feature.basename == basename)
        .flat_map(|feature| {
            let feature_ids = feature
                .feature_id
                .as_ref()
                .map(expand_value_i64)
                .unwrap_or_default();
            let rts = expand_value_f64(&feature.exp_rt);
            let lefts = feature
                .left_width
                .as_ref()
                .map(expand_value_f64)
                .unwrap_or_default();
            let rights = feature
                .right_width
                .as_ref()
                .map(expand_value_f64)
                .unwrap_or_default();

            let len = rts
                .len()
                .min(feature_ids.len())
                .min(lefts.len())
                .min(rights.len());

            (0..len).map(move |idx| FeaturePeak {
                rt: rts[idx],
                left: lefts[idx],
                right: rights[idx],
            })
        })
        .collect()
}

fn expand_value_f64(value: &ValueEntryType<f64>) -> Vec<f64> {
    match value {
        ValueEntryType::Single(v) => vec![*v],
        ValueEntryType::Multiple(values) => values.clone(),
    }
}

fn expand_value_i64(value: &ValueEntryType<i64>) -> Vec<i64> {
    match value {
        ValueEntryType::Single(v) => vec![*v],
        ValueEntryType::Multiple(values) => values.clone(),
    }
}

#[derive(Clone, Copy)]
struct LaneMetrics {
    top: f64,
    height: f64,
}

fn lane_metrics(idx: usize) -> LaneMetrics {
    LaneMetrics {
        top: TOP_MARGIN + idx as f64 * (LANE_HEIGHT + LANE_GAP),
        height: LANE_HEIGHT,
    }
}

fn svg_height(num_lanes: usize) -> usize {
    (TOP_MARGIN + BOTTOM_MARGIN + num_lanes as f64 * (LANE_HEIGHT + LANE_GAP)) as usize
}

fn write_svg(path: &Path, width: usize, height: usize, body: &str) -> Result<()> {
    let svg = format!(
        r#"<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<style>
  text {{ font-family: monospace; fill: #111827; }}
</style>
<rect x="0" y="0" width="{width}" height="{height}" fill="white"/>
{body}
</svg>"#
    );
    fs::write(path, svg)?;
    Ok(())
}

fn svg_title(title: &str) -> String {
    format!(
        r#"<text x="{:.1}" y="24" font-size="22" font-weight="bold">{}</text>"#,
        LEFT_MARGIN,
        xml_escape(title)
    )
}

fn lane_background(lane: &LaneMetrics) -> String {
    format!(
        r##"<rect x="{:.1}" y="{:.1}" width="{:.1}" height="{:.1}" fill="#fafafa" stroke="#e5e7eb" stroke-width="1"/>"##,
        LEFT_MARGIN,
        lane.top,
        SVG_WIDTH as f64 - LEFT_MARGIN - RIGHT_MARGIN,
        lane.height
    )
}

fn lane_label(lane: &LaneMetrics, label: &str) -> String {
    format!(
        r#"<text x="{:.1}" y="{:.1}" font-size="12">{}</text>"#,
        10.0,
        lane.top + lane.height * 0.58,
        xml_escape(label)
    )
}

fn lane_note(lane: &LaneMetrics, label: &str) -> String {
    format!(
        r##"<text x="{:.1}" y="{:.1}" font-size="11" fill="#4b5563">{}</text>"##,
        SVG_WIDTH as f64 - RIGHT_MARGIN - 160.0,
        lane.top + 14.0,
        xml_escape(label)
    )
}

fn x_axis(x_min: f64, x_max: f64, num_lanes: usize) -> String {
    let y = TOP_MARGIN + num_lanes as f64 * (LANE_HEIGHT + LANE_GAP) - LANE_GAP / 2.0;
    let mut out = format!(
        r##"<line x1="{:.1}" y1="{:.1}" x2="{:.1}" y2="{:.1}" stroke="#111827" stroke-width="1"/>"##,
        LEFT_MARGIN,
        y,
        SVG_WIDTH as f64 - RIGHT_MARGIN,
        y
    );

    for idx in 0..=6 {
        let fraction = idx as f64 / 6.0;
        let x_value = x_min + fraction * (x_max - x_min);
        let x = data_to_px(x_value, x_min, x_max);
        out.push_str(&format!(
            r##"<line x1="{:.1}" y1="{:.1}" x2="{:.1}" y2="{:.1}" stroke="#111827" stroke-width="1"/>"##,
            x,
            y,
            x,
            y + 5.0
        ));
        out.push_str(&format!(
            r#"<text x="{:.1}" y="{:.1}" font-size="11" text-anchor="middle">{:.2}</text>"#,
            x,
            y + 20.0,
            x_value
        ));
    }

    out.push_str(&format!(
        r#"<text x="{:.1}" y="{:.1}" font-size="12" text-anchor="middle">Retention time</text>"#,
        (LEFT_MARGIN + (SVG_WIDTH as f64 - RIGHT_MARGIN)) / 2.0,
        y + 38.0
    ));
    out
}

fn boundary_rect(
    left: f64,
    right: f64,
    x_min: f64,
    x_max: f64,
    lane: &LaneMetrics,
    fill: &str,
    stroke: &str,
    opacity: f64,
    y_start_fraction: f64,
    y_end_fraction: f64,
) -> String {
    let x1 = data_to_px(left, x_min, x_max);
    let x2 = data_to_px(right, x_min, x_max);
    let y1 = lane.top + lane.height * (1.0 - y_end_fraction);
    let y2 = lane.top + lane.height * (1.0 - y_start_fraction);
    format!(
        r#"<rect x="{:.1}" y="{:.1}" width="{:.1}" height="{:.1}" fill="{}" fill-opacity="{:.3}" stroke="{}" stroke-width="1"/>"#,
        x1.min(x2),
        y1,
        (x2 - x1).abs().max(1.0),
        (y2 - y1).abs().max(1.0),
        fill,
        opacity,
        stroke
    )
}

fn boundary_pair_lines(
    left: f64,
    right: f64,
    x_min: f64,
    x_max: f64,
    lane: &LaneMetrics,
    color: &str,
    stroke_width: f64,
    dasharray: Option<&str>,
) -> String {
    let mut out = String::new();
    out.push_str(&vertical_marker(
        left,
        x_min,
        x_max,
        lane,
        color,
        stroke_width,
        dasharray,
    ));
    out.push_str(&vertical_marker(
        right,
        x_min,
        x_max,
        lane,
        color,
        stroke_width,
        dasharray,
    ));
    out
}

fn vertical_marker(
    rt: f64,
    x_min: f64,
    x_max: f64,
    lane: &LaneMetrics,
    color: &str,
    stroke_width: f64,
    dasharray: Option<&str>,
) -> String {
    let x = data_to_px(rt, x_min, x_max);
    let y1 = lane.top + lane.height * 0.08;
    let y2 = lane.top + lane.height * 0.92;
    let dash_attr = dasharray
        .map(|dash| format!(r#" stroke-dasharray="{}""#, dash))
        .unwrap_or_default();
    format!(
        r#"<line x1="{:.1}" y1="{:.1}" x2="{:.1}" y2="{:.1}" stroke="{}" stroke-width="{:.2}" stroke-opacity="0.95"{} />"#,
        x,
        y1,
        x,
        y2,
        color,
        stroke_width,
        dash_attr
    )
}

fn connector_segment(
    upper_rt: f64,
    lower_rt: f64,
    x_min: f64,
    x_max: f64,
    upper_lane: &LaneMetrics,
    lower_lane: &LaneMetrics,
    color: &str,
) -> String {
    let x1 = data_to_px(upper_rt, x_min, x_max);
    let y1 = upper_lane.top + upper_lane.height * 0.92;
    let x2 = data_to_px(lower_rt, x_min, x_max);
    let y2 = lower_lane.top + lower_lane.height * 0.08;
    format!(
        r#"<line x1="{:.1}" y1="{:.1}" x2="{:.1}" y2="{:.1}" stroke="{}" stroke-width="1.8" stroke-opacity="0.82"/>"#,
        x1, y1, x2, y2, color
    )
}

fn marker_circle(
    rt: f64,
    x_min: f64,
    x_max: f64,
    lane: &LaneMetrics,
    fill: &str,
    radius: f64,
    y_fraction: f64,
) -> String {
    let x = data_to_px(rt, x_min, x_max);
    let y = lane.top + lane.height - y_fraction.clamp(0.0, 1.0) * (lane.height * 0.88);
    format!(
        r#"<circle cx="{:.1}" cy="{:.1}" r="{:.1}" fill="{}" fill-opacity="0.9"/>"#,
        x, y, radius, fill
    )
}

fn diamond_marker(
    rt: f64,
    x_min: f64,
    x_max: f64,
    lane: &LaneMetrics,
    fill: &str,
    radius: f64,
    y_fraction: f64,
) -> String {
    let cx = data_to_px(rt, x_min, x_max);
    let cy = lane.top + lane.height - y_fraction.clamp(0.0, 1.0) * (lane.height * 0.88);
    format!(
        r##"<polygon points="{:.1},{:.1} {:.1},{:.1} {:.1},{:.1} {:.1},{:.1}" fill="{}" fill-opacity="0.95" stroke="#991b1b" stroke-width="1"/>"##,
        cx,
        cy - radius,
        cx + radius,
        cy,
        cx,
        cy + radius,
        cx - radius,
        cy,
        fill
    )
}

fn reference_star(cx: f64, cy: f64, radius: f64) -> String {
    let inner_radius = radius * 0.42;
    let mut points = Vec::with_capacity(10);
    for idx in 0..10 {
        let angle = -std::f64::consts::FRAC_PI_2 + idx as f64 * std::f64::consts::PI / 5.0;
        let current_radius = if idx % 2 == 0 { radius } else { inner_radius };
        points.push(format!(
            "{:.2},{:.2}",
            cx + current_radius * angle.cos(),
            cy + current_radius * angle.sin()
        ));
    }

    format!(
        r##"<polygon points="{}" fill="#111827" stroke="#111827" stroke-width="1"/>"##,
        points.join(" ")
    )
}

fn plot_legend() -> String {
    let legend_x = LEFT_MARGIN;
    let legend_y = 62.0;
    let diamond_x = legend_x + 90.0;
    let diamond_y = legend_y + 7.0;
    let mut out = String::new();
    out.push_str(&reference_star(legend_x + 10.0, legend_y + 8.0, 6.0));
    out.push_str(&format!(
        r#"<text x="{:.1}" y="{:.1}" font-size="11">reference run</text>"#,
        legend_x + 24.0,
        legend_y + 11.0
    ));
    out.push_str(&format!(
        r##"<polygon points="{:.1},{:.1} {:.1},{:.1} {:.1},{:.1} {:.1},{:.1}" fill="#dc2626" fill-opacity="0.95" stroke="#991b1b" stroke-width="1"/>"##,
        diamond_x,
        diamond_y - 5.0,
        diamond_x + 5.0,
        diamond_y,
        diamond_x,
        diamond_y + 5.0,
        diamond_x - 5.0,
        diamond_y,
    ));
    out.push_str(&format!(
        r#"<text x="{:.1}" y="{:.1}" font-size="11">unmapped reference peak</text>"#,
        legend_x + 112.0,
        legend_y + 11.0
    ));
    out
}

fn series_polyline(
    xs: &[f64],
    ys: &[f64],
    x_min: f64,
    x_max: f64,
    lane: &LaneMetrics,
    color: &str,
    stroke_width: f64,
    opacity: f64,
) -> String {
    if xs.is_empty() || ys.is_empty() {
        return String::new();
    }

    let len = xs.len().min(ys.len());
    let (xs, ys) = downsample_series(&xs[..len], &ys[..len], MAX_POINTS_PER_TRACE);
    let mut points = String::new();
    for (x, y) in xs.iter().zip(ys.iter()) {
        let px = data_to_px(*x, x_min, x_max);
        let py = lane.top + lane.height - y.clamp(0.0, 1.0) * (lane.height * 0.88);
        points.push_str(&format!("{:.2},{:.2} ", px, py));
    }

    format!(
        r#"<polyline fill="none" stroke="{}" stroke-width="{:.2}" stroke-opacity="{:.3}" points="{}"/>"#,
        color,
        stroke_width,
        opacity,
        points.trim()
    )
}

fn downsample_series<'a>(xs: &'a [f64], ys: &'a [f64], max_points: usize) -> (Vec<f64>, Vec<f64>) {
    if xs.len() <= max_points {
        return (xs.to_vec(), ys.to_vec());
    }

    let mut out_x = Vec::with_capacity(max_points);
    let mut out_y = Vec::with_capacity(max_points);
    let last_index = xs.len() - 1;

    for i in 0..max_points {
        let idx = i * last_index / (max_points - 1);
        out_x.push(xs[idx]);
        out_y.push(ys[idx]);
    }

    (out_x, out_y)
}

fn data_to_px(x: f64, x_min: f64, x_max: f64) -> f64 {
    let width = SVG_WIDTH as f64 - LEFT_MARGIN - RIGHT_MARGIN;
    if (x_max - x_min).abs() < f64::EPSILON {
        return LEFT_MARGIN;
    }
    LEFT_MARGIN + (x - x_min) / (x_max - x_min) * width
}

fn scale_values(values: &[f64], divisor: f64) -> Vec<f64> {
    values.iter().map(|v| v / divisor).collect()
}

fn min_max_normalize_values(values: &[f64]) -> Vec<f64> {
    if values.is_empty() {
        return Vec::new();
    }

    let min_value = values.iter().copied().fold(f64::INFINITY, f64::min);
    let max_value = values
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);

    if (max_value - min_value).abs() < f64::EPSILON {
        return vec![0.0; values.len()];
    }

    values
        .iter()
        .map(|value| (value - min_value) / (max_value - min_value))
        .collect()
}

fn normalize_series(values: &[f64]) -> Vec<f64> {
    let max_value = values.iter().copied().fold(0.0_f64, f64::max).max(1e-12);
    scale_values(values, max_value)
}

fn range_from_slice(values: &[f64]) -> Option<(f64, f64)> {
    if values.is_empty() {
        return None;
    }

    let mut min_value = f64::INFINITY;
    let mut max_value = f64::NEG_INFINITY;
    for value in values {
        min_value = min_value.min(*value);
        max_value = max_value.max(*value);
    }
    Some((min_value, max_value))
}

fn format_optional_f64(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.6}"))
        .unwrap_or_default()
}

fn format_optional_i64(value: Option<i64>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn format_optional_i32(value: Option<i32>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn format_optional_isize(value: Option<isize>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn color_for_index(idx: usize) -> &'static str {
    const COLORS: [&str; 10] = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    ];
    COLORS[idx % COLORS.len()]
}

fn sanitize_filename(input: &str) -> String {
    input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn xml_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
