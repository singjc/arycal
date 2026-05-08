use arycal_common::chromatogram::AlignedRTPointPair;
use arycal_common::config::AlignmentConfig;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::f64;
use union_find::{QuickFindUf, UnionBySize, UnionFind};

use arycal_cloudpath::osw::{FeatureData, ValueEntryType};
use arycal_cloudpath::sqmass::TransitionGroup;
use arycal_common::{
    chromatogram::{apply_common_rt_space_single, AlignedChromatogram, Chromatogram},
    PeakMapping, PeakMappingCandidate,
};

use super::fast_fourier_lag::shift_chromatogram;

#[derive(Debug)]
pub enum ValueType {
    Int32(i32),
    Int64(i64),
    Float(f64),
    Text(String),
}

impl ValueType {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ValueType::Int32(v) => Some(*v as i64),
            ValueType::Int64(v) => Some(*v),
            ValueType::Float(v) => Some(*v as i64),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ValueType::Int32(v) => Some(*v as f64),
            ValueType::Int64(v) => Some(*v as f64),
            ValueType::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<String> {
        match self {
            ValueType::Text(s) => Some(s.clone()),
            _ => None,
        }
    }
}

impl From<ValueType> for i64 {
    fn from(value: ValueType) -> Self {
        match value {
            ValueType::Int32(v) => v as i64,
            ValueType::Int64(v) => v,
            ValueType::Float(v) => v as i64,
            ValueType::Text(_) => 0,
        }
    }
}

impl From<ValueType> for f64 {
    fn from(value: ValueType) -> Self {
        match value {
            ValueType::Int32(v) => v as f64,
            ValueType::Int64(v) => v as f64,
            ValueType::Float(v) => v,
            ValueType::Text(_) => 0.0,
        }
    }
}

impl From<ValueType> for String {
    fn from(value: ValueType) -> Self {
        match value {
            ValueType::Text(s) => s,
            _ => String::new(),
        }
    }
}

/// Enum for the alignment method. Either DTW, FFT or FFT-DTW.
/// FFT-DTW is a hybrid method that uses FFT for cross-correlation and DTW for local refinement.
#[derive(Debug, Clone)]
pub enum AlignmentMethod {
    DTW,
    FFT,
    FFTDTW,
}

impl Default for AlignmentMethod {
    fn default() -> Self {
        AlignmentMethod::FFT
    }
}

impl AlignmentMethod {
    pub fn as_str(&self) -> &str {
        match self {
            AlignmentMethod::DTW => "dtw",
            AlignmentMethod::FFT => "ffw",
            AlignmentMethod::FFTDTW => "fftdtw",
        }
    }
}

/// Enum for reference method. Either STAR, MST, or PROGRESSIVE.
/// - STAR uses a single rrandomly selected reference chromatogram.
/// - MST constructs a minimum spanning tree from pairwise distances and selects the centroid.
/// - PROGRESSIVE aligns chromatograms in a progressive manner. Selects the first chromatogram as the reference, and then aligns the next chromatogram to the reference. The aligned chromatogram is averaged with the reference, to be used as the new reference for the next chromatogram. This process is repeated until all chromatograms are aligned.
#[derive(Debug, Clone)]
pub enum ReferenceMethod {
    STAR,
    MST,
    PROGRESSIVE,
}

impl Default for ReferenceMethod {
    fn default() -> Self {
        ReferenceMethod::STAR
    }
}

impl ReferenceMethod {
    pub fn as_str(&self) -> &str {
        match self {
            ReferenceMethod::STAR => "star",
            ReferenceMethod::MST => "mst",
            ReferenceMethod::PROGRESSIVE => "progressive",
        }
    }
}

/// Calculates the Euclidean distance between two chromatograms.
///
/// # Parameters
/// - `chrom1`: First chromatogram
/// - `chrom2`: Second chromatogram
///
/// # Returns
/// - The Euclidean distance between the two chromatograms
pub fn calculate_distance(chrom1: &Chromatogram, chrom2: &Chromatogram) -> f64 {
    // Ensure the chromatograms have the same length
    if chrom1.intensities.len() != chrom2.intensities.len() {
        panic!("Chromatograms must have the same number of intensities");
    }

    // Calculate the Euclidean distance between the intensities
    let mut sum: f64 = 0.0;
    for (i1, i2) in chrom1.intensities.iter().zip(&chrom2.intensities) {
        sum += (i1 - i2).powi(2);
    }

    sum.sqrt() // Return the Euclidean distance (square root of sum of squares)
}

/// Constructs a minimum spanning tree (MST) from a list of distances between chromatograms.
///
/// # Parameters
/// - `distances`: A list of distances between chromatograms (chrom1, chrom2, distance)
/// - `num_chromatograms`: The total number of chromatograms
///
/// # Returns
/// - A list of edges in the MST (chrom1, chrom2, distance)
pub fn construct_mst(
    distances: &[(usize, usize, f64)],
    num_chromatograms: usize,
) -> Vec<(usize, usize, f64)> {
    // Use QuickFindUf with UnionBySize as the union strategy
    let mut uf: QuickFindUf<UnionBySize> = QuickFindUf::new(num_chromatograms);
    let mut mst_edges = Vec::new();

    // Sort edges by distance
    let mut sorted_distances = distances.to_vec();
    sorted_distances.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap());

    // Kruskal's algorithm
    for (i, j, dist) in sorted_distances {
        if uf.find(i) != uf.find(j) {
            uf.union(i, j);
            mst_edges.push((i, j, dist)); // Now return the distance as well
        }
    }

    mst_edges
}

/// Validates the widths of two peaks and switches the order if necessary.
pub fn validate_widths(left_width: f64, right_width: f64) -> (f64, f64) {
    if left_width >= right_width {
        // log::trace!(
        //     "Invalid widths: left_width ({}) is not smaller than right_width ({}). Switching the order.",
        //     left_width,
        //     right_width
        // );
        (right_width, left_width)
    } else {
        (left_width, right_width)
    }
}

#[derive(Debug, Clone)]
pub struct PeakMappingCandidateDebug {
    pub candidate_rank: usize,
    pub aligned_feature_id: i64,
    pub aligned_rt: f64,
    pub aligned_left_width: f64,
    pub aligned_right_width: f64,
    pub abs_rt_diff_to_target: f64,
    pub abs_rt_diff_to_reference: f64,
    pub within_tolerance: bool,
    pub selected_by_current_logic: bool,
    pub intensity: Option<f64>,
    pub normalized_summed_intensity: Option<f64>,
    pub peakgroup_rank: Option<i32>,
    pub qvalue: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct PeakMappingInspection {
    pub alignment_id: i64,
    pub reference_feature_id: i64,
    pub reference_rt: f64,
    pub reference_left_width: f64,
    pub reference_right_width: f64,
    pub mapped_target_rt: f64,
    pub roundtrip_reference_rt: Option<f64>,
    pub roundtrip_error: Option<f64>,
    pub lag: Option<isize>,
    pub candidate_total_count: usize,
    pub candidate_within_tolerance_count: usize,
    pub selected_feature_id: Option<i64>,
    pub selected_feature_rt: Option<f64>,
    pub selected_abs_rt_diff_to_target: Option<f64>,
    pub selected_abs_rt_diff_to_reference: Option<f64>,
    pub candidates: Vec<PeakMappingCandidateDebug>,
}

#[derive(Debug, Clone)]
struct FlattenedFeatureCandidate {
    feature_id: i64,
    rt: f64,
    left_width: f64,
    right_width: f64,
    intensity: Option<f64>,
    normalized_summed_intensity: Option<f64>,
    peakgroup_rank: Option<i32>,
    qvalue: Option<f64>,
}

#[derive(Debug, Clone)]
struct ReferencePeakDescriptor {
    alignment_id: i64,
    precursor_id: i32,
    run_id: i64,
    reference_feature_id: i64,
    reference_rt: f64,
    reference_left_width: f64,
    reference_right_width: f64,
    reference_filename: String,
    aligned_filename: String,
    mapped_target_rt: f64,
    expected_aligned_left_width: f64,
    expected_aligned_right_width: f64,
    roundtrip_error: Option<f64>,
    reference_intensity: Option<f64>,
}

#[derive(Debug, Clone)]
struct CandidateEdge {
    alignment_id: i64,
    precursor_id: i32,
    run_id: i64,
    reference_feature_id: i64,
    aligned_feature_id: i64,
    reference_rt: f64,
    mapped_target_rt: f64,
    aligned_rt: f64,
    reference_left_width: f64,
    reference_right_width: f64,
    expected_aligned_left_width: f64,
    expected_aligned_right_width: f64,
    aligned_left_width: f64,
    aligned_right_width: f64,
    reference_filename: String,
    aligned_filename: String,
    candidate_total_count: usize,
    candidate_within_tolerance_count: usize,
    within_tolerance: bool,
    candidate_score: f64,
    normalized_rt_error: f64,
    abs_rt_diff_to_target: f64,
    abs_rt_diff_to_reference: f64,
    roundtrip_error: Option<f64>,
    rt_score: f64,
    width_overlap_score: f64,
    width_similarity_score: f64,
    intensity_similarity_score: Option<f64>,
    rank_score: Option<f64>,
    qvalue_score: Option<f64>,
    feature_rank: Option<i32>,
    feature_qvalue: Option<f64>,
    feature_intensity: Option<f64>,
    feature_normalized_summed_intensity: Option<f64>,
}

/// Maps peaks across aligned chromatograms using the alignment information.
///
/// # Parameters
///
/// - `aligned_chrom`: The aligned chromatogram.
/// - `reference_features`: The features in the reference chromatogram.
/// - `aligned_features`: The features in the query chromatogram that are to be mapped to the aligned chromatogram.
/// - `rt_tolerance`: The retention time tolerance for mapping peaks.
// pub fn map_peaks_across_runs(
//     aligned_chrom: &AlignedChromatogram,
//     reference_features: Vec<FeatureData>,
//     aligned_features: Vec<FeatureData>,
//     rt_tolerance: f64,
//     alignment_config: &AlignmentConfig,
// ) -> Vec<PeakMapping> {
//     let mut peak_mappings = Vec::new();

//     log::trace!("There are {} reference features and {} query features to map", reference_features[0].feature_id.clone().unwrap().as_multiple().unwrap().len(), aligned_features[0].feature_id.clone().unwrap().as_multiple().unwrap().len() );

//     // Step 1: Map peaks from reference to query chromatogram
//     for ref_feature in &reference_features { // TODO: This is only a Vec of one element which contains inner Vecs of features
//         let mut alignment_id = 0;

//         for (i, &rt) in ref_feature.exp_rt.as_multiple().unwrap().iter().enumerate() {
//             // Get the aligned query rt that maps to the reference rt
//             let target_rt = map_retention_time(rt, &aligned_chrom.rt_mapping);

//             // Reverse the rt_mapping to get the aligned query rt in it's original space
//             // let original_target_rt = reverse_rt_mapping(target_rt, &aligned_chrom, alignment_config).unwrap();

//             log::trace!("Mapping closest aligned query RT: {:?} to reference RT feature: {:?}", target_rt, rt);

//             // Generate alignment_id based on reference_rt (or another unique identifier)
//             // let alignment_id = rt.to_bits() as i64; // Use the bits of the reference_rt as alignment_id

//             if let Some((aligned_feature_id, aligned_rt, aligned_left_width, aligned_right_width)) =
//                 find_closest_feature(target_rt, &aligned_features, rt_tolerance)
//             {
//                 log::trace!("Found query feature (id: {}) mapping to reference feature (id: {}): {} -> {}", aligned_feature_id, ref_feature.feature_id.clone().unwrap().as_multiple().unwrap()[i], aligned_rt, rt);

//                 // TODO: Really shouldn't need to have to validate widths, as these are derived from OpenSwath
//                 let (validated_left_width_ref, validated_right_width_ref) = validate_widths(
//                     ref_feature.left_width.as_ref().unwrap().as_multiple().unwrap()[i],
//                     ref_feature.right_width.as_ref().unwrap().as_multiple().unwrap()[i],
//                 );

//                 let (validated_left_width_aligned, validated_right_width_aligned) = validate_widths(
//                     aligned_left_width,
//                     aligned_right_width,
//                 );

//                 peak_mappings.push(PeakMapping {
//                     alignment_id,
//                     precursor_id: ref_feature.precursor_id.clone(),
//                     run_id: aligned_features[0].run_id.clone(),
//                     reference_feature_id: ref_feature.feature_id.clone().unwrap().as_multiple().unwrap()[i],
//                     aligned_feature_id,
//                     reference_rt: rt,
//                     aligned_rt,
//                     reference_left_width: validated_left_width_ref,
//                     reference_right_width: validated_right_width_ref,
//                     aligned_left_width: validated_left_width_aligned,
//                     aligned_right_width: validated_right_width_aligned,
//                     reference_filename: ref_feature.basename.clone(),
//                     aligned_filename: aligned_features[0].basename.clone(),
//                     label: 1,
//                     xcorr_coelution_to_ref: None,
//                     xcorr_shape_to_ref: None,
//                     mi_to_ref: None,
//                     xcorr_coelution_to_all: None,
//                     xcorr_shape_to_all: None,
//                     mi_to_all: None,
//                     rt_deviation: None,
//                     intensity_ratio: None,
//                 });
//             } else {
//                 log::trace!("Couldn't find a matching feature for reference RT: {:?} with id: {}", rt, ref_feature.feature_id.clone().unwrap().as_multiple().unwrap()[i]);
//                 // // Recover missing peak in the query chromatogram
//                 // log::trace!("Recovering missing peak in query chromatogram for reference RT: {:?}", rt);
//                 // let (validated_left_width_ref, validated_right_width_ref) = validate_widths(
//                 //     ref_feature.left_width.as_ref().unwrap().as_multiple().unwrap()[i],
//                 //     ref_feature.right_width.as_ref().unwrap().as_multiple().unwrap()[i],
//                 // );
//                 // peak_mappings.push(PeakMapping {
//                 //     alignment_id, // Use the same alignment_id for the same peak across runs
//                 //     reference_feature_id: *ref_feature.feature_id.clone().unwrap(),
//                 //     aligned_feature_id: -1, // Use -1 to indicate a missing peak
//                 //     reference_rt: rt,
//                 //     aligned_rt: target_rt,
//                 //     reference_left_width: validated_left_width_ref,
//                 //     reference_right_width: validated_right_width_ref,
//                 //     aligned_left_width: validated_left_width_ref, // Use reference values as placeholder
//                 //     aligned_right_width: validated_right_width_ref, // Use reference values as placeholder
//                 //     reference_filename: ref_feature.basename.clone(),
//                 //     aligned_filename: aligned_features[0].basename.clone(),
//                 //     label: 1,
//                 //     xcorr_coelution_to_ref: None,
//                 //     xcorr_shape_to_ref: None,
//                 //     mi_to_ref: None,
//                 //     xcorr_coelution_to_all: None,
//                 //     xcorr_shape_to_all: None,
//                 //     mi_to_all: None,
//                 //     rt_deviation: None,
//                 //     intensity_ratio: None,
//                 // });
//             }
//             alignment_id += 1;
//         }
//     }

//     // // Step 2: Map peaks from query to reference chromatogram (to recover missing peaks in the reference)
//     // for aligned_feature in &aligned_features {
//     //     for (i, &rt) in aligned_feature.exp_rt.as_multiple().unwrap().iter().enumerate() {
//     //         let reference_rt = map_retention_time(rt, &aligned_chrom.rt_mapping);

//     //         // println!("Mapping reference RT: {:?} to query aligned RT: {:?}", reference_rt, rt);

//     //         // Generate alignment_id based on reference_rt (or another unique identifier)
//     //         let alignment_id = reference_rt.to_bits() as i64; // Use the bits of the reference_rt as alignment_id

//     //         if let Some((reference_feature_id, reference_rt, reference_left_width, reference_right_width)) =
//     //             find_closest_feature(reference_rt, &reference_features, rt_tolerance)
//     //         {
//     //             // Skip if the peak is already mapped in Step 1
//     //             if !peak_mappings.iter().any(|m| m.aligned_feature_id == *aligned_feature.feature_id.clone().unwrap()) {
//     //                 // println!("Found reference feature mapping to query feature: {:?}", reference_feature_id);
//     //                 peak_mappings.push(PeakMapping {
//     //                     alignment_id, // Use the same alignment_id for the same peak across runs
//     //                     reference_feature_id,
//     //                     aligned_feature_id: *aligned_feature.feature_id.clone().unwrap(),
//     //                     reference_rt,
//     //                     aligned_rt: rt,
//     //                     reference_left_width,
//     //                     reference_right_width,
//     //                     aligned_left_width: aligned_feature.left_width.as_ref().unwrap().as_multiple().unwrap()[i],
//     //                     aligned_right_width: aligned_feature.right_width.as_ref().unwrap().as_multiple().unwrap()[i],
//     //                     reference_filename: reference_features[0].basename.clone(),
//     //                     aligned_filename: aligned_feature.basename.clone(),
//     //                 });
//     //             }
//     //         } else {
//     //             // Recover missing peak in the reference chromatogram
//     //             // println!("Recovering missing peak in reference chromatogram for query RT: {:?}", rt);
//     //             peak_mappings.push(PeakMapping {
//     //                 alignment_id, // Use the same alignment_id for the same peak across runs
//     //                 reference_feature_id: -1, // Use -1 to indicate a missing peak
//     //                 aligned_feature_id: *aligned_feature.feature_id.clone().unwrap(),
//     //                 reference_rt,
//     //                 aligned_rt: rt,
//     //                 reference_left_width: aligned_feature.left_width.as_ref().unwrap().as_multiple().unwrap()[i], // Use query values as placeholder
//     //                 reference_right_width: aligned_feature.right_width.as_ref().unwrap().as_multiple().unwrap()[i], // Use query values as placeholder
//     //                 aligned_left_width: aligned_feature.left_width.as_ref().unwrap().as_multiple().unwrap()[i],
//     //                 aligned_right_width: aligned_feature.right_width.as_ref().unwrap().as_multiple().unwrap()[i],
//     //                 reference_filename: reference_features[0].basename.clone(),
//     //                 aligned_filename: aligned_feature.basename.clone(),
//     //             });
//     //         }
//     //     }
//     // }

//     // Step 3: Remove overlapping peaks
//     // let filtered_peaks = remove_overlapping_peaks(peak_mappings);

//     peak_mappings
// }

pub fn map_peaks_across_runs(
    aligned_chrom: &AlignedChromatogram,
    reference_features: Vec<FeatureData>,
    aligned_features: Vec<FeatureData>,
    rt_tolerance: f64,
    alignment_config: &AlignmentConfig,
) -> Vec<PeakMapping> {
    map_peaks_across_runs_with_confidence(
        aligned_chrom,
        reference_features,
        aligned_features,
        rt_tolerance,
        alignment_config,
    )
    .0
}

pub fn map_peaks_across_runs_with_confidence(
    aligned_chrom: &AlignedChromatogram,
    reference_features: Vec<FeatureData>,
    aligned_features: Vec<FeatureData>,
    rt_tolerance: f64,
    alignment_config: &AlignmentConfig,
) -> (Vec<PeakMapping>, Vec<PeakMappingCandidate>) {
    if reference_features.is_empty() || aligned_features.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let ref_feature = &reference_features[0];
    let aligned_feature = &aligned_features[0];
    let flattened_candidates = flatten_feature_candidates(&aligned_features);
    if flattened_candidates.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let reference_peaks = flatten_reference_peaks(
        ref_feature,
        aligned_feature,
        aligned_chrom,
        alignment_config,
    );
    if reference_peaks.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let candidate_rows: Vec<Vec<CandidateEdge>> = reference_peaks
        .par_iter()
        .map(|reference_peak| {
            build_candidate_edges(reference_peak, &flattened_candidates, rt_tolerance)
        })
        .collect();

    let mut eligible_edges: Vec<CandidateEdge> = candidate_rows
        .iter()
        .flat_map(|rows| rows.iter().filter(|edge| edge.within_tolerance).cloned())
        .collect();

    eligible_edges.sort_by(|a, b| {
        b.candidate_score
            .partial_cmp(&a.candidate_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(
                a.abs_rt_diff_to_target
                    .partial_cmp(&b.abs_rt_diff_to_target)
                    .unwrap_or(std::cmp::Ordering::Equal),
            )
            .then(
                a.feature_rank
                    .unwrap_or(i32::MAX)
                    .cmp(&b.feature_rank.unwrap_or(i32::MAX)),
            )
            .then(a.aligned_feature_id.cmp(&b.aligned_feature_id))
    });

    let mut selected_by_alignment_id: HashMap<i64, CandidateEdge> = HashMap::new();
    let mut used_aligned_feature_ids: HashSet<i64> = HashSet::new();
    for edge in eligible_edges {
        if selected_by_alignment_id.contains_key(&edge.alignment_id)
            || used_aligned_feature_ids.contains(&edge.aligned_feature_id)
        {
            continue;
        }
        used_aligned_feature_ids.insert(edge.aligned_feature_id);
        selected_by_alignment_id.insert(edge.alignment_id, edge);
    }

    let mut selected_peak_mappings = Vec::new();
    let mut candidate_records = Vec::new();

    for mut rows in candidate_rows {
        if rows.is_empty() {
            continue;
        }

        rows.sort_by(|a, b| {
            b.candidate_score
                .partial_cmp(&a.candidate_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(
                    a.abs_rt_diff_to_target
                        .partial_cmp(&b.abs_rt_diff_to_target)
                        .unwrap_or(std::cmp::Ordering::Equal),
                )
                .then(a.aligned_feature_id.cmp(&b.aligned_feature_id))
        });

        let alignment_id = rows[0].alignment_id;
        let selected_edge = selected_by_alignment_id.get(&alignment_id);
        let second_best_score = selected_edge.and_then(|selected| {
            rows.iter()
                .filter(|row| {
                    row.within_tolerance && row.aligned_feature_id != selected.aligned_feature_id
                })
                .map(|row| row.candidate_score)
                .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        });

        let score_margin = selected_edge.map(|selected| {
            let next_best = second_best_score.unwrap_or(0.0);
            (selected.candidate_score - next_best).max(0.0)
        });

        let mapping_confidence = selected_edge.map(|selected| {
            let margin = score_margin.unwrap_or(selected.candidate_score);
            let normalized_margin = if selected.candidate_score > 0.0 {
                (margin / selected.candidate_score).clamp(0.0, 1.0)
            } else {
                0.0
            };
            let roundtrip_score = selected
                .roundtrip_error
                .map(|err| {
                    if rt_tolerance <= 0.0 {
                        1.0
                    } else {
                        (1.0 - (err / rt_tolerance).clamp(0.0, 1.0)).clamp(0.0, 1.0)
                    }
                })
                .unwrap_or(0.5);
            let uniqueness_score = if selected.candidate_within_tolerance_count <= 1 {
                1.0
            } else {
                (1.0 / selected.candidate_within_tolerance_count as f64).clamp(0.0, 1.0)
            };

            (0.55 * selected.candidate_score
                + 0.25 * normalized_margin
                + 0.10 * roundtrip_score
                + 0.10 * uniqueness_score)
                .clamp(0.0, 1.0)
        });

        if let Some(selected) = selected_edge {
            selected_peak_mappings.push(PeakMapping {
                alignment_id: selected.alignment_id,
                precursor_id: selected.precursor_id,
                run_id: selected.run_id,
                reference_feature_id: selected.reference_feature_id,
                aligned_feature_id: selected.aligned_feature_id,
                reference_rt: selected.reference_rt,
                aligned_rt: selected.aligned_rt,
                reference_left_width: selected.reference_left_width,
                reference_right_width: selected.reference_right_width,
                aligned_left_width: selected.aligned_left_width,
                aligned_right_width: selected.aligned_right_width,
                reference_filename: selected.reference_filename.clone(),
                aligned_filename: selected.aligned_filename.clone(),
                label: 1,
                xcorr_coelution_to_ref: None,
                xcorr_shape_to_ref: None,
                mi_to_ref: None,
                xcorr_coelution_to_all: None,
                xcorr_shape_to_all: None,
                mi_to_all: None,
                rt_deviation: None,
                intensity_ratio: None,
                mapped_target_rt: Some(selected.mapped_target_rt),
                roundtrip_error: selected.roundtrip_error,
                candidate_total_count: Some(selected.candidate_total_count as i32),
                candidate_within_tolerance_count: Some(
                    selected.candidate_within_tolerance_count as i32,
                ),
                normalized_rt_error: Some(selected.normalized_rt_error),
                mapping_score: Some(selected.candidate_score),
                mapping_confidence,
                score_margin_to_second_best: score_margin,
                feature_rank: selected.feature_rank,
                feature_qvalue: selected.feature_qvalue,
            });
        }

        for (candidate_rank, row) in rows.into_iter().enumerate() {
            let is_selected = selected_edge
                .map(|selected| selected.aligned_feature_id == row.aligned_feature_id)
                .unwrap_or(false);
            candidate_records.push(PeakMappingCandidate {
                alignment_id: row.alignment_id,
                precursor_id: row.precursor_id,
                run_id: row.run_id,
                reference_feature_id: row.reference_feature_id,
                aligned_feature_id: row.aligned_feature_id,
                reference_rt: row.reference_rt,
                mapped_target_rt: row.mapped_target_rt,
                aligned_rt: row.aligned_rt,
                reference_left_width: row.reference_left_width,
                reference_right_width: row.reference_right_width,
                expected_aligned_left_width: row.expected_aligned_left_width,
                expected_aligned_right_width: row.expected_aligned_right_width,
                aligned_left_width: row.aligned_left_width,
                aligned_right_width: row.aligned_right_width,
                reference_filename: row.reference_filename,
                aligned_filename: row.aligned_filename,
                candidate_rank: (candidate_rank + 1) as i32,
                candidate_total_count: row.candidate_total_count as i32,
                candidate_within_tolerance_count: row.candidate_within_tolerance_count as i32,
                selected: is_selected,
                within_tolerance: row.within_tolerance,
                candidate_score: row.candidate_score,
                mapping_confidence: is_selected.then_some(mapping_confidence.unwrap_or(0.0)),
                score_margin_to_next: is_selected.then_some(score_margin.unwrap_or(0.0)),
                normalized_rt_error: row.normalized_rt_error,
                abs_rt_diff_to_target: row.abs_rt_diff_to_target,
                abs_rt_diff_to_reference: row.abs_rt_diff_to_reference,
                roundtrip_error: row.roundtrip_error,
                rt_score: row.rt_score,
                width_overlap_score: row.width_overlap_score,
                width_similarity_score: row.width_similarity_score,
                intensity_similarity_score: row.intensity_similarity_score,
                rank_score: row.rank_score,
                qvalue_score: row.qvalue_score,
                feature_rank: row.feature_rank,
                feature_qvalue: row.feature_qvalue,
                feature_intensity: row.feature_intensity,
                feature_normalized_summed_intensity: row.feature_normalized_summed_intensity,
            });
        }
    }

    (selected_peak_mappings, candidate_records)
}

pub fn inspect_peak_mapping_candidates(
    aligned_chrom: &AlignedChromatogram,
    reference_features: &[FeatureData],
    aligned_features: &[FeatureData],
    rt_tolerance: f64,
    alignment_config: &AlignmentConfig,
) -> Vec<PeakMappingInspection> {
    if reference_features.is_empty() || aligned_features.is_empty() {
        return Vec::new();
    }

    let ref_feature = &reference_features[0];
    let ref_len = value_entry_len(&ref_feature.exp_rt);
    let flattened_candidates = flatten_feature_candidates(aligned_features);

    (0..ref_len)
        .filter_map(|i| {
            let reference_feature_id = optional_value_entry_at(ref_feature.feature_id.as_ref(), i)?;
            let reference_rt = value_entry_at(&ref_feature.exp_rt, i)?;
            let reference_left_width = optional_value_entry_at(ref_feature.left_width.as_ref(), i)?;
            let reference_right_width =
                optional_value_entry_at(ref_feature.right_width.as_ref(), i)?;
            let mapped_target_rt = map_retention_time(reference_rt, &aligned_chrom.rt_mapping);
            let roundtrip_reference_rt =
                reverse_rt_mapping(mapped_target_rt, aligned_chrom, alignment_config);
            let roundtrip_error = roundtrip_reference_rt.map(|rt| (rt - reference_rt).abs());

            let selected = find_closest_feature(mapped_target_rt, aligned_features, rt_tolerance);

            let mut candidates: Vec<PeakMappingCandidateDebug> = flattened_candidates
                .iter()
                .map(|candidate| PeakMappingCandidateDebug {
                    candidate_rank: 0,
                    aligned_feature_id: candidate.feature_id,
                    aligned_rt: candidate.rt,
                    aligned_left_width: candidate.left_width,
                    aligned_right_width: candidate.right_width,
                    abs_rt_diff_to_target: (candidate.rt - mapped_target_rt).abs(),
                    abs_rt_diff_to_reference: (candidate.rt - reference_rt).abs(),
                    within_tolerance: (candidate.rt - mapped_target_rt).abs() <= rt_tolerance,
                    selected_by_current_logic: selected
                        .as_ref()
                        .map(|(feature_id, rt, left_width, right_width)| {
                            *feature_id == candidate.feature_id
                                && (candidate.rt - *rt).abs() < 1e-6
                                && (candidate.left_width - *left_width).abs() < 1e-6
                                && (candidate.right_width - *right_width).abs() < 1e-6
                        })
                        .unwrap_or(false),
                    intensity: candidate.intensity,
                    normalized_summed_intensity: candidate.normalized_summed_intensity,
                    peakgroup_rank: candidate.peakgroup_rank,
                    qvalue: candidate.qvalue,
                })
                .collect();

            candidates.sort_by(|a, b| {
                a.abs_rt_diff_to_target
                    .partial_cmp(&b.abs_rt_diff_to_target)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.aligned_feature_id.cmp(&b.aligned_feature_id))
            });
            for (candidate_rank, candidate) in candidates.iter_mut().enumerate() {
                candidate.candidate_rank = candidate_rank + 1;
            }

            let candidate_total_count = candidates.len();
            let candidate_within_tolerance_count = candidates
                .iter()
                .filter(|candidate| candidate.within_tolerance)
                .count();
            let selected_candidate = candidates
                .iter()
                .find(|candidate| candidate.selected_by_current_logic);

            Some(PeakMappingInspection {
                alignment_id: i as i64,
                reference_feature_id,
                reference_rt,
                reference_left_width,
                reference_right_width,
                mapped_target_rt,
                roundtrip_reference_rt,
                roundtrip_error,
                lag: aligned_chrom.lag,
                candidate_total_count,
                candidate_within_tolerance_count,
                selected_feature_id: selected_candidate
                    .map(|candidate| candidate.aligned_feature_id),
                selected_feature_rt: selected_candidate.map(|candidate| candidate.aligned_rt),
                selected_abs_rt_diff_to_target: selected_candidate
                    .map(|candidate| candidate.abs_rt_diff_to_target),
                selected_abs_rt_diff_to_reference: selected_candidate
                    .map(|candidate| candidate.abs_rt_diff_to_reference),
                candidates,
            })
        })
        .collect()
}

/// Removes overlapping peaks within the same run by comparing peak boundaries.
fn remove_overlapping_peaks(peak_mappings: Vec<PeakMapping>) -> Vec<PeakMapping> {
    let mut filtered_peaks = Vec::new();

    // Group peaks by run (filename or chromatogram ID)
    let mut peaks_grouped: HashMap<String, Vec<PeakMapping>> = HashMap::new();
    for peak in peak_mappings {
        let run_id = peak.aligned_filename.clone(); // Assuming `aligned_filename` is part of PeakMapping
        peaks_grouped
            .entry(run_id)
            .or_insert_with(Vec::new)
            .push(peak);
    }

    // Process each group of peaks separately
    for (_, peaks) in peaks_grouped {
        let mut non_overlapping_peaks = Vec::new();

        // Sort peaks by retention time (reference_rt or aligned_rt)
        let mut sorted_peaks = peaks.clone();
        sorted_peaks.sort_by(|a, b| a.reference_rt.partial_cmp(&b.reference_rt).unwrap());

        // Iterate through sorted peaks and remove overlaps
        let mut prev_peak: Option<PeakMapping> = None;
        for peak in sorted_peaks {
            if let Some(prev) = &prev_peak {
                // log::trace!("Checking for overlapping peaks: Peak 1 (RT: {}), Peak 2 (RT: {})", prev.reference_rt, peak.reference_rt);
                // Check if the current peak overlaps with the previous peak
                let current_left = peak.reference_left_width;
                let current_right = peak.reference_right_width;
                let prev_left = prev.reference_left_width;
                let prev_right = prev.reference_right_width;

                if current_left <= prev_right && current_right >= prev_left {
                    // Overlapping peaks detected
                    // println!(
                    //     "Overlapping peaks detected: Peak 1 (RT: {}-{}), Peak 2 (RT: {}-{})",
                    //     prev_left, prev_right, current_left, current_right
                    // );

                    // Resolve overlapping peaks based on feature IDs
                    if peak.aligned_feature_id != -1 || peak.reference_feature_id != -1 {
                        // Prefer the peak with a valid feature ID
                        if prev.aligned_feature_id == -1 && prev.reference_feature_id == -1 {
                            // log::trace!("Removing overlapping peaks with missing feature IDs");
                            // Replace the previous peak with the current one
                            non_overlapping_peaks.pop();
                            non_overlapping_peaks.push(peak.clone());
                        }
                    } else {
                        // log::trace!("Both peaks have missing feature IDs: computing a consensus peak");
                        // Both peaks have missing IDs: compute a consensus peak
                        let consensus_peak = PeakMapping {
                            alignment_id: peak.alignment_id,
                            precursor_id: peak.precursor_id,
                            run_id: peak.run_id,
                            reference_feature_id: -1,
                            aligned_feature_id: -1,
                            reference_rt: (prev.reference_rt + peak.reference_rt) / 2.0,
                            aligned_rt: (prev.aligned_rt + peak.aligned_rt) / 2.0,
                            reference_left_width: (prev.reference_left_width
                                + peak.reference_left_width)
                                / 2.0,
                            reference_right_width: (prev.reference_right_width
                                + peak.reference_right_width)
                                / 2.0,
                            aligned_left_width: (prev.aligned_left_width + peak.aligned_left_width)
                                / 2.0,
                            aligned_right_width: (prev.aligned_right_width
                                + peak.aligned_right_width)
                                / 2.0,
                            reference_filename: peak.reference_filename.clone(),
                            aligned_filename: peak.aligned_filename.clone(),
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
                        };
                        non_overlapping_peaks.pop();
                        non_overlapping_peaks.push(consensus_peak);
                    }
                } else {
                    // No overlap: add the current peak
                    non_overlapping_peaks.push(peak.clone());
                }
            } else {
                // First peak: add it
                non_overlapping_peaks.push(peak.clone());
            }

            // Update the previous peak
            prev_peak = Some(peak);
        }

        // Add non-overlapping peaks to the final list
        filtered_peaks.extend(non_overlapping_peaks);
    }

    filtered_peaks
}

// /// Maps a retention time from the reference chromatogram to the aligned chromatogram.
// fn map_retention_time(rt: f64, rt_mapping: &[HashMap<String, f64>]) -> f64 {
//     // If the mapping is empty, return the original RT (no mapping)
//     // TODO: Should we panic here instead?
//     if rt_mapping.is_empty() {
//         return rt;
//     }

//     // Find the nearest points in the mapping for interpolation
//     let mut lower_idx = 0;
//     let mut upper_idx = rt_mapping.len() - 1;

//     // Binary search to find the nearest lower and upper points
//     for (idx, map) in rt_mapping.iter().enumerate() {
//         let rt1 = *map
//             .get("rt1").unwrap();
//         if rt1 <= rt {
//             lower_idx = idx;
//         } else {
//             upper_idx = idx;
//             break;
//         }
//     }

//     // Get the lower and upper RT pairs
//     let lower_map = &rt_mapping[lower_idx];
//     let upper_map = &rt_mapping[upper_idx];

//     let rt1_lower = *lower_map
//         .get("rt1").unwrap();
//     let rt2_lower = *lower_map
//         .get("rt2").unwrap();

//     let rt1_upper = *upper_map
//         .get("rt1").unwrap();
//     let rt2_upper = *upper_map
//         .get("rt2").unwrap();

//     // If the RT matches exactly, return the mapped RT
//     if rt == rt1_lower {
//         return rt2_lower;
//     }
//     if rt == rt1_upper {
//         return rt2_upper;
//     }

//     // Interpolate between the nearest points
//     let slope = (rt2_upper - rt2_lower) / (rt1_upper - rt1_lower);
//     let mapped_rt = rt2_lower + slope * (rt - rt1_lower);

//     mapped_rt
// }

/// Maps a retention time from the reference chromatogram to the aligned chromatogram.
fn map_retention_time(rt: f64, rt_mapping: &[AlignedRTPointPair]) -> f64 {
    // Handle empty mapping case
    if rt_mapping.is_empty() {
        return rt;
    }

    // Convert input to f32 for comparison (since our mapping uses f32)
    let rt_f32 = rt as f32;

    // Binary search to find the insertion point
    let idx = match rt_mapping.binary_search_by(|pair| pair.rt1.partial_cmp(&rt_f32).unwrap()) {
        Ok(exact_idx) => return rt_mapping[exact_idx].rt2 as f64, // Exact match
        Err(insert_idx) => insert_idx,
    };

    // Handle edge cases
    if idx == 0 {
        return rt_mapping[0].rt2 as f64;
    }
    if idx >= rt_mapping.len() {
        return rt_mapping.last().unwrap().rt2 as f64;
    }

    // Get the neighboring points for interpolation
    let lower = &rt_mapping[idx - 1];
    let upper = &rt_mapping[idx];

    // Linear interpolation
    let ratio = (rt_f32 - lower.rt1) / (upper.rt1 - lower.rt1);
    let mapped_rt = lower.rt2 + ratio * (upper.rt2 - lower.rt2);

    mapped_rt as f64
}

/// Finds the closest retention time in the aligned feature's exp_rt vector.
/// Returns (feature_id, rt, left_width, right_width)
fn find_closest_feature(
    target_rt: f64,
    features: &[FeatureData],
    tolerance: f64,
) -> Option<(i64, f64, f64, f64)> {
    features
        .iter()
        .flat_map(|feature| {
            feature
                .exp_rt
                .as_multiple()
                .unwrap()
                .iter()
                .enumerate()
                .map(|(i, &rt)| {
                    (
                        feature.feature_id.as_ref().unwrap().as_multiple().unwrap()[i],
                        rt,
                        feature.left_width.as_ref().unwrap().as_multiple().unwrap()[i],
                        feature.right_width.as_ref().unwrap().as_multiple().unwrap()[i],
                        (rt - target_rt).abs(),
                    )
                })
        })
        .filter(|(_, _, _, _, diff)| *diff <= tolerance)
        .min_by(|(_, _, _, _, diff1), (_, _, _, _, diff2)| {
            diff1
                .partial_cmp(diff2)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(id, rt, left, right, _)| (id, rt, left, right))
}

fn flatten_reference_peaks(
    reference_feature: &FeatureData,
    aligned_feature: &FeatureData,
    aligned_chrom: &AlignedChromatogram,
    alignment_config: &AlignmentConfig,
) -> Vec<ReferencePeakDescriptor> {
    let len = value_entry_len(&reference_feature.exp_rt);
    (0..len)
        .filter_map(|idx| {
            let reference_feature_id =
                optional_value_entry_at(reference_feature.feature_id.as_ref(), idx)?;
            let reference_rt = value_entry_at(&reference_feature.exp_rt, idx)?;
            let reference_left_width =
                optional_value_entry_at(reference_feature.left_width.as_ref(), idx)?;
            let reference_right_width =
                optional_value_entry_at(reference_feature.right_width.as_ref(), idx)?;
            let mapped_target_rt = map_retention_time(reference_rt, &aligned_chrom.rt_mapping);
            let mapped_left_width =
                map_retention_time(reference_left_width, &aligned_chrom.rt_mapping);
            let mapped_right_width =
                map_retention_time(reference_right_width, &aligned_chrom.rt_mapping);
            let (expected_aligned_left_width, expected_aligned_right_width) =
                validate_widths(mapped_left_width, mapped_right_width);
            let roundtrip_error =
                reverse_rt_mapping(mapped_target_rt, aligned_chrom, alignment_config)
                    .map(|rt| (rt - reference_rt).abs());

            Some(ReferencePeakDescriptor {
                alignment_id: idx as i64,
                precursor_id: reference_feature.precursor_id,
                run_id: aligned_feature.run_id,
                reference_feature_id,
                reference_rt,
                reference_left_width,
                reference_right_width,
                reference_filename: reference_feature.basename.clone(),
                aligned_filename: aligned_feature.basename.clone(),
                mapped_target_rt,
                expected_aligned_left_width,
                expected_aligned_right_width,
                roundtrip_error,
                reference_intensity: optional_value_entry_at(
                    reference_feature.intensity.as_ref(),
                    idx,
                ),
            })
        })
        .collect()
}

fn build_candidate_edges(
    reference_peak: &ReferencePeakDescriptor,
    flattened_candidates: &[FlattenedFeatureCandidate],
    rt_tolerance: f64,
) -> Vec<CandidateEdge> {
    let total_candidates = flattened_candidates.len();
    let mut all_edges: Vec<CandidateEdge> = flattened_candidates
        .iter()
        .map(|candidate| {
            let abs_rt_diff_to_target = (candidate.rt - reference_peak.mapped_target_rt).abs();
            let abs_rt_diff_to_reference = (candidate.rt - reference_peak.reference_rt).abs();
            let within_tolerance = abs_rt_diff_to_target <= rt_tolerance;
            let expected_width = peak_width(
                reference_peak.expected_aligned_left_width,
                reference_peak.expected_aligned_right_width,
            );
            let candidate_width = peak_width(candidate.left_width, candidate.right_width);
            let normalized_rt_error = abs_rt_diff_to_target / expected_width.max(1.0);
            let rt_score = if rt_tolerance > 0.0 {
                (1.0 - (abs_rt_diff_to_target / rt_tolerance).clamp(0.0, 1.0)).clamp(0.0, 1.0)
            } else {
                0.0
            };
            let width_overlap_score = interval_overlap_score(
                reference_peak.expected_aligned_left_width,
                reference_peak.expected_aligned_right_width,
                candidate.left_width,
                candidate.right_width,
            );
            let width_similarity_score = (1.0
                - ((candidate_width - expected_width).abs()
                    / candidate_width.max(expected_width).max(1.0)))
            .clamp(0.0, 1.0);
            let intensity_similarity_score =
                intensity_similarity_score(reference_peak.reference_intensity, candidate.intensity);
            let rank_score = candidate
                .peakgroup_rank
                .map(|rank| 1.0 / rank.max(1) as f64);
            let qvalue_score = candidate
                .qvalue
                .map(|qvalue| (1.0 - qvalue.clamp(0.0, 1.0)).clamp(0.0, 1.0));
            let candidate_score = (0.45 * rt_score
                + 0.20 * width_overlap_score
                + 0.10 * width_similarity_score
                + 0.10 * rank_score.unwrap_or(0.5)
                + 0.10 * qvalue_score.unwrap_or(0.5)
                + 0.05 * intensity_similarity_score.unwrap_or(0.5))
            .clamp(0.0, 1.0);

            CandidateEdge {
                alignment_id: reference_peak.alignment_id,
                precursor_id: reference_peak.precursor_id,
                run_id: reference_peak.run_id,
                reference_feature_id: reference_peak.reference_feature_id,
                aligned_feature_id: candidate.feature_id,
                reference_rt: reference_peak.reference_rt,
                mapped_target_rt: reference_peak.mapped_target_rt,
                aligned_rt: candidate.rt,
                reference_left_width: reference_peak.reference_left_width,
                reference_right_width: reference_peak.reference_right_width,
                expected_aligned_left_width: reference_peak.expected_aligned_left_width,
                expected_aligned_right_width: reference_peak.expected_aligned_right_width,
                aligned_left_width: candidate.left_width,
                aligned_right_width: candidate.right_width,
                reference_filename: reference_peak.reference_filename.clone(),
                aligned_filename: reference_peak.aligned_filename.clone(),
                candidate_total_count: total_candidates,
                candidate_within_tolerance_count: 0,
                within_tolerance,
                candidate_score,
                normalized_rt_error,
                abs_rt_diff_to_target,
                abs_rt_diff_to_reference,
                roundtrip_error: reference_peak.roundtrip_error,
                rt_score,
                width_overlap_score,
                width_similarity_score,
                intensity_similarity_score,
                rank_score,
                qvalue_score,
                feature_rank: candidate.peakgroup_rank,
                feature_qvalue: candidate.qvalue,
                feature_intensity: candidate.intensity,
                feature_normalized_summed_intensity: candidate.normalized_summed_intensity,
            }
        })
        .collect();

    let within_tolerance_count = all_edges
        .iter()
        .filter(|edge| edge.within_tolerance)
        .count();
    for edge in &mut all_edges {
        edge.candidate_within_tolerance_count = within_tolerance_count;
    }

    if within_tolerance_count > 0 {
        all_edges.retain(|edge| edge.within_tolerance);
        all_edges
    } else {
        all_edges
            .into_iter()
            .min_by(|a, b| {
                a.abs_rt_diff_to_target
                    .partial_cmp(&b.abs_rt_diff_to_target)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.aligned_feature_id.cmp(&b.aligned_feature_id))
            })
            .into_iter()
            .collect()
    }
}

fn flatten_feature_candidates(features: &[FeatureData]) -> Vec<FlattenedFeatureCandidate> {
    features
        .iter()
        .flat_map(|feature| {
            let len = value_entry_len(&feature.exp_rt);
            (0..len).filter_map(move |idx| {
                Some(FlattenedFeatureCandidate {
                    feature_id: optional_value_entry_at(feature.feature_id.as_ref(), idx)?,
                    rt: value_entry_at(&feature.exp_rt, idx)?,
                    left_width: optional_value_entry_at(feature.left_width.as_ref(), idx)?,
                    right_width: optional_value_entry_at(feature.right_width.as_ref(), idx)?,
                    intensity: optional_value_entry_at(feature.intensity.as_ref(), idx),
                    normalized_summed_intensity: optional_value_entry_at(
                        feature.normalized_summed_intensity.as_ref(),
                        idx,
                    ),
                    peakgroup_rank: optional_value_entry_at(feature.rank.as_ref(), idx),
                    qvalue: optional_value_entry_at(feature.qvalue.as_ref(), idx),
                })
            })
        })
        .collect()
}

fn value_entry_len<T>(entry: &ValueEntryType<T>) -> usize {
    match entry {
        ValueEntryType::Single(_) => 1,
        ValueEntryType::Multiple(values) => values.len(),
    }
}

fn value_entry_at<T: Copy>(entry: &ValueEntryType<T>, idx: usize) -> Option<T> {
    match entry {
        ValueEntryType::Single(value) => (idx == 0).then_some(*value),
        ValueEntryType::Multiple(values) => values.get(idx).copied(),
    }
}

fn optional_value_entry_at<T: Copy>(entry: Option<&ValueEntryType<T>>, idx: usize) -> Option<T> {
    entry.and_then(|entry| value_entry_at(entry, idx))
}

fn peak_width(left_width: f64, right_width: f64) -> f64 {
    let (left_width, right_width) = validate_widths(left_width, right_width);
    (right_width - left_width).abs().max(1.0)
}

fn interval_overlap_score(a_left: f64, a_right: f64, b_left: f64, b_right: f64) -> f64 {
    let (a_left, a_right) = validate_widths(a_left, a_right);
    let (b_left, b_right) = validate_widths(b_left, b_right);
    let overlap = (a_right.min(b_right) - a_left.max(b_left)).max(0.0);
    if overlap <= 0.0 {
        return 0.0;
    }

    let union = (a_right.max(b_right) - a_left.min(b_left)).max(1.0);
    (overlap / union).clamp(0.0, 1.0)
}

fn intensity_similarity_score(
    reference_intensity: Option<f64>,
    aligned_intensity: Option<f64>,
) -> Option<f64> {
    let reference_intensity = reference_intensity?;
    let aligned_intensity = aligned_intensity?;
    let log_ratio = ((aligned_intensity + 1.0).ln() - (reference_intensity + 1.0).ln()).abs();
    Some((1.0 / (1.0 + log_ratio)).clamp(0.0, 1.0))
}

/// Applies alignment to a list of transition groups using an existing alignment result.
///
/// # Parameters
/// - `transition_groups`: The transition groups to be aligned
/// - `aligned_chromatograms`: The aligned chromatograms
/// - `common_rt_space`: The common retention time space
/// - `alignment_config`: The alignment configuration
pub fn apply_post_alignment_to_trgrp(
    transition_groups: Vec<TransitionGroup>,
    aligned_chromatograms: &Vec<AlignedChromatogram>,
    common_rt_space: &Vec<f64>,
    alignment_config: &AlignmentConfig,
) -> Vec<TransitionGroup> {
    let mut aligned_transition_groups = Vec::new();

    for trgrp in transition_groups {
        let mut aligned_trgrp = trgrp.clone();

        let current_filename = trgrp.metadata.get("basename").unwrap();

        let mut aligned_identifying_chromatograms = HashMap::new();
        // Apply post-alignment to each transition in the group
        for (transition_id, transition_xic) in &mut aligned_trgrp.chromatograms {
            let common_rt_chrom =
                apply_common_rt_space_single(transition_xic.clone(), &common_rt_space);

            let smooth_chrom = common_rt_chrom
                .smooth_sgolay(
                    alignment_config.smoothing.sgolay_window,
                    alignment_config.smoothing.sgolay_order,
                )
                .unwrap()
                .normalize()
                .unwrap();

            let query_aligned_chrom = aligned_chromatograms
                .iter()
                .find(|chrom| {
                    chrom.chromatogram.metadata.get("basename").unwrap() == current_filename
                })
                .unwrap();

            let aligned_chrom = apply_post_alignment_to_chromatogram(
                smooth_chrom,
                query_aligned_chrom.clone(),
                &alignment_config,
            );

            aligned_identifying_chromatograms
                .insert(transition_id.clone(), aligned_chrom.chromatogram);
        }

        aligned_trgrp.chromatograms = aligned_identifying_chromatograms;
        aligned_transition_groups.push(aligned_trgrp);
    }

    aligned_transition_groups
}

/// Applies alignment to a chromatogram using an existing alignment result
///
/// # Parameters
/// - `chromatogram`: The chromatogram to be aligned
/// - `aligned_chromatogram`: The aligned chromatogram
/// - `alignment_config`: The alignment configuration
///
/// # Returns
/// - The aligned chromatogram
pub fn apply_post_alignment_to_chromatogram(
    chromatogram: Chromatogram,
    aligned_chromatogram: AlignedChromatogram,
    alignment_config: &AlignmentConfig,
) -> AlignedChromatogram {
    let aligned_chromatogram = match alignment_config.method.to_lowercase().as_str() {
        "dtw" => {
            let alignment_path = aligned_chromatogram.alignment_path.clone();

            let mut aligned_chrom = AlignedChromatogram {
                chromatogram: chromatogram.clone(),
                alignment_path: alignment_path.clone(),
                lag: None,
                rt_mapping: aligned_chromatogram.rt_mapping.clone(),
                reference_basename: aligned_chromatogram.reference_basename.clone(),
            };

            // Apply the DTW alignment to the query chromatogram
            let (query_rt, query_intensities) = (
                chromatogram.retention_times.clone(),
                chromatogram.intensities.clone(),
            );

            let refined_rt: Vec<f64> = alignment_path.iter().map(|&(_, j)| query_rt[j]).collect();

            let refined_intensities: Vec<f64> = alignment_path
                .iter()
                .map(|&(_, j)| query_intensities[j])
                .collect();

            aligned_chrom.chromatogram.retention_times = refined_rt;
            aligned_chrom.chromatogram.intensities = refined_intensities;

            aligned_chrom
        }
        "fft" => {
            let lag = aligned_chromatogram.lag.unwrap();
            let aligned_chrom = AlignedChromatogram {
                chromatogram: shift_chromatogram(&chromatogram.clone(), lag),
                alignment_path: aligned_chromatogram.alignment_path.clone(),
                lag: Some(lag),
                rt_mapping: aligned_chromatogram.rt_mapping.clone(),
                reference_basename: aligned_chromatogram.reference_basename.clone(),
            };

            aligned_chrom
        }
        "fftdtw" => {
            let alignment_path = aligned_chromatogram.alignment_path.clone();
            let lag = aligned_chromatogram.lag.unwrap();
            let mut aligned_chrom = shift_chromatogram(&chromatogram, lag);

            let (query_rt, query_intensities) = (
                aligned_chrom.retention_times.clone(),
                aligned_chrom.intensities.clone(),
            );

            // Apply the DTW alignment to the query chromatogram
            let refined_rt: Vec<f64> = alignment_path.iter().map(|&(_, j)| query_rt[j]).collect();

            let refined_intensities: Vec<f64> = alignment_path
                .iter()
                .map(|&(_, j)| query_intensities[j])
                .collect();

            aligned_chrom.retention_times = refined_rt;
            aligned_chrom.intensities = refined_intensities;

            AlignedChromatogram {
                chromatogram: aligned_chrom,
                alignment_path: alignment_path,
                lag: Some(lag),
                rt_mapping: aligned_chromatogram.rt_mapping.clone(),
                reference_basename: aligned_chromatogram.reference_basename.clone(),
            }
        }
        _ => {
            let alignment_path = aligned_chromatogram.alignment_path.clone();

            let mut aligned_chrom = AlignedChromatogram {
                chromatogram: chromatogram.clone(),
                alignment_path: alignment_path.clone(),
                lag: None,
                rt_mapping: aligned_chromatogram.rt_mapping.clone(),
                reference_basename: aligned_chromatogram.reference_basename.clone(),
            };

            // Apply the DTW alignment to the query chromatogram
            let (query_rt, query_intensities) = (
                chromatogram.retention_times.clone(),
                chromatogram.intensities.clone(),
            );

            let refined_rt: Vec<f64> = alignment_path.iter().map(|&(_, j)| query_rt[j]).collect();

            let refined_intensities: Vec<f64> = alignment_path
                .iter()
                .map(|&(_, j)| query_intensities[j])
                .collect();

            aligned_chrom.chromatogram.retention_times = refined_rt;
            aligned_chrom.chromatogram.intensities = refined_intensities;

            aligned_chrom
        }
    };
    aligned_chromatogram
}

// /// Reverses the RT mapping to convert an aligned RT back to the original RT space.
// ///
// /// # Parameters
// /// - `aligned_rt`: The RT value in the aligned space.
// /// - `aligned_chromatogram`: The aligned chromatogram containing the alignment path, lag, and RT mapping.
// /// - `alignment_config`: The alignment configuration to determine the alignment method and parameters.
// ///
// /// # Returns
// /// - The original RT value corresponding to the aligned RT.
// pub fn reverse_rt_mapping(
//     aligned_rt: f64,
//     aligned_chromatogram: &AlignedChromatogram,
//     alignment_config: &AlignmentConfig,
// ) -> Option<f64> {

//     match alignment_config.method.to_lowercase().as_str() {
//         "dtw" => {
//             log::debug!("Getting original RT for aligned RT: {} using DTW alignment", aligned_rt);

//             // Use rt_mapping to get index where target_rt is closest to 'rt1'
//             let ref_rts = aligned_chromatogram.rt_mapping.iter().map(|m| *m.get("rt1").unwrap()).collect::<Vec<f64>>();

//             let closest_index = find_closest_index(&ref_rts, aligned_rt)?;

//             // Map back to the original RT using the alignment path
//             let query_rts = aligned_chromatogram.rt_mapping.iter().map(|m| *m.get("rt2").unwrap()).collect::<Vec<f64>>();

//             Some(query_rts[closest_index])
//         }
//         "fft" => {
//             log::debug!("Getting original RT for aligned RT: {} using FFT alignment", aligned_rt);
//             // For FFT, use the lag to reverse the mapping
//             let lag = aligned_chromatogram.lag? as f64;
//             Some(aligned_rt + lag)
//         }
//         "fftdtw" => {
//             log::debug!("Getting original RT for aligned RT: {} using FFT-DTW alignment", aligned_rt);
//             // For FFT-DTW, first reverse the FFT shift, then reverse the DTW mapping
//             let lag = aligned_chromatogram.lag? as f64;
//             let shifted_rt = aligned_rt + lag;

//             // Use the alignment path to reverse the DTW mapping
//             let ref_rts = aligned_chromatogram.rt_mapping.iter().map(|m| *m.get("rt1").unwrap()).collect::<Vec<f64>>();

//             let closest_index = find_closest_index(&ref_rts, shifted_rt)?;

//             // Map back to the original RT using the alignment path
//             let query_rts = aligned_chromatogram.rt_mapping.iter().map(|m| *m.get("rt2").unwrap()).collect::<Vec<f64>>();

//             Some(query_rts[closest_index])
//         }
//         _ => {
//             // Default to DTW behavior if the method is unknown
//             log::debug!("Getting original RT for aligned RT: {} using default DTW alignment", aligned_rt);

//             // Use rt_mapping to get index where target_rt is closest to 'rt1'
//             let ref_rts = aligned_chromatogram.rt_mapping.iter().map(|m| *m.get("rt1").unwrap()).collect::<Vec<f64>>();

//             let closest_index = find_closest_index(&ref_rts, aligned_rt)?;

//             // Map back to the original RT using the alignment path
//             let query_rts = aligned_chromatogram.rt_mapping.iter().map(|m| *m.get("rt2").unwrap()).collect::<Vec<f64>>();

//             Some(query_rts[closest_index])
//         }
//     }
// }

// /// Helper function to find the index of the closest value in a vector.
// fn find_closest_index(values: &[f64], target: f64) -> Option<usize> {
//     values
//         .iter()
//         .enumerate()
//         .min_by(|(_, &a), (_, &b)| {
//             let diff_a = (a - target).abs();
//             let diff_b = (b - target).abs();
//             diff_a.partial_cmp(&diff_b).unwrap_or(Ordering::Equal)
//         })
//         .map(|(index, _)| index)
// }

/// Reverses the RT mapping to convert an aligned RT back to the original RT space.
///
/// # Parameters
/// - `aligned_rt`: The RT value in the aligned space.
/// - `aligned_chromatogram`: The aligned chromatogram containing the alignment path, lag, and RT mapping.
/// - `alignment_config`: The alignment configuration to determine the alignment method and parameters.
///
/// # Returns
/// - The original RT value corresponding to the aligned RT.
pub fn reverse_rt_mapping(
    aligned_rt: f64,
    aligned_chromatogram: &AlignedChromatogram,
    alignment_config: &AlignmentConfig,
) -> Option<f64> {
    let aligned_rt_f32 = aligned_rt as f32;

    match alignment_config.method.to_lowercase().as_str() {
        "dtw" => {
            log::debug!(
                "Getting original RT for aligned RT: {} using DTW alignment",
                aligned_rt
            );

            // Find the point with rt2 closest to aligned_rt
            let closest_index = find_closest_index_by(
                &aligned_chromatogram.rt_mapping,
                |pair| pair.rt2,
                aligned_rt_f32,
            )?;

            Some(aligned_chromatogram.rt_mapping[closest_index].rt1 as f64)
        }
        "fft" => {
            log::debug!(
                "Getting original RT for aligned RT: {} using FFT alignment",
                aligned_rt
            );
            let lag = aligned_chromatogram.lag? as f64;
            Some(aligned_rt + lag)
        }
        "fftdtw" => {
            log::debug!(
                "Getting original RT for aligned RT: {} using FFT-DTW alignment",
                aligned_rt
            );
            let closest_index = find_closest_index_by(
                &aligned_chromatogram.rt_mapping,
                |pair| pair.rt2,
                aligned_rt_f32,
            )?;

            Some(aligned_chromatogram.rt_mapping[closest_index].rt1 as f64)
        }
        _ => {
            log::debug!(
                "Getting original RT for aligned RT: {} using default DTW alignment",
                aligned_rt
            );
            let closest_index = find_closest_index_by(
                &aligned_chromatogram.rt_mapping,
                |pair| pair.rt2,
                aligned_rt_f32,
            )?;

            Some(aligned_chromatogram.rt_mapping[closest_index].rt1 as f64)
        }
    }
}

/// Helper function to find the index of the element with value closest to target
fn find_closest_index_by<T, F>(slice: &[T], extractor: F, target: f32) -> Option<usize>
where
    F: Fn(&T) -> f32,
{
    if slice.is_empty() {
        return None;
    }

    let mut closest_index = 0;
    let mut smallest_diff = (extractor(&slice[0]) - target).abs();

    for (i, item) in slice.iter().enumerate().skip(1) {
        let current_diff = (extractor(item) - target).abs();
        if current_diff < smallest_diff {
            smallest_diff = current_diff;
            closest_index = i;
        }
    }

    Some(closest_index)
}
