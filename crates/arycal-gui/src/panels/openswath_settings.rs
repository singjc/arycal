use std::{env, path::PathBuf, sync::Arc};

use arycal_cli::input::Input;
use arycal_cloudpath::util::find_executable;
use arycal_common::config::{OpenSwathConfig, RawFileType};
use egui::{ComboBox, DragValue, TextEdit, Ui, Color32};

use crate::tabs::open_swath_tab::OpenSwathState;
use crate::openswath_params::{ParamCache, ParamNode};

use super::config_panel::edit_file_paths;


pub fn draw_osw_file_settings(ui: &mut Ui, osw_cfg: &mut OpenSwathConfig) {
    // Raw LC-MSMS Files
    egui::CollapsingHeader::new("Raw mzML Files")
    .default_open(true)
    .show(ui, |ui| {
        // if the user hasn’t picked a type yet, but they have dropped in files,
        // try to auto‐detect from the first file’s extension:
        if osw_cfg.file_type.is_none() {
            if let Some(first_path) = osw_cfg.file_paths.get(0) {
                if let Some(ext) = first_path.extension().and_then(|e| e.to_str()) {
                    match ext.to_lowercase().as_str() {
                        "mzml"  => osw_cfg.file_type = Some(RawFileType::MzML),
                        "mzxml" => osw_cfg.file_type = Some(RawFileType::MzXML),
                        _         => osw_cfg.file_type = Some(RawFileType::Unknown),
                    }
                }
            }
        }

        // file type combo
        ui.horizontal(|ui| {
            ui.label("Raw File Type:");
            let current = osw_cfg.file_type
                .as_ref()
                .map(|v| match v {
                    RawFileType::MzML   => "mzML",
                    RawFileType::MzXML  => "mzXML",
                    RawFileType::Unknown  => "unknown",
                })
                .unwrap_or("None")
                .to_string();

            ComboBox::from_id_salt("raw_type")
                .selected_text(current)
                .show_ui(ui, |ui| {
                    ui.selectable_value(&mut osw_cfg.file_type, Some(RawFileType::MzML),  "mzML");
                    ui.selectable_value(&mut osw_cfg.file_type, Some(RawFileType::MzXML), "mzXML");
                });
        });

        // file paths drag & drop
        edit_file_paths(ui, &mut osw_cfg.file_paths, "MS Data", "Raw LC-MSMS Files: mzML, mzXML", Some("Select mzML Files"), Some(&vec!["mzML", "mzXML", "mzML.gz", "mzXML.gz"]));
    });

    // Raw LC-MSMS Files
    egui::CollapsingHeader::new("Peptide Query and iRT Parameter Files")
    .default_open(true)
    .show(ui, |ui| {
        // Spectral library file paths drag & drop
        edit_file_paths(ui, &mut osw_cfg.spectral_library_paths, "PQP File", "PQP Files", Some("Select PQP Files"), Some(&vec!["pqp", "tsv", "traML"]));

        // iRT linear Spectral library file paths drag & drop
        edit_file_paths(ui, &mut osw_cfg.linear_irt_library_paths, "Linear iRT", "Linear iRT PQP Files", Some("Select PQP Files"), Some(&vec!["pqp", "tsv", "traML"]));

        // Add checkbox if the user wants to include non‐linear iRT libraries
        ui.horizontal(|ui| {
            ui.checkbox(&mut osw_cfg.include_non_linear_irt, "Include Non-Linear iRT Libraries")
                .on_hover_text("Whether to include non-linear iRT libraries (e.g., endogenous peptides).");
        });

        if osw_cfg.include_non_linear_irt {
            // Non-linear iRT library file paths drag & drop
            edit_file_paths(ui, &mut osw_cfg.nonlinear_irt_library_paths, "Non-linear iRT", "Non-Linear iRT PQP Files", Some("Select PQP Files"), Some(&vec!["pqp", "tsv", "traML"]));
        }
    });

    // Add input text with file dialog button to set the output directory
    ui.horizontal(|ui| {
        ui.label("Output Directory:");
        let mut output_dir = osw_cfg.output_path.to_string_lossy().into_owned();
        if ui.add(TextEdit::singleline(&mut output_dir)).changed() {
            osw_cfg.output_path = PathBuf::from(output_dir);
        }
        if ui.button("…").on_hover_text("Browse for output directory").clicked() {
            if let Some(dir) = rfd::FileDialog::new()
                .set_title("Select Output Directory")
                .set_directory(env::current_dir().unwrap_or_default())
                .pick_folder() 
            {
                osw_cfg.output_path = dir;
            }
        }
    });
    // Add dropdown list for the output file type either as "osw" or "tsv" or "featureXML", default to "osw"
    ui.horizontal(|ui| {
        ui.label("Output File Type:");
        let current = osw_cfg.output_file_type.as_str();
        ComboBox::from_id_salt("output_file_type")
            .selected_text(current)
            .show_ui(ui, |ui| {
                ui.selectable_value(&mut osw_cfg.output_file_type, "osw".to_string(), "OSW");
                ui.selectable_value(&mut osw_cfg.output_file_type, "tsv".to_string(), "TSV");
                ui.selectable_value(&mut osw_cfg.output_file_type, "featureXML".to_string(), "FeatureXML");
            });
    });

    // Add checkbox to enable outputting debug files
    ui.horizontal(|ui| {
        ui.checkbox(&mut osw_cfg.output_debug_files, "Output Debug Files")
            .on_hover_text("Whether to output additional debug files (e.g., irt_mzml, irt_trafo, debug_mz_file, debug_im_file).");
    });
}

/// Draw additional settings for OpenSwath tab
pub fn draw_open_swath(ui: &mut Ui, config: &mut Input, state: &mut OpenSwathState,) {
    ui.heading("OpenSwath Workflow Settings");

    let mut osw_cfg: &mut OpenSwathConfig =
        config.openswath.get_or_insert_with(OpenSwathConfig::default);

    // — binary path + auto-detect button —
    if osw_cfg.binary_path.as_os_str().is_empty() {
        if let Some(p) = find_executable("OpenSwathWorkflow", None) {
            osw_cfg.binary_path = p;
        }
    }

    ui.horizontal(|ui| {
        ui.add(egui::Label::new("OpenSwathWorkflow binary:")).on_hover_text(
            "Path to the OpenSwathWorkflow binary. If not set, it will try to find it in your PATH."
        );

        // Show & edit it as a String
        let mut path_str = osw_cfg.binary_path.to_string_lossy().into_owned();
        if ui.add(TextEdit::singleline(&mut path_str)).changed() {
            osw_cfg.binary_path = PathBuf::from(path_str.clone());
        }

        // File dialog button
        if ui.button("…").on_hover_text("Browse for binary").clicked() {
            if let Some(file) = rfd::FileDialog::new()
                .set_title("Select OpenSwathWorkflow binary")
                // start in current dir or last used
                .set_directory(env::current_dir().unwrap_or_default())
                .pick_file() 
            {
                osw_cfg.binary_path = file;
            }
        }

        // Add "Validate Parameters" button
        if !osw_cfg.binary_path.as_os_str().is_empty() && osw_cfg.binary_path.exists() {
            if ui.button("🔄 Validate").on_hover_text("Validate parameters with OpenSwathWorkflow binary").clicked() {
                match ParamCache::refresh(&osw_cfg.binary_path) {
                    Ok(cache) => {
                        log::info!("Successfully validated parameters from OpenSwathWorkflow");
                        
                        // Check if key parameters exist
                        if let Some(osw_node) = cache.params.children.get("OpenSwathWorkflow") {
                            let mut warnings = Vec::new();
                            
                            // Check some critical parameters
                            if osw_node.find_param("RTNormalization:alignmentMethod").is_none() {
                                warnings.push("RTNormalization:alignmentMethod");
                            }
                            if osw_node.find_param("mz_correction_function").is_none() {
                                warnings.push("mz_correction_function");
                            }
                            
                            if warnings.is_empty() {
                                log::info!("All expected parameters found in OpenSwathWorkflow");
                            } else {
                                log::warn!("Some expected parameters not found: {:?}", warnings);
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to validate parameters: {}", e);
                    }
                }
            }
        }
    });

    ui.separator();

    // — simple flags —
    ui.horizontal(|ui| {
        ui.checkbox(&mut osw_cfg.enable_ms1, "Enable MS1");
        ui.checkbox(&mut osw_cfg.enable_ipf, "Enable IPF");
        ui.checkbox(&mut osw_cfg.is_pasef, "PASEF data").on_hover_text("Check if the data is PASEF (ion mobility) data. \
            This will enable ion mobility extraction.");
    });

    ui.separator();

    // — extraction windows —
    ui.label("Extraction Windows:");
    ui.horizontal(|ui| {
        ui.label("RT (s):");
        ui.add(DragValue::new(&mut osw_cfg.rt_extraction_window).speed(1.0));
        if osw_cfg.is_pasef {
            ui.label("Ion mobility:");
            ui.add(DragValue::new(&mut osw_cfg.ion_mobility_window).speed(1.0));
        }
    });
    ui.horizontal(|ui| {
        ui.label("MS2 m/z:");
        ui.add(DragValue::new(&mut osw_cfg.ms2_mz_extraction_window).speed(1.0));
        ui.label(&osw_cfg.ms2_mz_extraction_window_unit);

        ui.label("MS1 m/z:");
        ui.add(DragValue::new(&mut osw_cfg.ms1_mz_extraction_window).speed(1.0));
        ui.label(&osw_cfg.ms1_mz_extraction_window_unit);
        if osw_cfg.is_pasef {
            ui.label("MS1 ion mobility:");
            ui.add(DragValue::new(&mut osw_cfg.ms1_ion_mobility_extraction_window).speed(1.0));
        }
    });
    ui.horizontal(|ui| {
        ui.label("iRT m/z:");
        ui.add(DragValue::new(&mut osw_cfg.irt_mz_extraction_window).speed(1.0));
        ui.label(&osw_cfg.irt_mz_extraction_window_unit);

        if osw_cfg.is_pasef {
            ui.label("iRT ion mobility:");
            ui.add(DragValue::new(&mut osw_cfg.irt_ion_mobility_extraction_window).speed(1.0));
        }
    });

    ui.separator();

    // — other numeric / text parameters —
    ui.horizontal(|ui| {
        // Label with hover‐hint
        ui.add(
            egui::Label::new("Mz correction fn:")
        ).on_hover_text(
            "Use the retention time normalization peptide MS2 masses to \
             perform a mass correction (linear, weighted by intensity \
             linear or quadratic) of all spectra."
        );
    
        // Dropdown (combo box) for selecting the correction function
        egui::ComboBox::from_id_salt("mz_correction_fn")
            .selected_text(&osw_cfg.mz_correction_function)
            .show_ui(ui, |ui| {
                for &opt in &[
                    "none",
                    "regression_delta_ppm",
                    "unweighted_regression",
                    "weighted_regression",
                    "quadratic_regression",
                    "weighted_quadratic_regression",
                    "weighted_quadratic_regression_delta_ppm",
                    "quadratic_regression_delta_ppm",
                ] {
                    ui.selectable_value(
                        &mut osw_cfg.mz_correction_function,
                        opt.to_string(),
                        opt,
                    );
                }
            });
    });
    ui.horizontal(|ui| {
        // Read options dropdown with hover‐hint
        ui.add(
            egui::Label::new("Read options:")
        ).on_hover_text(
            "Whether to run OpenSWATH directly on the input data, cache data to disk \
             first or to perform a datareduction step first. If you choose cache, \
             make sure to also set tempDirectory",
        );
        egui::ComboBox::from_id_salt("read_options")
            .selected_text(&osw_cfg.read_options)
            .show_ui(ui, |ui| {
                for &opt in &[
                    "normal",
                    "cache",
                    "cacheWorkingInMemory",
                    "workingInMemory",
                ] {
                    ui.selectable_value(
                        &mut osw_cfg.read_options,
                        opt.to_string(),
                        opt,
                    );
                }
            });
    });
    ui.horizontal(|ui| {
        // Temp directory chooser
        ui.add(
            egui::Label::new("Temp Directory:")
        ).on_hover_text("Directory to cache intermediate files for ‘cache’ read option");
    
        // Editable text box
        let mut temp_dir_str = osw_cfg
            .temp_directory
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default();
        if ui.add(TextEdit::singleline(&mut temp_dir_str)).changed() {
            osw_cfg.temp_directory = if temp_dir_str.is_empty() {
                None
            } else {
                Some(PathBuf::from(temp_dir_str.clone()))
            };
        }
    
        // Folder-pick button
        if ui.button("…").on_hover_text("Browse for temp directory").clicked() {
            if let Some(dir) = rfd::FileDialog::new()
                .set_title("Select Temp Directory")
                .set_directory(env::current_dir().unwrap_or_default())
                .pick_folder()
            {
                osw_cfg.temp_directory = Some(dir);
            }
        }
    });
    ui.horizontal(|ui| {
        ui.add(egui::Label::new("Batch Size:")).on_hover_text("The batch size of chromatograms to process (0 means to only have one batch, sensible values are around 250-1000)");
        ui.add(DragValue::new(&mut osw_cfg.batch_size).speed(1.0));
        ui.label("Threads:");
        ui.add(DragValue::new(&mut osw_cfg.threads).speed(1.0));
        ui.label("Outer-loop threads:");
        ui.add(DragValue::new(&mut osw_cfg.outer_loop_threads).speed(1.0)).on_hover_text("How many threads should be used for the outer loop (-1 use all threads, use 4 to analyze 4 SWATH windows in memory at once).");
    });

    ui.separator();

    // — advanced parameters textbox —
    
    // RT normalization alignment method
    ui.horizontal(|ui| {
        ui.add(
            egui::Label::new("RT norm alignment:")
        ).on_hover_text(
            "How to perform the alignment to the normalized RT space using anchor points. \
            ‘linear’: perform linear regression (for few anchor points). \
            ‘interpolated’: interpolate between anchor points (for few, noise-free anchor points). \
            ‘lowess’: use local regression (for many, noisy anchor points). \
            ‘b_spline’: use B-splines for smoothing."
        );
        egui::ComboBox::from_id_salt("rt_norm_alignment_method")
            .selected_text(&osw_cfg.rt_normalization_alignment_method)
            .show_ui(ui, |ui| {
                for &opt in &["linear", "interpolated", "lowess", "b_spline"] {
                    ui.selectable_value(
                        &mut osw_cfg.rt_normalization_alignment_method,
                        opt.to_string(),
                        opt,
                    );
                }
            });
    });

    // RT normalization outlier method
    ui.horizontal(|ui| {
        ui.add(
            egui::Label::new("RT norm outlier:")
        ).on_hover_text(
            "Which outlier detection method to use (valid: ‘iter_residual’, \
            ‘iter_jackknife’, ‘ransac’, ‘none’). Iterative methods remove one \
            outlier at a time. Jackknife optimizes for max r-squared improvement, \
            while ‘iter_residual’ removes the datapoint with largest residual \
            error (cheaper for many peptides)."
        );
        egui::ComboBox::from_id_salt("rt_norm_outlier_method")
            .selected_text(&osw_cfg.rt_normalization_outlier_method)
            .show_ui(ui, |ui| {
                for &opt in &["iter_residual", "iter_jackknife", "ransac", "none"] {
                    ui.selectable_value(
                        &mut osw_cfg.rt_normalization_outlier_method,
                        opt.to_string(),
                        opt,
                    );
                }
            });
    });

    // Estimate best peptides checkbox
    ui.horizontal(|ui| {
        ui.add(
            egui::Checkbox::new(
                &mut osw_cfg.rt_normalization_estimate_best_peptides,
                "Estimate best peptides"
            )
        ).on_hover_text(
            "Whether the algorithm should choose the best peptides based on peak shape \
            for normalization. Use when not all peptides are detected or when many \
            'bad' peptides (e.g., endogenous) might enter outlier removal."
        );
    });

    // RT normalization LOWESS span
    ui.horizontal(|ui| {
        ui.add(
            egui::Label::new("LOWESS span:")
        ).on_hover_text(
            "The smoothing parameter for LOWESS alignment (fraction of points used)."
        );
        ui.add(
            egui::DragValue::new(&mut osw_cfg.rt_normalization_lowess_span)
                .speed(0.01)
        );
    });

    ui.label("Advanced Parameters:");
    ui.add(
        TextEdit::multiline(&mut osw_cfg.advanced_params)
            .desired_rows(4)
            .lock_focus(true)
            .hint_text("e.g. -debug 10"),
    );

    // Add info about parameter validation
    if !osw_cfg.binary_path.as_os_str().is_empty() && osw_cfg.binary_path.exists() {
        ui.separator();
        ui.horizontal(|ui| {
            ui.label("ℹ");
            ui.label("Use the 'Validate' button above to check parameter compatibility with your OpenSwathWorkflow version.");
        });
        ui.label("Advanced parameters can be used to specify any additional OpenSwathWorkflow flags not shown in this UI.");
    }
}

/// Helper function to get parameter options from cached params
fn get_param_options(binary_path: &PathBuf, param_path: &str) -> Option<Vec<String>> {
    if let Ok(cache) = ParamCache::load_or_create(binary_path) {
        if let Some(osw_node) = cache.params.children.get("OpenSwathWorkflow") {
            if let Some(param) = osw_node.find_param(param_path) {
                if !param.valid_strings.is_empty() {
                    return Some(param.valid_strings.clone());
                }
            }
        }
    }
    None
}