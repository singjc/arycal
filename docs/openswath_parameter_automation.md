# OpenSwathWorkflow Parameter Automation

## Overview

The arycal-gui now automatically discovers and validates OpenSwathWorkflow parameters instead of hardcoding them. This ensures compatibility with different versions of OpenSwathWorkflow and allows the UI to automatically adapt when new parameters are added or existing ones change.

## How It Works

### 1. Parameter Discovery

When you set the path to the OpenSwathWorkflow binary in the GUI, arycal will:

1. Run `OpenSwathWorkflow -write_ini` to generate a complete parameter file (in XML INI format)
2. Parse the XML to extract all available parameters, their types, valid values, and descriptions
3. Cache this information locally to avoid re-running the command on every startup

### 2. Dynamic UI

The UI dropdowns for parameters like:
- `mz_correction_function`
- `readOptions`
- `RTNormalization:alignmentMethod`
- `RTNormalization:outlierMethod`

...are now populated dynamically from the discovered parameters. If your version of OpenSwathWorkflow has different or additional options, they will automatically appear in the dropdown.

### 3. Parameter Validation

Click the "🔄 Validate" button next to the binary path to:
- Refresh the parameter cache from the current OpenSwathWorkflow binary
- Check that expected parameters exist
- Log warnings if parameters are missing or changed

### 4. Fallback Behavior

If parameter discovery fails (e.g., binary not found, old version of OpenSwathWorkflow), the UI falls back to sensible default options based on common OpenMS versions.

## Using Advanced Parameters

Any OpenSwathWorkflow parameter not shown in the main UI can be added via the "Advanced Parameters" text box at the bottom of the settings panel. For example:

```
-debug 10
-Calibration:ms1_im_calibration true
```

## Cache Location

Parameter metadata is cached at:
- **Linux**: `~/.cache/arycal_openswath_params.json`
- **macOS**: `~/Library/Caches/arycal_openswath_params.json`  
- **Windows**: `%LOCALAPPDATA%\arycal_openswath_params.json`

You can delete this file to force a refresh of parameters.

## For Developers

### Adding New Dynamic Parameters

To make a new parameter dynamic:

1. Use the `get_param_options()` helper function in `openswath_settings.rs`:

```rust
let available_options = get_param_options(&osw_cfg.binary_path, "ParameterPath:paramName")
    .unwrap_or_else(|| vec![
        "default1".to_string(),
        "default2".to_string(),
    ]);
```

2. Update the ComboBox to use `available_options`:

```rust
egui::ComboBox::from_id_salt("my_param")
    .selected_text(&osw_cfg.my_param)
    .show_ui(ui, |ui| {
        for opt in &available_options {
            ui.selectable_value(
                &mut osw_cfg.my_param,
                opt.clone(),
                opt,
            );
        }
    });
```

### Parameter Paths

Parameters in OpenMS INI files are hierarchical. For example:
- Top-level: `readOptions`, `mz_correction_function`
- Nested: `RTNormalization:alignmentMethod`, `RTNormalization:outlierMethod`

Use `:` to separate levels when calling `get_param_options()` or `find_param()`.

## Benefits

1. **Version Compatibility**: Works with different OpenSwathWorkflow versions automatically
2. **Future-Proof**: New parameters added to OpenSwathWorkflow appear in the UI without code changes
3. **Validation**: Helps users identify when their OpenSwathWorkflow version differs from expectations
4. **Flexibility**: Advanced users can still use any parameter via the advanced params field
