# Implementation Summary: OpenSwathWorkflow Parameter Automation

## Issue
The arycal-gui OpenSwath settings panel had hardcoded parameters that could break when OpenSwathWorkflow's available parameters changed between versions.

## Solution
Implemented automatic parameter discovery and validation using OpenSwathWorkflow's `-write_ini` feature.

## Key Components

### 1. Parameter Discovery (`openswath_params.rs`)
- **XML Parser**: Parses OpenMS INI XML format
- **Data Structures**: 
  - `ParamNode`: Hierarchical parameter tree
  - `ParamItem`: Individual parameter metadata
- **Caching**: `ParamCache` stores discovered parameters locally
- **Functions**:
  - `parse_openms_ini()`: Parse XML into parameter tree
  - `fetch_openswath_params()`: Run -write_ini and parse result
  - `ParamCache::load_or_create()`: Load from cache or fetch fresh

### 2. UI Integration (`openswath_settings.rs`)
- **Validate Button**: Refreshes parameters from binary
- **Dynamic Dropdowns**: 
  - `mz_correction_function`
  - `readOptions`
  - `RTNormalization:alignmentMethod`
  - `RTNormalization:outlierMethod`
- **Helper**: `get_param_options()` retrieves options from cache with fallback to defaults

### 3. User Experience
- Parameters auto-discovered on binary path change
- "Validate" button to manually refresh/check compatibility
- Warnings logged when expected parameters missing
- Graceful fallback to defaults if discovery fails
- Advanced params field for any unlisted parameters

## Testing
- Unit tests for XML parsing
- Integration test with realistic OpenMS XML
- Parameter path resolution tests

## Files Changed
1. `crates/arycal-gui/src/openswath_params.rs` (new)
2. `crates/arycal-gui/src/panels/openswath_settings.rs` (modified)
3. `crates/arycal-gui/src/lib.rs` (added module)
4. `crates/arycal-gui/Cargo.toml` (added quick-xml dependency)
5. `docs/openswath_parameter_automation.md` (new documentation)

## Cache Location
Parameters cached at platform-specific locations:
- Linux: `~/.cache/arycal_openswath_params.json`
- macOS: `~/Library/Caches/arycal_openswath_params.json`
- Windows: `%LOCALAPPDATA%\arycal_openswath_params.json`

## Benefits
1. **Version Compatibility**: Works across OpenSwathWorkflow versions
2. **Future-Proof**: New parameters appear automatically
3. **Validation**: Users can verify binary compatibility
4. **Minimal Changes**: Existing UI preserved, enhanced with dynamic behavior
5. **Graceful Degradation**: Falls back to defaults if discovery fails

## Usage
1. User sets OpenSwathWorkflow binary path
2. System automatically discovers parameters (cached)
3. UI dropdowns show available options
4. Click "Validate" to refresh from binary
5. Warnings appear if parameters don't match expectations

## Developer Notes
To add new dynamic parameters:
```rust
let available_options = get_param_options(&osw_cfg.binary_path, "Param:Path")
    .unwrap_or_else(|| vec!["default1".to_string(), "default2".to_string()]);

egui::ComboBox::from_id_salt("param_id")
    .selected_text(&osw_cfg.param)
    .show_ui(ui, |ui| {
        for opt in &available_options {
            ui.selectable_value(&mut osw_cfg.param, opt.clone(), opt);
        }
    });
```
