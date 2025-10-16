# OpenSwathWorkflow Parameter Automation - Architecture

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                         User Action                          │
│                                                               │
│  1. Sets OpenSwathWorkflow binary path in GUI                │
│  2. (Optional) Clicks "Validate" button                      │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Parameter Discovery                        │
│                                                               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  1. Check cache (~/.cache/arycal_openswath_params.json)│ │
│  │     ├─ Cache valid? → Use cached parameters            │  │
│  │     └─ Cache invalid/missing? ↓                         │  │
│  └───────────────────────────────────────────────────────┘  │
│                              │                                │
│  ┌───────────────────────────▼───────────────────────────┐  │
│  │  2. Run: OpenSwathWorkflow -write_ini osw_params.ini   │  │
│  └───────────────────────────┬───────────────────────────┘  │
│                              │                                │
│  ┌───────────────────────────▼───────────────────────────┐  │
│  │  3. Parse XML INI file                                 │  │
│  │     <?xml version="1.0"?>                              │  │
│  │     <PARAMETERS>                                        │  │
│  │       <NODE name="OpenSwathWorkflow">                  │  │
│  │         <ITEM name="readOptions" value="normal".../>   │  │
│  │         <NODE name="RTNormalization">                  │  │
│  │           <ITEM name="alignmentMethod".../>            │  │
│  │         </NODE>                                         │  │
│  │       </NODE>                                           │  │
│  │     </PARAMETERS>                                       │  │
│  └───────────────────────────┬───────────────────────────┘  │
│                              │                                │
│  ┌───────────────────────────▼───────────────────────────┐  │
│  │  4. Build ParamNode tree structure                     │  │
│  │     root                                                │  │
│  │     └── OpenSwathWorkflow                              │  │
│  │         ├── items: [readOptions, mz_correction_fn]     │  │
│  │         └── children                                    │  │
│  │             └── RTNormalization                        │  │
│  │                 └── items: [alignmentMethod, ...]      │  │
│  └───────────────────────────┬───────────────────────────┘  │
│                              │                                │
│  ┌───────────────────────────▼───────────────────────────┐  │
│  │  5. Save to cache (ParamCache struct)                  │  │
│  │     - binary_path                                       │  │
│  │     - timestamp                                         │  │
│  │     - params (ParamNode tree)                          │  │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                        UI Rendering                          │
│                                                               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  get_param_options(binary_path, "readOptions")        │  │
│  │    ├─ Load from cache                                  │  │
│  │    ├─ Find param in tree                               │  │
│  │    ├─ Extract valid_strings                            │  │
│  │    └─ Return: ["normal", "cache", "cacheWorkingIn..."] │  │
│  └───────────────────────────┬───────────────────────────┘  │
│                              │                                │
│  ┌───────────────────────────▼───────────────────────────┐  │
│  │  ComboBox::show_ui()                                   │  │
│  │    for opt in available_options:                       │  │
│  │      ui.selectable_value(cfg.param, opt.clone(), opt)  │  │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  Result: Dynamic dropdown with options from binary           │
└─────────────────────────────────────────────────────────────┘
```

## Component Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    openswath_params.rs                       │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ Data Structures                                      │    │
│  │  • ParamItem: name, value, type, description, ...   │    │
│  │  • ParamNode: name, items[], children{}             │    │
│  │  • ParamCache: binary_path, timestamp, params       │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ Core Functions                                       │    │
│  │  • parse_openms_ini(xml) → ParamNode                │    │
│  │  • fetch_openswath_params(binary) → ParamNode       │    │
│  │  • ParamNode::find_param(path) → ParamItem          │    │
│  │  • ParamCache::load_or_create(binary) → Cache       │    │
│  │  • ParamCache::refresh(binary) → Cache              │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ uses
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                  openswath_settings.rs                       │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ UI Components                                        │    │
│  │  • Binary path input + "..." browse button          │    │
│  │  • "Validate" button → refresh params                │    │
│  │  • Dynamic ComboBoxes (use get_param_options())     │    │
│  │  • Advanced params text field (fallback)            │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ Helper Functions                                     │    │
│  │  • get_param_options(binary, path) → Vec<String>    │    │
│  │    ├─ Try load from cache                            │    │
│  │    └─ Fallback to hardcoded defaults                 │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

## Fallback Strategy

```
┌─────────────────────────────────────────────────┐
│ get_param_options(binary_path, "param_name")    │
└────────────────┬────────────────────────────────┘
                 │
                 ▼
        ┌────────────────┐
        │ Cache exists?  │
        └────┬──────┬────┘
             │ YES  │ NO
             ▼      ▼
    ┌────────────┐ ┌──────────────────────┐
    │ Load cache │ │ Binary path valid?   │
    └────┬───────┘ └────┬─────────────────┘
         │              │ YES        │ NO
         │              ▼            ▼
         │     ┌─────────────────┐  │
         │     │ Try -write_ini  │  │
         │     └────┬──────┬─────┘  │
         │          │ OK   │ FAIL   │
         │          ▼      ▼        │
         │     ┌─────────┐ │        │
         │     │Parse XML│ │        │
         │     └────┬────┘ │        │
         │          │      │        │
         ▼          ▼      ▼        ▼
    ┌────────────────────────────────┐
    │   Find param in tree           │
    └────┬──────────────────┬────────┘
         │ Found            │ Not Found
         ▼                  ▼
    ┌─────────────┐    ┌─────────────────┐
    │Return values│    │Return defaults  │
    └─────────────┘    │(hardcoded list) │
                       └─────────────────┘
```

## Benefits

1. **Graceful Degradation**: Each step has a fallback
2. **Performance**: Caching avoids repeated -write_ini calls
3. **Compatibility**: Works with any OpenSwathWorkflow version
4. **User Friendly**: Transparent to users, just works
5. **Developer Friendly**: Easy to extend with more parameters
