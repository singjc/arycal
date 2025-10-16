use anyhow::{Context, Result};
use quick_xml::events::Event;
use quick_xml::Reader;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Represents a parameter item in the OpenMS INI XML
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParamItem {
    /// The name/key of the parameter
    pub name: String,
    /// The parameter value
    pub value: String,
    /// The parameter type (e.g., "string", "int", "double", "bool")
    pub param_type: String,
    /// Description/documentation for the parameter
    pub description: String,
    /// Valid range for numeric types (min, max)
    pub restrictions: Option<String>,
    /// Valid choices for string types
    pub valid_strings: Vec<String>,
    /// Tags associated with the parameter (e.g., "advanced", "required")
    pub tags: Vec<String>,
}

/// Represents a node (section) in the parameter tree
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParamNode {
    /// The name of this section/node
    pub name: String,
    /// Description of this node/section
    pub description: String,
    /// Parameters directly in this node
    pub items: Vec<ParamItem>,
    /// Sub-nodes/sections
    pub children: HashMap<String, ParamNode>,
}

impl ParamNode {
    pub fn new(name: String, description: String) -> Self {
        ParamNode {
            name,
            description,
            items: Vec::new(),
            children: HashMap::new(),
        }
    }

    /// Find a parameter by its full path (e.g., "RTNormalization:alignmentMethod")
    pub fn find_param(&self, path: &str) -> Option<&ParamItem> {
        let parts: Vec<&str> = path.split(':').collect();
        self.find_param_recursive(&parts)
    }

    fn find_param_recursive(&self, parts: &[&str]) -> Option<&ParamItem> {
        if parts.is_empty() {
            return None;
        }

        if parts.len() == 1 {
            // Last part - look in items
            return self.items.iter().find(|item| item.name == parts[0]);
        }

        // Recurse into child node
        if let Some(child) = self.children.get(parts[0]) {
            return child.find_param_recursive(&parts[1..]);
        }

        None
    }

    /// Get all parameter paths in this tree
    pub fn get_all_paths(&self) -> Vec<String> {
        let mut paths = Vec::new();
        self.collect_paths("", &mut paths);
        paths
    }

    fn collect_paths(&self, prefix: &str, paths: &mut Vec<String>) {
        for item in &self.items {
            if prefix.is_empty() {
                paths.push(item.name.clone());
            } else {
                paths.push(format!("{}:{}", prefix, item.name));
            }
        }
        for (name, child) in &self.children {
            let new_prefix = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{}:{}", prefix, name)
            };
            child.collect_paths(&new_prefix, paths);
        }
    }
}

/// Parse OpenMS INI XML into a parameter tree
pub fn parse_openms_ini(xml_content: &str) -> Result<ParamNode> {
    let mut reader = Reader::from_str(xml_content);
    reader.config_mut().trim_text(true);

    let mut root = ParamNode::new("root".to_string(), "OpenSwathWorkflow parameters".to_string());
    let mut node_stack = vec![&mut root as *mut ParamNode];
    let mut current_item: Option<ParamItem> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                match e.name().as_ref() {
                    b"NODE" => {
                        let mut node_name = String::new();
                        let mut node_desc = String::new();
                        
                        for attr in e.attributes() {
                            if let Ok(attr) = attr {
                                match attr.key.as_ref() {
                                    b"name" => {
                                        node_name = String::from_utf8_lossy(&attr.value).to_string();
                                    }
                                    b"description" => {
                                        node_desc = String::from_utf8_lossy(&attr.value).to_string();
                                    }
                                    _ => {}
                                }
                            }
                        }
                        
                        // Create new node
                        let new_node = ParamNode::new(node_name.clone(), node_desc);
                        
                        // Add it to current parent
                        unsafe {
                            if let Some(&parent_ptr) = node_stack.last() {
                                let parent = &mut *parent_ptr;
                                parent.children.insert(node_name.clone(), new_node);
                                
                                // Push pointer to the newly inserted node
                                let child_ptr = parent.children.get_mut(&node_name).unwrap() as *mut ParamNode;
                                node_stack.push(child_ptr);
                            }
                        }
                    }
                    b"ITEM" => {
                        let mut item = ParamItem {
                            name: String::new(),
                            value: String::new(),
                            param_type: String::new(),
                            description: String::new(),
                            restrictions: None,
                            valid_strings: Vec::new(),
                            tags: Vec::new(),
                        };
                        
                        for attr in e.attributes() {
                            if let Ok(attr) = attr {
                                match attr.key.as_ref() {
                                    b"name" => {
                                        item.name = String::from_utf8_lossy(&attr.value).to_string();
                                    }
                                    b"value" => {
                                        item.value = String::from_utf8_lossy(&attr.value).to_string();
                                    }
                                    b"type" => {
                                        item.param_type = String::from_utf8_lossy(&attr.value).to_string();
                                    }
                                    b"description" => {
                                        item.description = String::from_utf8_lossy(&attr.value).to_string();
                                    }
                                    b"restrictions" => {
                                        item.restrictions = Some(String::from_utf8_lossy(&attr.value).to_string());
                                    }
                                    b"tags" => {
                                        let tags_str = String::from_utf8_lossy(&attr.value);
                                        item.tags = tags_str.split(',').map(|s| s.trim().to_string()).collect();
                                    }
                                    _ => {}
                                }
                            }
                        }
                        current_item = Some(item);
                    }
                    b"ITEMLIST" => {
                        // Handle valid string lists for ITEM
                    }
                    b"LISTITEM" => {
                        if let Some(ref mut item) = current_item {
                            for attr in e.attributes() {
                                if let Ok(attr) = attr {
                                    if attr.key.as_ref() == b"value" {
                                        item.valid_strings.push(
                                            String::from_utf8_lossy(&attr.value).to_string()
                                        );
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                match e.name().as_ref() {
                    b"NODE" => {
                        node_stack.pop();
                    }
                    b"ITEM" => {
                        if let Some(item) = current_item.take() {
                            unsafe {
                                if let Some(&parent_ptr) = node_stack.last() {
                                    let parent = &mut *parent_ptr;
                                    parent.items.push(item);
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(anyhow::anyhow!("Error parsing XML at position {}: {:?}", reader.buffer_position(), e));
            }
            _ => {}
        }
    }

    Ok(root)
}

/// Run OpenSwathWorkflow -write_ini and parse the result
pub fn fetch_openswath_params(binary_path: &Path) -> Result<ParamNode> {
    // Create a temporary file for the INI output
    let temp_dir = std::env::temp_dir();
    let ini_path = temp_dir.join("openswath_params.ini");

    // Run OpenSwathWorkflow -write_ini
    let output = Command::new(binary_path)
        .arg("-write_ini")
        .arg(&ini_path)
        .output()
        .context("Failed to execute OpenSwathWorkflow -write_ini")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!(
            "OpenSwathWorkflow -write_ini failed: {}",
            stderr
        ));
    }

    // Read the INI file
    let xml_content = std::fs::read_to_string(&ini_path)
        .context("Failed to read generated INI file")?;

    // Clean up
    let _ = std::fs::remove_file(&ini_path);

    // Parse the XML
    parse_openms_ini(&xml_content)
}

/// Cache for parameter metadata
#[derive(Debug, Serialize, Deserialize)]
pub struct ParamCache {
    /// Path to the binary used to generate this cache
    pub binary_path: PathBuf,
    /// Timestamp when cache was created
    pub timestamp: u64,
    /// The parameter tree
    pub params: ParamNode,
}

impl ParamCache {
    /// Load or create parameter cache
    pub fn load_or_create(binary_path: &Path) -> Result<Self> {
        let cache_dir = dirs::cache_dir()
            .or_else(|| std::env::temp_dir().into())
            .unwrap_or_else(|| PathBuf::from("."));
        
        let cache_path = cache_dir.join("arycal_openswath_params.json");

        // Try to load existing cache
        if cache_path.exists() {
            if let Ok(content) = std::fs::read_to_string(&cache_path) {
                if let Ok(cache) = serde_json::from_str::<ParamCache>(&content) {
                    // Check if cache is still valid (same binary path)
                    if cache.binary_path == binary_path {
                        log::info!("Loaded OpenSwathWorkflow parameters from cache");
                        return Ok(cache);
                    }
                }
            }
        }

        // Cache doesn't exist or is invalid - fetch fresh
        log::info!("Fetching OpenSwathWorkflow parameters from binary");
        let params = fetch_openswath_params(binary_path)?;
        
        let cache = ParamCache {
            binary_path: binary_path.to_path_buf(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            params,
        };

        // Save cache
        if let Ok(json) = serde_json::to_string_pretty(&cache) {
            let _ = std::fs::write(&cache_path, json);
        }

        Ok(cache)
    }

    /// Force refresh the cache
    pub fn refresh(binary_path: &Path) -> Result<Self> {
        let cache_dir = dirs::cache_dir()
            .or_else(|| std::env::temp_dir().into())
            .unwrap_or_else(|| PathBuf::from("."));
        
        let cache_path = cache_dir.join("arycal_openswath_params.json");

        // Fetch fresh
        log::info!("Refreshing OpenSwathWorkflow parameters from binary");
        let params = fetch_openswath_params(binary_path)?;
        
        let cache = ParamCache {
            binary_path: binary_path.to_path_buf(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            params,
        };

        // Save cache
        if let Ok(json) = serde_json::to_string_pretty(&cache) {
            let _ = std::fs::write(&cache_path, json);
        }

        Ok(cache)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_ini() {
        let xml = r#"<?xml version="1.0" encoding="ISO-8859-1"?>
<PARAMETERS>
  <NODE name="OpenSwathWorkflow" description="Complete workflow for OpenSWATH">
    <ITEM name="in" value="" type="input-file" description="Input file" />
    <ITEM name="tr" value="" type="input-file" description="Transition file" />
    <NODE name="RTNormalization" description="RT normalization parameters">
      <ITEM name="alignmentMethod" value="lowess" type="string" description="Alignment method" />
      <ITEMLIST name="validMethods">
        <LISTITEM value="linear"/>
        <LISTITEM value="lowess"/>
        <LISTITEM value="interpolated"/>
      </ITEMLIST>
    </NODE>
  </NODE>
</PARAMETERS>"#;

        let result = parse_openms_ini(xml);
        assert!(result.is_ok());
        
        let root = result.unwrap();
        assert_eq!(root.children.len(), 1);
        
        let osw_node = root.children.get("OpenSwathWorkflow").unwrap();
        assert_eq!(osw_node.items.len(), 2);
        assert_eq!(osw_node.children.len(), 1);
        
        let rt_node = osw_node.children.get("RTNormalization").unwrap();
        assert_eq!(rt_node.items.len(), 1);
        
        let align_param = rt_node.items.first().unwrap();
        assert_eq!(align_param.name, "alignmentMethod");
        assert_eq!(align_param.value, "lowess");
    }

    #[test]
    fn test_find_param() {
        let xml = r#"<?xml version="1.0" encoding="ISO-8859-1"?>
<PARAMETERS>
  <NODE name="OpenSwathWorkflow" description="Complete workflow">
    <NODE name="RTNormalization" description="RT norm">
      <ITEM name="alignmentMethod" value="lowess" type="string" description="Method" />
    </NODE>
  </NODE>
</PARAMETERS>"#;

        let root = parse_openms_ini(xml).unwrap();
        let osw_node = root.children.get("OpenSwathWorkflow").unwrap();
        
        let param = osw_node.find_param("RTNormalization:alignmentMethod");
        assert!(param.is_some());
        assert_eq!(param.unwrap().value, "lowess");
    }

    #[test]
    fn test_xml_parsing_with_realistic_structure() {
        let xml = r#"<?xml version="1.0" encoding="ISO-8859-1"?>
<PARAMETERS version="1.7.0" xsi:noNamespaceSchemaLocation="https://raw.githubusercontent.com/OpenMS/OpenMS/develop/share/OpenMS/SCHEMAS/Param_1_7_0.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <NODE name="OpenSwathWorkflow" description="Complete workflow for OpenSWATH">
    <ITEM name="in" value="" type="input-file" description="Input files separated by blank" required="true" advanced="false" tags="input file,required" />
    <ITEM name="tr" value="" type="input-file" description="Transition file" required="false" advanced="false" />
    <ITEM name="readOptions" value="normal" type="string" description="Read options for all input files" required="false" advanced="false" restrictions="normal,cache,cacheWorkingInMemory,workingInMemory" />
    <ITEM name="mz_extraction_window" value="0.05" type="double" description="Extraction window in m/z dimension (in ppm or Da)." required="false" advanced="false" />
    <NODE name="RTNormalization" description="Parameters for RT normalization">
      <ITEM name="alignmentMethod" value="linear" type="string" description="How to perform the alignment" required="false" advanced="false" />
      <ITEMLIST name="alignmentMethod_options" type="string">
        <LISTITEM value="linear"/>
        <LISTITEM value="interpolated"/>
        <LISTITEM value="lowess"/>
        <LISTITEM value="b_spline"/>
      </ITEMLIST>
      <ITEM name="outlierMethod" value="iter_residual" type="string" description="Which outlier detection method to use" required="false" advanced="false" />
    </NODE>
  </NODE>
</PARAMETERS>"#;

        let result = parse_openms_ini(xml);
        assert!(result.is_ok(), "Failed to parse XML: {:?}", result.err());
        
        let root = result.unwrap();
        
        // Check structure
        assert!(root.children.contains_key("OpenSwathWorkflow"));
        let osw = root.children.get("OpenSwathWorkflow").unwrap();
        
        // Check top-level items
        let in_param = osw.items.iter().find(|p| p.name == "in");
        assert!(in_param.is_some());
        assert_eq!(in_param.unwrap().param_type, "input-file");
        
        // Check readOptions
        let read_opts = osw.items.iter().find(|p| p.name == "readOptions");
        assert!(read_opts.is_some());
        assert_eq!(read_opts.unwrap().value, "normal");
        
        // Check nested node
        assert!(osw.children.contains_key("RTNormalization"));
        
        // Check nested parameter using path
        let align_method = osw.find_param("RTNormalization:alignmentMethod");
        assert!(align_method.is_some());
        assert_eq!(align_method.unwrap().value, "linear");
    }
}
