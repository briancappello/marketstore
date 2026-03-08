use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OHLCVData {
    pub epoch: i64,
    pub open: f32,
    pub high: f32,
    pub low: f32,
    pub close: f32,
    pub volume: f32,
}

#[derive(Debug, Clone)]
pub struct DataShape {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone)]
pub enum SymbolFormat {
    Symbol = 0,
    TimeBucketKey = 1,
}

impl From<SymbolFormat> for i32 {
    fn from(format: SymbolFormat) -> Self {
        format as i32
    }
}

#[derive(Debug, Clone)]
pub struct NumpyMultiDataset {
    pub data: Option<NumpyDataset>,
    pub start_index: HashMap<String, i32>,
    pub lengths: HashMap<String, i32>,
}

#[derive(Debug, Clone)]
pub struct NumpyDataset {
    pub column_types: Vec<String>,
    pub column_names: Vec<String>,
    pub column_data: Vec<Vec<u8>>,
    pub length: i32,
}

impl Default for NumpyDataset {
    fn default() -> Self {
        Self {
            column_types: Vec::new(),
            column_names: Vec::new(),
            column_data: Vec::new(),
            length: 0,
        }
    }
}

impl Default for NumpyMultiDataset {
    fn default() -> Self {
        Self {
            data: None,
            start_index: HashMap::new(),
            lengths: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamPayload {
    pub key: String,
    pub data: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeMessage {
    pub action: String,
    pub tbks: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorMessage {
    pub error: String,
}
