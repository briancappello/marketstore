use crate::error::{MarketStoreError, Result};
use crate::models::{DataShape, OHLCVData};

#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub destination: String,
    pub epoch_start: Option<i64>,
    pub epoch_end: Option<i64>,
    pub limit_record_count: Option<i32>,
    pub limit_from_start: bool,
    pub columns: Vec<String>,
}

impl QueryRequest {
    pub fn builder() -> QueryRequestBuilder {
        QueryRequestBuilder::default()
    }
}

#[derive(Default)]
pub struct QueryRequestBuilder {
    symbol: Option<String>,
    timeframe: Option<String>,
    attr_group: Option<String>,
    start_time: Option<i64>,
    end_time: Option<i64>,
    limit: Option<i32>,
    columns: Vec<String>,
}

impl QueryRequestBuilder {
    pub fn symbol(mut self, symbol: &str) -> Self {
        self.symbol = Some(symbol.to_string());
        self
    }

    pub fn timeframe(mut self, timeframe: &str) -> Self {
        self.timeframe = Some(timeframe.to_string());
        self
    }

    pub fn attr_group(mut self, attr_group: &str) -> Self {
        self.attr_group = Some(attr_group.to_string());
        self
    }

    pub fn start_time(mut self, start_time: i64) -> Self {
        self.start_time = Some(start_time);
        self
    }

    pub fn end_time(mut self, end_time: i64) -> Self {
        self.end_time = Some(end_time);
        self
    }

    pub fn limit(mut self, limit: i32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn columns(mut self, columns: Vec<String>) -> Self {
        self.columns = columns;
        self
    }

    pub fn build(self) -> Result<QueryRequest> {
        let symbol = self
            .symbol
            .ok_or_else(|| MarketStoreError::InvalidData("Symbol is required".to_string()))?;
        let timeframe = self
            .timeframe
            .ok_or_else(|| MarketStoreError::InvalidData("Timeframe is required".to_string()))?;
        let attr_group = self.attr_group.ok_or_else(|| {
            MarketStoreError::InvalidData("Attribute group is required".to_string())
        })?;

        Ok(QueryRequest {
            destination: format!("{}/{}/{}", symbol, timeframe, attr_group),
            epoch_start: self.start_time,
            epoch_end: self.end_time,
            limit_record_count: self.limit,
            limit_from_start: false,
            columns: self.columns,
        })
    }
}

#[derive(Debug, Clone)]
pub struct WriteRequest {
    pub symbol: String,
    pub timeframe: String,
    pub attr_group: String,
    pub data: Vec<OHLCVData>,
}

impl WriteRequest {
    pub fn new(symbol: &str, timeframe: &str, attr_group: &str, data: Vec<OHLCVData>) -> Self {
        Self {
            symbol: symbol.to_string(),
            timeframe: timeframe.to_string(),
            attr_group: attr_group.to_string(),
            data,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateRequest {
    pub symbol: String,
    pub timeframe: String,
    pub attr_group: String,
    pub data_shapes: Vec<DataShape>,
}

impl CreateRequest {
    pub fn new(
        symbol: &str,
        timeframe: &str,
        attr_group: &str,
        data_shapes: Vec<DataShape>,
    ) -> Self {
        Self {
            symbol: symbol.to_string(),
            timeframe: timeframe.to_string(),
            attr_group: attr_group.to_string(),
            data_shapes,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DestroyRequest {
    pub symbol: String,
    pub timeframe: String,
    pub attr_group: String,
}

impl DestroyRequest {
    pub fn new(symbol: &str, timeframe: &str, attr_group: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            timeframe: timeframe.to_string(),
            attr_group: attr_group.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamSubscription {
    pub tbks: Vec<String>,
}

impl StreamSubscription {
    pub fn new() -> Self {
        Self { tbks: Vec::new() }
    }

    pub fn add_stream(mut self, stream: &str) -> Self {
        self.tbks.push(stream.to_string());
        self
    }

    pub fn add_streams(mut self, streams: Vec<String>) -> Self {
        self.tbks.extend(streams);
        self
    }
}
