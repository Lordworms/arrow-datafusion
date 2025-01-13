// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
use arrow::row::Row;
use arrow::row::RowConverter;
use arrow::row::RowConverter;
use arrow::row::Rows;
use bzip2::{read::BzDecoder, write::BzEncoder};
use datafusion_common::error::DataFusionError;
use datafusion_common::Result;
use datafusion_common::{exec_err, parsers::CompressionTypeVariant};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use itertools::Itertools;
use std::io::{self, Read, Write};
use std::sync::Arc;
use xz2::{read::XzDecoder, write::XzEncoder};
use zstd::stream::{Decoder as ZstdDecoder, Encoder as ZstdEncoder};
/// used for spill Rows
pub trait RowDataWriter {
    type Data;
    fn write(&mut self, rows: &Self::Data) -> Result<(), DataFusionError>;
    fn compression(&self) -> CompressionTypeVariant;
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, DataFusionError> {
        match self.compression() {
            CompressionTypeVariant::UNCOMPRESSED => Ok(data.to_vec()),
            CompressionTypeVariant::GZIP => {
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(data).map_err(DataFusionError::IoError)?;
                Ok(encoder.finish().map_err(DataFusionError::IoError)?)
            }
            CompressionTypeVariant::BZIP2 => {
                let mut encoder = BzEncoder::new(Vec::new(), bzip2::Compression::Default);
                encoder.write_all(data).map_err(DataFusionError::IoError)?;
                Ok(encoder.finish().map_err(DataFusionError::IoError)?)
            }
            CompressionTypeVariant::XZ => {
                let mut encoder = XzEncoder::new(Vec::new(), 9);
                encoder.write_all(data).map_err(DataFusionError::IoError)?;
                Ok(encoder.finish().map_err(DataFusionError::IoError)?)
            }
            CompressionTypeVariant::ZSTD => {
                let mut encoder =
                    ZstdEncoder::new(Vec::new(), 0).map_err(DataFusionError::IoError)?;
                encoder.write_all(data).map_err(DataFusionError::IoError)?;
                Ok(encoder.finish().map_err(DataFusionError::IoError)?)
            }
        }
    }
}

pub trait RowDataReader {
    type Data;
    fn read_all(&mut self) -> Result<Vec<Self::Data>, DataFusionError>;
    fn compression(&self) -> CompressionTypeVariant;
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, DataFusionError> {
        match self.compression() {
            CompressionTypeVariant::UNCOMPRESSED => Ok(data.to_vec()),
            CompressionTypeVariant::GZIP => {
                let mut decoder = GzDecoder::new(data);
                let mut decompressed = Vec::new();
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(DataFusionError::IoError)?;
                Ok(decompressed)
            }
            CompressionTypeVariant::BZIP2 => {
                let mut decoder = BzDecoder::new(data);
                let mut decompressed = Vec::new();
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(DataFusionError::IoError)?;
                Ok(decompressed)
            }
            CompressionTypeVariant::XZ => {
                let mut decoder = XzDecoder::new(data);
                let mut decompressed = Vec::new();
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(DataFusionError::IoError)?;
                Ok(decompressed)
            }
            CompressionTypeVariant::ZSTD => {
                let mut decoder =
                    ZstdDecoder::new(data).map_err(DataFusionError::IoError)?;
                let mut decompressed = Vec::new();
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(DataFusionError::IoError)?;
                Ok(decompressed)
            }
        }
    }
}

fn serilize_row(row: Row<'_>) -> Result<Vec<u8>> {
    let mut serilized_row = Vec::new();
    let len = row.data().len();
    serialize
}

pub struct CommonRowWriter {
    compression: CompressionTypeVariant,
}

impl CommonRowWriter {
    pub fn new(compression: CompressionTypeVariant) -> Self {
        Self { compression }
    }
}

impl RowDataWriter for CommonRowWriter {
    type Data = Rows;

    fn write(&mut self, rows: &Self::Data) -> Result<(), DataFusionError> {
        let mut buffer: Vec<u8> = Vec::new();
        let mut current_offset = 0;
        let mut offsets = Vec::new();
        for row in rows.into_iter() {
            offsets.push(current_offset);
            current_offset += row.data().len();
        }
        let offsets_len = offsets.len() as u32;
        buffer.extend_from_slice(&offsets_len.to_le_bytes());

        for &offset in &offsets {
            buffer.extend_from_slice(&(offset as u32).to_le_bytes());
        }

        for row in rows.into_iter() {
            buffer.extend_from_slice(row.data());
        }
        let compressed_data = self.compress(&buffer)?;
        Ok(())
    }

    fn compression(&self) -> CompressionTypeVariant {
        self.compression
    }
}

pub struct CommonRowReader {
    compression: CompressionTypeVariant,
    converter: Arc<RowConverter>,
}

impl CommonRowReader {
    pub fn new(
        compression: CompressionTypeVariant,
        converter: Arc<RowConverter>,
    ) -> Self {
        Self {
            compression,
            converter,
        }
    }
}

impl RowDataReader for CommonRowReader {
    type Data = Rows;

    fn read_all(&mut self) -> Result<Vec<Self::Data>, DataFusionError> {
        let compressed_data = vec![];
        let decompressed_data = self.decompress(&compressed_data)?;
        let mut cursor = std::io::Cursor::new(decompressed_data);

        let mut offsets_len_buf = [0u8; 4];
        cursor
            .read_exact(&mut offsets_len_buf)
            .map_err(DataFusionError::IoError)?;
        let offsets_len = u32::from_le_bytes(offsets_len_buf) as usize;
        let mut offsets = Vec::with_capacity(offsets_len);
        for _ in 0..offsets_len {
            let mut offset_buf = [0u8; 4];
            cursor
                .read_exact(&mut offset_buf)
                .map_err(DataFusionError::IoError)?;
            offsets.push(u32::from_le_bytes(offset_buf) as usize);
        }
        let mut buffer = Vec::new();
        cursor
            .read_to_end(&mut buffer)
            .map_err(DataFusionError::IoError)?;

        let parser = self.converter.parser();
        let mut rows = self.converter.empty_rows(0, 0);

        for i in 0..offsets.len() {
            let start = offsets[i];
            let end = if i + 1 < offsets.len() {
                offsets[i + 1]
            } else {
                buffer.len()
            };

            let row_data = &buffer[start..end];
            let row = parser.parse(row_data);
            rows.push(row);
        }

        Ok(vec![rows])
    }

    fn compression(&self) -> CompressionTypeVariant {
        self.compression
    }
}
