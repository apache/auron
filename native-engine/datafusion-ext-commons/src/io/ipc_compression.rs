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

use std::io::{BufRead, BufReader, Read, Take, Write};

use arrow::{array::ArrayRef, datatypes::SchemaRef};
use auron_jni_bridge::{
    conf,
    conf::{IntConf, StringConf},
    is_jni_bridge_inited,
};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use datafusion::common::Result;
use once_cell::sync::OnceCell;
use zstd::stream::{raw, raw::Operation, zio};

use crate::{
    df_execution_err,
    io::{read_one_batch, write_one_batch},
};

/// Block-level compressor used by [`IpcCompressionWriter`]. Unlike
/// [`IoCompressionWriter`] (which is used for whole-stream compression, e.g.
/// spilling), this type is specifically designed so that the underlying
/// codec context (lz4's compression hash table / zstd's `CCtx`) can be
/// reused across consecutive blocks instead of being recreated from scratch
/// every time a block is finished.
enum BlockCompressor {
    LZ4(lz4_flex::frame::FrameEncoder<VecBufferWrite>),
    // wrapped in Option so finish_current_buf() can take the inner zio::Writer
    // out, reinit the underlying raw::Encoder (CCtx), and put a freshly
    // constructed zio::Writer (pointing at the next block's buffer) back in.
    ZSTD(Option<zio::Writer<VecBufferWrite, raw::Encoder<'static>>>),
}

impl BlockCompressor {
    fn new(shared_buf: &mut VecBuffer) -> Result<Self> {
        match io_compression_codec() {
            "lz4" => Ok(Self::LZ4(lz4_flex::frame::FrameEncoder::new(
                shared_buf.writer(),
            ))),
            "zstd" => {
                let level = conf::SPARK_IO_COMPRESSION_ZSTD_LEVEL.value().unwrap_or(1);
                let raw_encoder = raw::Encoder::new(level)?;
                Ok(Self::ZSTD(Some(zio::Writer::new(
                    shared_buf.writer(),
                    raw_encoder,
                ))))
            }
            other => df_execution_err!("unsupported codec: {}", other),
        }
    }
}

impl Write for BlockCompressor {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            BlockCompressor::LZ4(w) => w.write(buf),
            BlockCompressor::ZSTD(w) => w.as_mut().expect("zstd block writer missing").write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            BlockCompressor::LZ4(w) => w.flush(),
            BlockCompressor::ZSTD(w) => w.as_mut().expect("zstd block writer missing").flush(),
        }
    }
}

pub struct IpcCompressionWriter<W: Write> {
    output: W,
    shared_buf: VecBuffer,
    block_writer: BlockCompressor,
    block_empty: bool,
}
unsafe impl<W: Write> Send for IpcCompressionWriter<W> {}

impl<W: Write> IpcCompressionWriter<W> {
    pub fn new(output: W) -> Self {
        let mut shared_buf = VecBuffer::default();
        shared_buf.inner_mut().extend_from_slice(&[0u8; 4]);

        let block_writer =
            BlockCompressor::new(&mut shared_buf).expect("error creating compression encoder");
        Self {
            output,
            shared_buf,
            block_writer,
            block_empty: true,
        }
    }

    pub fn set_output(&mut self, output: W) {
        assert!(
            self.block_empty,
            "IpcCompressionWriter must be empty while changing output"
        );
        self.output = output;
    }

    pub fn write_batch(&mut self, num_rows: usize, cols: &[ArrayRef]) -> Result<()> {
        if num_rows == 0 {
            return Ok(());
        }
        write_one_batch(num_rows, cols, &mut self.block_writer)?;
        self.block_empty = false;

        let buf_len = self.shared_buf.inner().len();
        if buf_len as f64
            >= conf::SHUFFLE_COMPRESSION_TARGET_BUF_SIZE
                .value()
                .unwrap_or(4194304) as f64 // default to 4MB
                * 0.9
        {
            self.finish_current_buf()?;
        }
        Ok(())
    }

    pub fn finish_current_buf(&mut self) -> Result<()> {
        if !self.block_empty {
            // finish current block, reusing the underlying codec context where possible
            let mut pending_zstd_encoder = None;
            match &mut self.block_writer {
                BlockCompressor::LZ4(w) => {
                    // try_finish() only closes the current lz4 frame, it does not consume
                    // the encoder, so the compression hash table can be reused as-is for
                    // the next frame.
                    w.try_finish()
                        .or_else(|_| df_execution_err!("ipc compression error"))?;
                }
                BlockCompressor::ZSTD(slot) => {
                    let mut w = slot.take().expect("zstd block writer missing");
                    w.finish()?;
                    let (_old_output, mut raw_encoder) = w.into_inner();
                    // reinit() is a cheap SessionOnly reset that prepares the CCtx for a
                    // new, independent frame without reallocating it.
                    raw_encoder.reinit()?;
                    pending_zstd_encoder = Some(raw_encoder);
                }
            }

            // write
            let block_len = self.shared_buf.inner().len() - 4;
            self.shared_buf.inner_mut()[0..4]
                .as_mut()
                .write_u32::<LittleEndian>(block_len as u32)?;
            self.output.write_all(self.shared_buf.inner())?;

            // open next buf
            self.shared_buf.inner_mut().clear();
            self.shared_buf.inner_mut().extend_from_slice(&[0u8; 4]);

            // lz4's FrameEncoder keeps writing to the same (now-cleared) shared buffer and
            // will automatically start a new frame on next write(); zstd needs a fresh
            // zio::Writer wrapping the reused raw_encoder and the new buffer.
            if let Some(raw_encoder) = pending_zstd_encoder {
                if let BlockCompressor::ZSTD(slot) = &mut self.block_writer {
                    *slot = Some(zio::Writer::new(self.shared_buf.writer(), raw_encoder));
                }
            }
            self.block_empty = true;
        }
        Ok(())
    }

    pub fn inner(&self) -> &W {
        &self.output
    }

    pub fn inner_mut(&mut self) -> &mut W {
        &mut self.output
    }
}

/// Block-level decompressor used by [`IpcCompressionReader`]. Mirrors
/// [`BlockCompressor`] on the write side: unlike [`IoCompressionReader`]
/// (used for whole-stream decompression, e.g. spilling), this type is
/// designed so that the underlying codec context (lz4's decode buffers /
/// zstd's `DCtx`) can be reused across consecutive blocks instead of being
/// recreated from scratch every time a block is finished.
enum BlockDecompressor<R: Read> {
    LZ4(lz4_flex::frame::FrameDecoder<Take<R>>),
    ZSTD(ZstdBlockReader<R>),
}

impl<R: Read> BlockDecompressor<R> {
    fn new(take: Take<R>) -> Result<Self> {
        match io_compression_codec() {
            "lz4" => Ok(Self::LZ4(lz4_flex::frame::FrameDecoder::new(take))),
            "zstd" => Ok(Self::ZSTD(ZstdBlockReader::new(take)?)),
            other => df_execution_err!("unsupported codec: {}", other),
        }
    }

    /// Prepares the decompressor to read a new block of `block_len` bytes
    /// from the same underlying reader, reusing the already-allocated codec
    /// context where possible.
    fn next_block(&mut self, block_len: u64) {
        match self {
            // lz4's FrameDecoder automatically parses a fresh frame header on the
            // next read() once the previous frame's EndMark has been consumed, and
            // reuses its internal src/dst buffers as-is, so only the `Take` limit
            // needs to be reset here.
            Self::LZ4(d) => d.get_mut().set_limit(block_len),
            Self::ZSTD(d) => d.next_block(block_len),
        }
    }

    /// Returns a mutable reference to the raw underlying reader, bypassing the
    /// current block's `Take` limit; used to read the next block's length
    /// prefix once the current block has been fully consumed.
    fn inner_reader_mut(&mut self) -> &mut R {
        match self {
            Self::LZ4(d) => d.get_mut().get_mut(),
            Self::ZSTD(d) => d.inner_reader_mut(),
        }
    }
}

impl<R: Read> Read for BlockDecompressor<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::LZ4(d) => d.read(buf),
            Self::ZSTD(d) => d.read(buf),
        }
    }
}

enum ZstdReadState {
    Reading,
    PastEof,
    Finished,
}

/// Minimal reimplementation of zstd's `zio::Reader`, specialized for reading
/// a single block bounded by a `Take<R>`. Unlike `zio::Reader` (whose
/// `into_inner()` only returns the wrapped reader and silently drops the
/// decode operation/context), this keeps the `raw::Decoder` (and therefore
/// its `DCtx`) as a directly-accessible field, so [`BlockDecompressor`] can
/// reuse it across block boundaries via [`ZstdBlockReader::next_block`]
/// instead of re-allocating a new `DCtx` for every block.
struct ZstdBlockReader<R: Read> {
    reader: BufReader<Take<R>>,
    decoder: raw::Decoder<'static>,
    state: ZstdReadState,
    finished_frame: bool,
}

impl<R: Read> ZstdBlockReader<R> {
    fn new(take: Take<R>) -> Result<Self> {
        Ok(Self {
            reader: BufReader::new(take),
            decoder: raw::Decoder::new()?,
            state: ZstdReadState::Reading,
            finished_frame: false,
        })
    }

    fn next_block(&mut self, block_len: u64) {
        self.reader.get_mut().set_limit(block_len);
        self.state = ZstdReadState::Reading;
        // `finished_frame` is intentionally left untouched: if the previous
        // block completed normally it is already `true`, which makes
        // the first read() of the new block lazily reinit() the DCtx (a
        // cheap SessionOnly reset) before decoding, reusing the same
        // context instead of allocating a new one.
    }

    fn inner_reader_mut(&mut self) -> &mut R {
        self.reader.get_mut().get_mut()
    }
}

impl<R: Read> Read for ZstdBlockReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut first = true;
        loop {
            match self.state {
                ZstdReadState::Reading => {
                    let (bytes_read, bytes_written) = {
                        let input: &[u8] = if first { b"" } else { self.reader.fill_buf()? };
                        if !first && input.is_empty() {
                            self.state = ZstdReadState::PastEof;
                            continue;
                        }
                        first = false;

                        let mut src = raw::InBuffer::around(input);
                        let mut dst = raw::OutBuffer::around(buf);

                        if self.finished_frame && !input.is_empty() {
                            self.decoder.reinit()?;
                            self.finished_frame = false;
                        }

                        let hint = self.decoder.run(&mut src, &mut dst)?;
                        if hint == 0 {
                            self.finished_frame = true;
                        }
                        (src.pos(), dst.pos())
                    };
                    self.reader.consume(bytes_read);
                    if bytes_written > 0 {
                        return Ok(bytes_written);
                    }
                }
                ZstdReadState::PastEof => {
                    let mut dst = raw::OutBuffer::around(buf);
                    let hint = self.decoder.finish(&mut dst, self.finished_frame)?;
                    if hint == 0 {
                        self.state = ZstdReadState::Finished;
                    }
                    return Ok(dst.pos());
                }
                ZstdReadState::Finished => return Ok(0),
            }
        }
    }
}

pub struct IpcCompressionReader<R: Read + 'static> {
    input: InputState<R>,
}
unsafe impl<R: Read> Send for IpcCompressionReader<R> {}

#[derive(Default)]
enum InputState<R: Read + 'static> {
    #[default]
    Unreachable,
    BlockStart(R),
    BlockContent(BlockDecompressor<R>),
    Finished,
}

impl<R: Read> IpcCompressionReader<R> {
    pub fn new(input: R) -> Self {
        Self {
            input: InputState::BlockStart(input),
        }
    }

    pub fn read_batch(&mut self, schema: &SchemaRef) -> Result<Option<(usize, Vec<ArrayRef>)>> {
        struct Reader<'a, R: Read + 'static>(&'a mut IpcCompressionReader<R>);
        impl<'a, R: Read> Read for Reader<'a, R> {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                match std::mem::take(&mut self.0.input) {
                    InputState::BlockStart(mut input) => {
                        let block_len = match input.read_u32::<LittleEndian>() {
                            Ok(block_len) => block_len,
                            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                                self.0.input = InputState::Finished;
                                return Ok(0);
                            }
                            Err(err) => {
                                return Err(err);
                            }
                        };
                        let taken = input.take(block_len as u64);

                        self.0.input = InputState::BlockContent(BlockDecompressor::new(taken)?);
                        self.read(buf)
                    }
                    InputState::BlockContent(mut decompressor) => match decompressor.read(buf) {
                        Ok(len) if len > 0 => {
                            self.0.input = InputState::BlockContent(decompressor);
                            Ok(len)
                        }
                        Ok(_zero) => {
                            // current block fully consumed; try to move on to the next
                            // block while reusing the same decompressor (and therefore
                            // the same lz4/zstd codec context) instead of recreating it.
                            let next_block_len = {
                                let inner = decompressor.inner_reader_mut();
                                match inner.read_u32::<LittleEndian>() {
                                    Ok(len) => Some(len),
                                    Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                                        None
                                    }
                                    Err(err) => return Err(err),
                                }
                            };
                            match next_block_len {
                                Some(len) => {
                                    decompressor.next_block(len as u64);
                                    self.0.input = InputState::BlockContent(decompressor);
                                    self.read(buf)
                                }
                                None => {
                                    self.0.input = InputState::Finished;
                                    Ok(0)
                                }
                            }
                        }
                        Err(err) => Err(err),
                    },
                    InputState::Finished => Ok(0),
                    InputState::Unreachable => unreachable!(),
                }
            }
        }
        read_one_batch(&mut Reader(self), schema)
    }
}

pub enum IoCompressionWriter<W: Write> {
    LZ4(lz4_flex::frame::FrameEncoder<W>),
    ZSTD(zstd::Encoder<'static, W>),
}

impl<W: Write> IoCompressionWriter<W> {
    pub fn new_with_configured_codec(inner: W) -> Self {
        Self::try_new(io_compression_codec(), inner).expect("error creating compression encoder")
    }

    pub fn try_new(codec: &str, inner: W) -> Result<Self> {
        match codec {
            "lz4" => Ok(Self::LZ4(lz4_flex::frame::FrameEncoder::new(inner))),
            "zstd" => Ok(Self::ZSTD(zstd::Encoder::new(
                inner,
                conf::SPARK_IO_COMPRESSION_ZSTD_LEVEL.value().unwrap_or(1),
            )?)),
            _ => df_execution_err!("unsupported codec: {}", codec),
        }
    }

    pub fn finish(mut self) -> Result<()> {
        self.finish_internal()
    }

    fn finish_internal(&mut self) -> Result<()> {
        match self {
            IoCompressionWriter::LZ4(w) => {
                w.try_finish()
                    .or_else(|_| df_execution_err!("ipc compression error"))?;
            }
            IoCompressionWriter::ZSTD(w) => {
                w.do_finish()?;
            }
        }
        Ok(())
    }
}

impl<W: Write> Write for IoCompressionWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            IoCompressionWriter::LZ4(w) => w.write(buf),
            IoCompressionWriter::ZSTD(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            IoCompressionWriter::LZ4(w) => w.flush(),
            IoCompressionWriter::ZSTD(w) => w.flush(),
        }
    }
}

pub enum IoCompressionReader<R: Read> {
    LZ4(lz4_flex::frame::FrameDecoder<R>),
    ZSTD(zstd::Decoder<'static, BufReader<R>>),
}

impl<R: Read> IoCompressionReader<R> {
    pub fn new_with_configured_codec(inner: R) -> Self {
        Self::try_new(io_compression_codec(), inner).expect("error creating compression encoder")
    }

    pub fn try_new(codec: &str, inner: R) -> Result<Self> {
        match codec {
            "lz4" => Ok(Self::LZ4(lz4_flex::frame::FrameDecoder::new(inner))),
            "zstd" => Ok(Self::ZSTD(zstd::Decoder::new(inner)?)),
            _ => df_execution_err!("unsupported codec: {}", codec),
        }
    }

    pub fn finish_into_inner(self) -> Result<R> {
        match self {
            Self::LZ4(r) => Ok(r.into_inner()),
            Self::ZSTD(r) => Ok(r.finish().into_inner()),
        }
    }
}

impl<R: Read> Read for IoCompressionReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::LZ4(r) => r.read(buf),
            Self::ZSTD(r) => r.read(buf),
        }
    }
}

fn io_compression_codec() -> &'static str {
    static CODEC: OnceCell<String> = OnceCell::new();
    CODEC
        .get_or_try_init(|| {
            if is_jni_bridge_inited() {
                conf::SPARK_IO_COMPRESSION_CODEC.value()
            } else {
                Ok("lz4".to_string()) // for testing
            }
        })
        .expect("error reading spark.io.compression.codec")
        .as_str()
}

#[derive(Default)]
struct VecBuffer {
    vec: Box<Vec<u8>>,
}

struct VecBufferWrite {
    unsafe_inner: *mut Vec<u8>,
}

impl Write for VecBufferWrite {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let inner = unsafe { &mut *self.unsafe_inner };
        inner.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl VecBuffer {
    fn inner(&self) -> &Vec<u8> {
        &self.vec
    }

    fn inner_mut(&mut self) -> &mut Vec<u8> {
        &mut self.vec
    }

    fn writer(&mut self) -> VecBufferWrite {
        VecBufferWrite {
            unsafe_inner: &mut *self.vec as *mut Vec<u8>,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io::Cursor, sync::Arc};

    use arrow::{
        array::StringArray,
        datatypes::{DataType, Field, Schema},
    };

    use super::*;

    #[test]
    fn test_ipc_compression() -> Result<(), Box<dyn Error>> {
        let mut buf = vec![];
        let mut writer = IpcCompressionWriter::new(&mut buf);

        let test_array1: ArrayRef = Arc::new(StringArray::from(vec![Some("hello"), Some("world")]));
        let test_array2: ArrayRef = Arc::new(StringArray::from(vec![Some("foo"), Some("bar")]));
        let schema = Arc::new(Schema::new(vec![Field::new("", DataType::Utf8, false)]));

        writer.write_batch(2, &[test_array1.clone()])?;
        // force finishing the first block before writing the second batch, so that the
        // block-level compressor (lz4 FrameEncoder reused across blocks by default in
        // tests) has to start a brand new frame for the second block.
        writer.finish_current_buf()?;
        writer.write_batch(2, &[test_array2.clone()])?;
        writer.finish_current_buf()?;

        let mut reader = IpcCompressionReader::new(Cursor::new(buf));
        let (num_rows1, arrays1) = reader.read_batch(&schema)?.expect("non-empty batch");
        assert_eq!(num_rows1, 2);
        assert_eq!(arrays1, &[test_array1]);
        let (num_rows2, arrays2) = reader.read_batch(&schema)?.expect("non-empty batch");
        assert_eq!(num_rows2, 2);
        assert_eq!(arrays2, &[test_array2]);
        assert!(reader.read_batch(&schema)?.is_none());
        Ok(())
    }

    #[test]
    fn test_zstd_cctx_reuse_across_frames() -> Result<(), Box<dyn Error>> {
        let payload1 = b"the quick brown fox jumps over the lazy dog".repeat(16);
        let payload2 = b"another totally different payload for the second frame".repeat(16);

        let raw_encoder = raw::Encoder::new(1)?;

        // frame 1
        let mut buf1 = vec![];
        let mut w = zio::Writer::new(&mut buf1, raw_encoder);
        w.write_all(&payload1)?;
        w.finish()?;
        let (_, mut raw_encoder) = w.into_inner();

        // reuse the same raw_encoder (and therefore the same CCtx) for an independent
        // second frame, after a cheap reinit() instead of recreating the encoder.
        raw_encoder.reinit()?;

        let mut buf2 = vec![];
        let mut w = zio::Writer::new(&mut buf2, raw_encoder);
        w.write_all(&payload2)?;
        w.finish()?;

        let decoded1 = zstd::decode_all(Cursor::new(buf1))?;
        let decoded2 = zstd::decode_all(Cursor::new(buf2))?;
        assert_eq!(decoded1, payload1);
        assert_eq!(decoded2, payload2);
        Ok(())
    }

    #[test]
    fn test_ipc_compression_many_blocks() -> Result<(), Box<dyn Error>> {
        let mut buf = vec![];
        let mut writer = IpcCompressionWriter::new(&mut buf);

        let schema = Arc::new(Schema::new(vec![Field::new("", DataType::Utf8, false)]));
        let arrays: Vec<ArrayRef> = (0..5)
            .map(|i| {
                Arc::new(StringArray::from(vec![
                    Some(format!("row{i}-a")),
                    Some(format!("row{i}-b")),
                ])) as ArrayRef
            })
            .collect();

        for array in &arrays {
            writer.write_batch(2, &[array.clone()])?;
            // force a block boundary after every batch, exercising the decompressor's
            // block-boundary reuse logic across many consecutive blocks.
            writer.finish_current_buf()?;
        }

        let mut reader = IpcCompressionReader::new(Cursor::new(buf));
        for array in &arrays {
            let (num_rows, read_arrays) = reader.read_batch(&schema)?.expect("non-empty batch");
            assert_eq!(num_rows, 2);
            assert_eq!(&read_arrays, &[array.clone()]);
        }
        assert!(reader.read_batch(&schema)?.is_none());
        Ok(())
    }

    #[test]
    fn test_zstd_block_reader_reuse_across_blocks() -> Result<(), Box<dyn Error>> {
        let payloads = [
            b"the quick brown fox jumps over the lazy dog".repeat(16),
            b"another totally different payload for the second block".repeat(16),
            b"a third, independent payload for the third block".repeat(16),
        ];

        // encode each payload as an independent zstd frame, length-prefixed the same
        // way `IpcCompressionWriter` frames its blocks.
        let mut stream = vec![];
        for payload in &payloads {
            let raw_encoder = raw::Encoder::new(1)?;
            let mut block_buf = vec![];
            let mut w = zio::Writer::new(&mut block_buf, raw_encoder);
            w.write_all(payload)?;
            w.finish()?;
            stream.write_u32::<LittleEndian>(block_buf.len() as u32)?;
            stream.extend_from_slice(&block_buf);
        }

        let mut cursor = Cursor::new(stream);
        let block_len = Read::by_ref(&mut cursor).read_u32::<LittleEndian>()?;
        let take = Read::by_ref(&mut cursor).take(block_len as u64);

        // only construct a single ZstdBlockReader (and therefore a single DCtx) for the
        // first block; every subsequent block reuses it via `next_block`, mirroring
        // what `BlockDecompressor::next_block` does for real multi-block IPC
        // streams.
        let mut block_reader = ZstdBlockReader::new(take)?;
        for (i, payload) in payloads.iter().enumerate() {
            let mut decoded = vec![];
            block_reader.read_to_end(&mut decoded)?;
            assert_eq!(&decoded, payload);

            if i + 1 < payloads.len() {
                let next_len = block_reader.inner_reader_mut().read_u32::<LittleEndian>()?;
                block_reader.next_block(next_len as u64);
            }
        }
        Ok(())
    }
}
