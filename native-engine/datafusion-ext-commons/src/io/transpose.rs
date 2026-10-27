// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::simd::{Simd, Swizzle};

use num::Num;

use crate::SliceAsRawBytes;

/// Number of bytes processed by a single SIMD block. Must be divisible by
/// `size_of::<T>()` for every `T` that implements `Lanes` below.
const BLOCK_BYTES: usize = 64;

/// Computes the index table that permutes an `a`-row x `b`-col (row-major)
/// byte block into a `b`-row x `a`-col (row-major) block. `a * b` must equal
/// `BLOCK_BYTES`.
const fn make_block_idx(a: usize, b: usize) -> [usize; BLOCK_BYTES] {
    let mut idx = [0usize; BLOCK_BYTES];
    let mut i = 0;
    while i < BLOCK_BYTES {
        idx[i] = (i % a) * b + i / a;
        i += 1;
    }
    idx
}

/// Provides, for a numeric type `T`, how many rows (`ROWS`) of `T` fit in one
/// `BLOCK_BYTES`-sized SIMD block (`W` is `size_of::<T>()`), together with the
/// SIMD shuffle that transposes one such block. Implementations are generated
/// by the `impl_lanes!` macro below for all concrete native types used by
/// Arrow arrays, so that the shuffle index table is a compile-time constant
/// and no unstable `generic_const_exprs` is needed.
pub trait Lanes: Num + Copy {
    const W: usize;
    const ROWS: usize = BLOCK_BYTES / Self::W;

    /// Transposes one ROWS x W (row-major) block into W x ROWS (row-major).
    fn fwd_swizzle(v: Simd<u8, BLOCK_BYTES>) -> Simd<u8, BLOCK_BYTES>;
    /// Inverse of `fwd_swizzle`: transposes W x ROWS back into ROWS x W.
    fn inv_swizzle(v: Simd<u8, BLOCK_BYTES>) -> Simd<u8, BLOCK_BYTES>;
}

macro_rules! impl_lanes {
    ($t:ty) => {
        impl Lanes for $t {
            const W: usize = ::std::mem::size_of::<$t>();

            #[inline]
            fn fwd_swizzle(v: Simd<u8, BLOCK_BYTES>) -> Simd<u8, BLOCK_BYTES> {
                struct S;
                impl Swizzle<BLOCK_BYTES> for S {
                    const INDEX: [usize; BLOCK_BYTES] = make_block_idx(
                        BLOCK_BYTES / ::std::mem::size_of::<$t>(),
                        ::std::mem::size_of::<$t>(),
                    );
                }
                S::swizzle(v)
            }

            #[inline]
            fn inv_swizzle(v: Simd<u8, BLOCK_BYTES>) -> Simd<u8, BLOCK_BYTES> {
                struct S;
                impl Swizzle<BLOCK_BYTES> for S {
                    const INDEX: [usize; BLOCK_BYTES] = make_block_idx(
                        ::std::mem::size_of::<$t>(),
                        BLOCK_BYTES / ::std::mem::size_of::<$t>(),
                    );
                }
                S::swizzle(v)
            }
        }
    };
}

impl_lanes!(u8);
impl_lanes!(i8);
impl_lanes!(u16);
impl_lanes!(i16);
impl_lanes!(u32);
impl_lanes!(i32);
impl_lanes!(f32);
impl_lanes!(u64);
impl_lanes!(i64);
impl_lanes!(f64);
impl_lanes!(u128);
impl_lanes!(i128);

/// Transposes `input` (`input.len()` rows x `size_of::<T>()` cols, row-major)
/// into `output` (`size_of::<T>()` rows x `input.len()` cols, row-major),
/// where `output.len() == input.len() * size_of::<T>()`.
///
/// Full `BLOCK_BYTES`-sized blocks are transposed with a single SIMD load +
/// shuffle, writing `size_of::<T>()` contiguous chunks into `output`; any
/// remaining rows that don't fill a full block are handled by a scalar
/// fallback loop.
pub fn transpose<T: Lanes>(input: &[T], output: &mut [u8]) {
    let h = input.len();
    assert_eq!(output.len(), h * T::W);

    let bytes = input.as_raw_bytes();
    let full_blocks = h / T::ROWS;
    let simd_rows = full_blocks * T::ROWS;
    let out_ptr = output.as_mut_ptr();

    for (block_idx, chunk) in bytes[..simd_rows * T::W]
        .chunks_exact(BLOCK_BYTES)
        .enumerate()
    {
        let v = Simd::<u8, BLOCK_BYTES>::from_slice(chunk);
        let t = T::fwd_swizzle(v);
        let arr: [u8; BLOCK_BYTES] = t.to_array();

        let row0 = block_idx * T::ROWS;
        for c in 0..T::W {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    arr.as_ptr().add(c * T::ROWS),
                    out_ptr.add(c * h + row0),
                    T::ROWS,
                );
            }
        }
    }

    for row in simd_rows..h {
        for c in 0..T::W {
            output[c * h + row] = bytes[row * T::W + c];
        }
    }
}

/// Inverse of `transpose`: reconstructs `output` (`output.len()` elements of
/// `T`) from `input` (`size_of::<T>()` rows x `output.len()` cols, row-major),
/// where `input.len() == output.len() * size_of::<T>()`.
pub fn inverse_transpose<T: Lanes>(input: &[u8], output: &mut [T]) {
    let h = output.len();
    assert_eq!(input.len(), h * T::W);

    let out_bytes = output.as_raw_bytes_mut();
    let full_blocks = h / T::ROWS;
    let simd_rows = full_blocks * T::ROWS;
    let in_ptr = input.as_ptr();
    let mut block_buf = [0u8; BLOCK_BYTES];

    for block_idx in 0..full_blocks {
        let row0 = block_idx * T::ROWS;
        for c in 0..T::W {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    in_ptr.add(c * h + row0),
                    block_buf.as_mut_ptr().add(c * T::ROWS),
                    T::ROWS,
                );
            }
        }

        let v = Simd::<u8, BLOCK_BYTES>::from_slice(&block_buf);
        let t = T::inv_swizzle(v);
        let arr: [u8; BLOCK_BYTES] = t.to_array();

        unsafe {
            std::ptr::copy_nonoverlapping(
                arr.as_ptr(),
                out_bytes.as_mut_ptr().add(row0 * T::W),
                BLOCK_BYTES,
            );
        }
    }

    for row in simd_rows..h {
        for c in 0..T::W {
            out_bytes[row * T::W + c] = input[c * h + row];
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn test_roundtrip<T: Lanes + std::fmt::Debug + PartialEq>(input: Vec<T>) {
        let h = input.len();
        let mut transposed = vec![0u8; h * T::W];
        transpose(&input, &mut transposed);

        let mut restored: Vec<T> = (0..h).map(|_| T::zero()).collect();
        inverse_transpose(&transposed, &mut restored);
        assert_eq!(input, restored);
    }

    #[test]
    fn test_roundtrip_various_lens_u64() {
        for len in [0, 1, 7, 8, 9, 63, 64, 65, 1000, 1_000_000] {
            let input: Vec<u64> = (0..len as u64)
                .map(|i| i.wrapping_mul(0x9E3779B9))
                .collect();
            test_roundtrip(input);
        }
    }

    #[test]
    fn test_roundtrip_various_lens_i32() {
        for len in [0, 1, 7, 16, 17, 100, 12345] {
            let input: Vec<i32> = (0i32..len).map(|i| i.wrapping_mul(-1640531527)).collect();
            test_roundtrip(input);
        }
    }

    #[test]
    fn test_roundtrip_u8() {
        let input: Vec<u8> = (0..=255u8).cycle().take(4097).collect();
        test_roundtrip(input);
    }

    #[test]
    fn test_roundtrip_i128() {
        let input: Vec<i128> = (0..1000i128).map(|i| i.wrapping_mul(123456789)).collect();
        test_roundtrip(input);
    }

    #[test]
    fn test_matches_naive_transpose() {
        let input: Vec<i64> = (0..513i64).map(|i| i.wrapping_mul(0xabcdef)).collect();
        let h = input.len();
        let w = 8;
        let bytes = input.as_raw_bytes();
        let mut expected = vec![0u8; w * h];
        for row in 0..h {
            for col in 0..w {
                expected[col * h + row] = bytes[row * w + col];
            }
        }
        let mut actual = vec![0u8; w * h];
        transpose(&input, &mut actual);
        assert_eq!(expected, actual);
    }
}
