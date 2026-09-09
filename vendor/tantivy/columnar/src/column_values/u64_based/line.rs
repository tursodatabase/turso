use std::io;
use std::num::NonZeroU32;

use common::{BinarySerializable, VInt};

use crate::column_values::ColumnValues;

const MID_POINT: u64 = (1u64 << 32) - 1u64;

/// `Line` describes a line function `y: ax + b` using integer
/// arithmetic.
///
/// The slope is in fact a decimal split into a 32 bit integer value,
/// and a 32-bit decimal value.
///
/// The multiplication then becomes.
/// `y = m * x >> 32 + b`
#[derive(Debug, Clone, Copy, Default)]
pub struct Line {
    pub(crate) slope: u64,
    pub(crate) intercept: u64,
}

/// Compute the line slope.
///
/// This function has the nice property of being
/// invariant by translation.
/// `
///   compute_slope(y0, y1)
/// = compute_slope(y0 + X % 2^64, y1 + X % 2^64)
/// `
fn compute_slope(y0: u64, y1: u64, num_vals: NonZeroU32) -> u64 {
    let dy = y1.wrapping_sub(y0);
    let sign = dy <= (1 << 63);
    let abs_dy = if sign {
        y1.wrapping_sub(y0)
    } else {
        y0.wrapping_sub(y1)
    };
    if abs_dy >= 1 << 32 {
        // This is outside of realm we handle.
        // Let's just bail.
        return 0u64;
    }

    let abs_slope = (abs_dy << 32) / num_vals.get() as u64;
    if sign {
        abs_slope
    } else {
        // The complement does indeed create the
        // opposite decreasing slope...
        //
        // Intuitively (without the bitshifts and % u64::MAX)
        // ```
        //    (x + shift)*(u64::MAX - abs_slope)
        // -  (x * (u64::MAX - abs_slope))
        // = - shift * abs_slope
        // ```
        u64::MAX - abs_slope
    }
}

impl Line {
    #[inline(always)]
    pub fn eval(&self, x: u32) -> u64 {
        let linear_part = ((x as u64).wrapping_mul(self.slope) >> 32) as i32 as u64;
        self.intercept.wrapping_add(linear_part)
    }

    // Intercept is only computed from provided positions
    pub fn train_from(
        first_val: u64,
        last_val: u64,
        num_vals: u32,
        positions_and_values: impl Iterator<Item = (u64, u64)>,
    ) -> Self {
        // TODO replace with let else
        let idx_last_val = if let Some(idx_last_val) = NonZeroU32::new(num_vals - 1) {
            idx_last_val
        } else {
            return Line::default();
        };

        let y0 = first_val;
        let y1 = last_val;

        // We first independently pick our slope.
        let slope = compute_slope(y0, y1, idx_last_val);

        // We picked our slope. Note that it does not have to be perfect.
        // Now we need to compute the best intercept.
        //
        // Intuitively, the best intercept is such that line passes through one of the
        // `(i, ys[])`.
        //
        // The best intercept therefore has the form
        // `y[i] - line.eval(i)` (using wrapping arithmetic).
        // In other words, the best intercept is one of the `y - Line::eval(ys[i])`
        // and our task is just to pick the one that minimizes our error.
        //
        // Without sorting our values, this is a difficult problem.
        // We however rely on the following trick...
        //
        // We only focus on the case where the interpolation is half decent.
        // If the line interpolation is doing its job on a dataset suited for it,
        // we can hope that the maximum error won't be larger than `u64::MAX / 2`.
        //
        // In other words, even without the intercept the values `y - Line::eval(ys[i])` will all be
        // within an interval that takes less than half of the modulo space of `u64`.
        //
        // Our task is therefore to identify this interval.
        // Here we simply translate all of our values by `y0 - 2^63` and pick the min.
        let mut line = Line {
            slope,
            intercept: 0,
        };
        let heuristic_shift = y0.wrapping_sub(MID_POINT);
        line.intercept = positions_and_values
            .map(|(pos, y)| y.wrapping_sub(line.eval(pos as u32)))
            .min_by_key(|&val| val.wrapping_sub(heuristic_shift))
            .unwrap_or(0u64); //< Never happens.
        line
    }

    /// Returns a line that attempts to approximate a function
    /// f: i in 0..[ys.num_vals()) -> ys[i].
    ///
    /// - The approximation is always lower than the actual value. Or more rigorously, formally
    ///   `f(i).wrapping_sub(ys[i])` is small for any i in [0..ys.len()).
    /// - It computes without panicking for any value of it.
    ///
    /// This function is only invariable by translation if all of the
    /// `ys` are packaged into half of the space. (See heuristic below)
    /// TODO USE array
    pub fn train(ys: &dyn ColumnValues) -> Self {
        let first_val = ys.iter().next().unwrap();
        let last_val = ys.iter().nth(ys.num_vals() as usize - 1).unwrap();
        Self::train_from(
            first_val,
            last_val,
            ys.num_vals(),
            ys.iter().enumerate().map(|(pos, val)| (pos as u64, val)),
        )
    }
}

impl BinarySerializable for Line {
    fn serialize<W: io::Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        VInt(self.slope).serialize(writer)?;
        VInt(self.intercept).serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let slope = VInt::deserialize(reader)?.0;
        let intercept = VInt::deserialize(reader)?.0;
        Ok(Line { slope, intercept })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_values::VecColumn;

    /// Test training a line and ensuring that the maximum difference between
    /// the data points and the line is `expected`.
    ///
    /// This function operates translation over the data for better coverage.
    #[track_caller]
    fn test_line_interpol_with_translation(ys: &[u64], expected: Option<u64>) {
        let mut translations = vec![0, 100, u64::MAX / 2, u64::MAX, u64::MAX - 1];
        translations.extend_from_slice(ys);
        for translation in translations {
            let translated_ys: Vec<u64> = ys
                .iter()
                .copied()
                .map(|y| y.wrapping_add(translation))
                .collect();
            let largest_err = test_eval_max_err(&translated_ys);
            assert_eq!(largest_err, expected);
        }
    }

    fn test_eval_max_err(ys: &[u64]) -> Option<u64> {
        let line = Line::train(&VecColumn::from(ys.to_vec()));
        ys.iter()
            .enumerate()
            .map(|(x, y)| y.wrapping_sub(line.eval(x as u32)))
            .max()
    }

    #[test]
    fn test_train() {
        test_line_interpol_with_translation(&[11, 11, 11, 12, 12, 13], Some(1));
        test_line_interpol_with_translation(&[13, 12, 12, 11, 11, 11], Some(1));
        test_line_interpol_with_translation(&[13, 13, 12, 11, 11, 11], Some(1));
        test_line_interpol_with_translation(&[13, 13, 12, 11, 11, 11], Some(1));
        test_line_interpol_with_translation(&[u64::MAX - 1, 0, 0, 1], Some(1));
        test_line_interpol_with_translation(&[u64::MAX - 1, u64::MAX, 0, 1], Some(0));
        test_line_interpol_with_translation(&[0, 1, 2, 3, 5], Some(0));
        test_line_interpol_with_translation(&[1, 2, 3, 4], Some(0));

        let data: Vec<u64> = (0..255).collect();
        test_line_interpol_with_translation(&data, Some(0));
        let data: Vec<u64> = (0..255).map(|el| el * 2).collect();
        test_line_interpol_with_translation(&data, Some(0));
    }
}
