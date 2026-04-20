//! Savitzky-Golay filter implementation using ndarray.
//!
//! This module provides a lightweight implementation of the Savitzky-Golay smoothing filter,
//! which applies polynomial least-squares fitting to smooth data with minimal phase distortion.

use ndarray::{Array1, Array2};

/// Applies Savitzky-Golay smoothing to the input data.
///
/// For each point, a polynomial of the given order is fit to a window of data
/// centered at that point, and the smoothed value is the value of the polynomial
/// at the center. This provides smoothing with minimal phase distortion.
///
/// # Arguments
///
/// * `data` - The input data to smooth
/// * `window_length` - The length of the filter window (must be a positive odd integer)
/// * `poly_order` - The order of the polynomial used to fit the samples (must be < window_length)
///
/// # Returns
///
/// A vector of smoothed intensities, or an error message if parameters are invalid.
pub fn savgol_filter(data: &[f64], window_length: usize, poly_order: usize) -> Result<Vec<f64>, String> {
    // Validate parameters
    if window_length % 2 == 0 {
        return Err("Window length must be odd".to_string());
    }
    if poly_order >= window_length {
        return Err("Polynomial order must be less than window length".to_string());
    }
    if data.is_empty() {
        return Err("Data cannot be empty".to_string());
    }
    if window_length < 1 {
        return Err("Window length must be at least 1".to_string());
    }

    let half_window = window_length / 2;
    let mut smoothed = Vec::with_capacity(data.len());

    for i in 0..data.len() {
        // For each point, fit a polynomial to window and evaluate at center
        let smoothed_value = compute_smoothed_point(data, i, window_length, poly_order)?;
        smoothed.push(smoothed_value);
    }

    Ok(smoothed)
}

/// Compute the smoothed value at a single point using polynomial least-squares fitting.
fn compute_smoothed_point(
    data: &[f64],
    center_idx: usize,
    window_length: usize,
    poly_order: usize,
) -> Result<f64, String> {
    let half_window = window_length / 2;
    let n = data.len();

    // Determine window bounds by clamping to valid data range
    let raw_start = center_idx as i32 - half_window as i32;
    let raw_end = center_idx as i32 + half_window as i32 + 1;

    let start_idx = raw_start.max(0) as usize;
    let end_idx = (raw_end as usize).min(n);

    let window_data: Vec<f64> = data[start_idx..end_idx].to_vec();

    // Fit polynomial to window
    let coeffs = fit_polynomial(&window_data, poly_order)?;

    // Evaluate polynomial at the position corresponding to center_idx
    // Adjust x-coordinate to be relative to start_idx
    let center_x = (center_idx as i32 - start_idx as i32) as f64;
    let mut result = 0.0;
    for (i, &c) in coeffs.iter().enumerate() {
        result += c * center_x.powi(i as i32);
    }

    Ok(result)
}

/// Fit a polynomial of order `poly_order` to the data using least-squares.
fn fit_polynomial(data: &[f64], poly_order: usize) -> Result<Vec<f64>, String> {
    if data.is_empty() || data.len() < poly_order + 1 {
        return Err("Not enough data points for polynomial fit".to_string());
    }

    let n = data.len();
    let m = poly_order + 1;

    // Build Vandermonde matrix
    #[allow(non_snake_case)]
    let mut A = Array2::<f64>::zeros((n, m));
    for i in 0..n {
        for j in 0..m {
            A[[i, j]] = (i as f64).powi(j as i32);
        }
    }

    // Build target vector
    let b = Array1::from(data.to_vec());

    // Solve using Gaussian elimination with partial pivoting
    let coeffs = gaussian_elimination(&A, &b)?;

    Ok(coeffs.to_vec())
}

/// Solve Ax = b using Gaussian elimination with partial pivoting.
#[allow(non_snake_case)]
fn gaussian_elimination(A: &Array2<f64>, b: &Array1<f64>) -> Result<Array1<f64>, String> {
    let n = A.nrows();
    let m = A.ncols();

    if n < m {
        return Err("Underdetermined system".to_string());
    }

    // Create augmented matrix [A | b]
    let mut aug = Array2::<f64>::zeros((n, m + 1));
    for i in 0..n {
        for j in 0..m {
            aug[[i, j]] = A[[i, j]];
        }
        aug[[i, m]] = b[i];
    }

    // Forward elimination with partial pivoting
    for col in 0..m {
        // Find pivot
        let mut max_row = col;
        let mut max_val = aug[[col, col]].abs();
        for row in col + 1..n {
            if aug[[row, col]].abs() > max_val {
                max_val = aug[[row, col]].abs();
                max_row = row;
            }
        }

        // Swap rows if needed
        if max_row != col {
            for j in 0..m + 1 {
                let temp = aug[[col, j]];
                aug[[col, j]] = aug[[max_row, j]];
                aug[[max_row, j]] = temp;
            }
        }

        // Eliminate column
        for row in col + 1..n {
            if aug[[col, col]].abs() < 1e-14 {
                continue;
            }
            let factor = aug[[row, col]] / aug[[col, col]];
            for j in col..m + 1 {
                aug[[row, j]] -= factor * aug[[col, j]];
            }
        }
    }

    // Back substitution
    let mut x = Array1::<f64>::zeros(m);
    for i in (0..m).rev() {
        let mut sum = aug[[i, m]];
        for j in i + 1..m {
            sum -= aug[[i, j]] * x[j];
        }
        if aug[[i, i]].abs() < 1e-14 {
            x[i] = 0.0;
        } else {
            x[i] = sum / aug[[i, i]];
        }
    }

    Ok(x)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_length_must_be_odd() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = savgol_filter(&data, 4, 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_poly_order_must_be_less_than_window() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = savgol_filter(&data, 5, 5);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_data() {
        let data = vec![];
        let result = savgol_filter(&data, 3, 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_single_element() {
        let data = vec![5.0];
        let result = savgol_filter(&data, 1, 0);
        assert!(result.is_ok());
        let smoothed = result.unwrap();
        assert_eq!(smoothed.len(), 1);
        assert!((smoothed[0] - 5.0).abs() < 0.01);
    }

    #[test]
    fn test_constant_data() {
        let data = vec![5.0, 5.0, 5.0, 5.0, 5.0];
        let result = savgol_filter(&data, 3, 1);
        assert!(result.is_ok());
        let smoothed = result.unwrap();
        for value in &smoothed {
            assert!((value - 5.0).abs() < 0.01);
        }
    }

    #[test]
    fn test_linear_data() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = savgol_filter(&data, 3, 1);
        assert!(result.is_ok());
        let smoothed = result.unwrap();
        // Linear polynomial should be preserved
        for (i, value) in smoothed.iter().enumerate() {
            let expected = (i + 1) as f64;
            assert!((value - expected).abs() < 0.1, "At index {}: expected {}, got {}", i, expected, value);
        }
    }

    #[test]
    fn test_quadratic_data() {
        let data = vec![0.0, 1.0, 4.0, 9.0, 16.0, 25.0];
        let result = savgol_filter(&data, 5, 2);
        assert!(result.is_ok());
        let smoothed = result.unwrap();
        // Quadratic polynomial should be preserved
        for (i, value) in smoothed.iter().enumerate() {
            let expected = (i as f64).powi(2);
            assert!((value - expected).abs() < 0.1, "At index {}: expected {}, got {}", i, expected, value);
        }
    }

    #[test]
    fn test_smoothing_noisy_data() {
        // Create noisy data around y = 2x
        let base: Vec<f64> = (0..10).map(|i| 2.0 * i as f64).collect();
        let noisy: Vec<f64> = base.iter()
            .enumerate()
            .map(|(i, &y)| y + if i % 2 == 0 { 0.5 } else { -0.5 })
            .collect();

        let result = savgol_filter(&noisy, 5, 1);
        assert!(result.is_ok());
        let smoothed = result.unwrap();

        // Check that smoothing reduces variance
        let original_variance = noisy.iter()
            .enumerate()
            .map(|(i, &y)| {
                let expected = 2.0 * i as f64;
                (y - expected).powi(2)
            })
            .sum::<f64>() / noisy.len() as f64;

        let smoothed_variance = smoothed.iter()
            .enumerate()
            .map(|(i, &y)| {
                let expected = 2.0 * i as f64;
                (y - expected).powi(2)
            })
            .sum::<f64>() / smoothed.len() as f64;

        assert!(smoothed_variance < original_variance);
    }

    #[test]
    fn test_derivative_order_zero() {
        // Polynomial order 0 should act like a moving average
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = savgol_filter(&data, 3, 0);
        assert!(result.is_ok());
        let smoothed = result.unwrap();

        // All points should be positive
        for value in &smoothed {
            assert!(*value > 0.0);
        }
    }

    #[test]
    fn test_edge_handling() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = savgol_filter(&data, 5, 2);
        assert!(result.is_ok());
        let smoothed = result.unwrap();

        // First value should be meaningful (not extrapolated too far)
        assert!(smoothed[0] > 0.0 && smoothed[0] < 3.0);
        // Last value should be meaningful
        assert!(smoothed[4] > 3.0 && smoothed[4] < 6.0);
    }

    #[test]
    fn test_large_window() {
        let data: Vec<f64> = (0..20).map(|i| (i as f64).sin()).collect();
        let result = savgol_filter(&data, 9, 3);
        assert!(result.is_ok());
        let smoothed = result.unwrap();
        assert_eq!(smoothed.len(), data.len());
    }
}
