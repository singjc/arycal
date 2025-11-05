

fn get_is_little_endian() -> bool {
    cfg!(target_endian = "little")
}



pub fn decode_fixed_point(data: &[u8]) -> f64 {
    let mut fixed_point: f64 = 0.0;
    let fp_bytes = unsafe {
        // Create a mutable slice of bytes pointing to the memory of `fixed_point`
        std::slice::from_raw_parts_mut(&mut fixed_point as *mut f64 as *mut u8, std::mem::size_of::<f64>())
    };

    // Copy bytes from data to fp_bytes based on endianness
    if get_is_little_endian() {
        for i in 0..8 {
            fp_bytes[i] = data[7 - i];
        }
    } else {
        fp_bytes.copy_from_slice(data);
    }

    fixed_point
}




/// Decodes an int from the half bytes in bp. Lossless reverse of encodeInt 
/// 
/// # Parameters
/// - `data`: A slice of bytes to decode
/// - `di`: A mutable reference to the index in the data array to start decoding (will be advanced)
/// - `max_di`: The size of the data array
/// - `half`: A mutable helper variable (do not change between multiple calls)
/// - `res`: A mutable reference to the result (a 32-bit integer)
///
/// # Note
/// the helper variable indicates whether we look at the first half byte
/// or second half byte of the current data (thus whether to interpret the first
/// half byte of data[*di] or the second half byte).
///
/// # Returns
/// - A `Result` containing `()` on success, or an error message on failure.
pub fn decode_int(
    data: &[u8],
    di: &mut usize,
    max_di: usize,
    half: &mut usize,
    res: &mut u32,
) -> Result<(), String> {
    let n: usize;
    let mask: u32;
    let mut m: u32;
    let head: u8;
    let mut hb: u8;

    // Extract the first half byte, specifying the number of leading zero half bytes of the final integer.
    // If half is zero, we look at the first half byte, otherwise we look at the second (lower) half byte.
    if *half == 0 {
        head = data[*di] >> 4;
    } else {
        head = data[*di] & 0xf;
        *di += 1; // Advance the index
    }

    *half = 1 - *half; // Switch to the other half byte
    *res = 0;

    if head <= 8 {
        n = head as usize; // Number of leading zeros
    } else { // We have n leading ones, fill n half bytes in res with 0xf
        n = (head - 8) as usize;
        mask = 0xf0000000; // Mask for filling leading ones
        for i in 0..n {
            m = mask >> (4 * i);
            *res |= m; // Set leading bits to 1
        }
    }

    if n == 8 {
        return Ok(()); // No more bytes to read
    }

    // Check if we have enough data left to read
    if *di + ((8 - n) - (1 - *half)) / 2 >= max_di {
        return Err("[MSNumpress::decodeInt] Corrupt input data!".to_string());
    }

    for i in n..8 {
        if *half == 0 {
            hb = data[*di] >> 4; // First half byte
        } else {
            hb = data[*di] & 0xf; // Second half byte
            *di += 1; // Advance the index
        }
        *res |= (hb as u32) << ((i - n) * 4); // Shift and set bits
        *half = 1 - *half; // Switch half byte state
    }

    Ok(()) // Return success
}


// Linear

pub fn _encode_linear(_data: &[f64], _result: &mut Vec<u8>, _fixed_point: f64) -> Result<usize, String> {
    unimplemented!("Encoding linear data is not yet implemented.")
}



pub fn _decode_linear(data: &[u8], data_size: usize, result: &mut [f64]) -> Result<usize, String> {
    let mut init: u8;
    let mut buff: u32 = 0;
    let mut diff: i32;
    let mut ints = [0i64; 3];
    let mut di: usize;
    let mut half: usize;
    let mut extrapol: i64;
    let mut y: i64;
    let fixed_point: f64;

    if data_size == 8 {
        return Ok(0);
    }

    if data_size < 8 {
        return Err("[MSNumpress::decodeLinear] Corrupt input data: not enough bytes to read fixed point!".to_string());
    }

    // Decode the fixed point
    fixed_point = decode_fixed_point(data);

    if data_size < 12 {
        return Err("[MSNumpress::decodeLinear] Corrupt input data: not enough bytes to read first value!".to_string());
    }

    ints[1] = 0;
    for i in 0..4 {
        init = data[8 + i];
        ints[1] |= ((init & 0xff) as i64) << (i * 8); // Fixing the shift operation
    }
    
    result[0] = ints[1] as f64 / fixed_point;

    if data_size == 12 {
        return Ok(1);
    }
    
    if data_size < 16 {
        return Err("[MSNumpress::decodeLinear] Corrupt input data: not enough bytes to read second value!".to_string());
    }

    ints[2] = 0;
    for i in 0..4 {
        init = data[12 + i];
        ints[2] |= ((init & 0xff) as i64) << (i * 8); // Fixing the shift operation
    }
    
    result[1] = ints[2] as f64 / fixed_point;

    let mut ri: usize;
    half = 0;
    ri = 2; // Start filling result from index 2
    di = 16; // Start reading from index 16

    while di < data_size {
        if di == (data_size - 1) && half == 1 {
            if (data[di] & 0xf) == 0x0 {
                break; // End of decoding
            }
        }

        ints[0] = ints[1];
        ints[1] = ints[2];

        // Decode the next integer
        decode_int(data, &mut di, data_size, &mut half, &mut buff)?;

        diff = buff as i32; // Cast to int

        extrapol = ints[1] + (ints[1] - ints[0]);
        y = extrapol + diff as i64;

        result[ri] = y as f64 / fixed_point; // Store the result
        ri += 1;
        ints[2] = y; // Update the last integer
    }

    Ok(ri) // Return the number of results decoded
}

pub fn decode_linear(data: &[u8]) -> Result<Vec<f64>, String> {
    let data_size = data.len();
    
    if data_size < 8 {
        return Err("Data size must be at least 8 bytes.".to_string());
    }

    // Initialize the result vector with the expected size
    let mut result = vec![0.0; (data_size - 8) * 2];

    // Call the existing decode_linear function that takes a slice and a mutable reference
    let decoded_length = _decode_linear(data, data_size, &mut result)?;

    // Resize the result to the actual number of decoded values
    result.resize(decoded_length, 0.0);

    Ok(result) // Return the decoded results
}


// SLOF

pub fn _decode_slof(data: &[u8], data_size: usize, result: &mut [f64]) -> Result<usize, String> {
    if data_size < 8 {
        return Err("[MSNumpress::decodeSlof] Corrupt input data: not enough bytes to read fixed point!".to_string());
    }

    let fixed_point = decode_fixed_point(data); // Decode the fixed point
    let mut ri = 0; // Result index

    // Iterate over the data starting from index 8, stepping by 2 bytes
    for i in (8..data_size).step_by(2) {
        // Combine two bytes into an unsigned short
        let x = (data[i] as u16) | ((data[i + 1] as u16) << 8);
        result[ri] = (x as f64 / fixed_point).exp() - 1.0; // Calculate the result
        ri += 1; // Increment result index
    }

    Ok(ri) // Return the number of results decoded
}

pub fn decode_slof(data: &[u8]) -> Result<Vec<f64>, String> {
    let data_size = data.len();
    
    if data_size < 8 {
        return Err("Data size must be at least 8 bytes.".to_string());
    }

    // Initialize the result vector with the expected size
    let mut result = vec![0.0; (data_size - 8) / 2];

    // Call the existing decode_slof function that takes a slice and a mutable reference
    let decoded_length = _decode_slof(data, data_size, &mut result)?;

    // Resize the result to the actual number of decoded values
    result.resize(decoded_length, 0.0);

    Ok(result) // Return the decoded results
}



#[cfg(test)]
mod tests {
    use super::*;
    use std::f64;

    // Test data
    const DATA: [f64; 4] = [100.0, 101.0, 102.0, 103.0];
    const DATA_LONG: [f64; 7] = [
        100.0,
        200.0,
        300.00005,
        400.00010,
        450.00010,
        455.00010,
        700.00010,
    ];
    const DATA_SLOF: [f64; 4] = [100.0, 200.0, 300.00005, 400.00010];
    const FP_SLOF: f64 = 10000.0;
    
    // Expected linear result in raw bytes
    // 40 f8 6a 00 00 00 00 00 80 96 98 00 20 1d 9a 00 88
    const LINEAR_RESULT: [u8; 17] = [
        0x40, 0xf8, 0x6a, 0x00, 
        0x00, 0x00, 0x00, 0x00, 
        0x80, 0x96, 0x98, 0x00, 
        0x20, 0x1d, 0x9a, 0x00,
        0x88
    ];

    // Expected SLOF result in raw bytes
    // 40 c3 88 00 00 00 00 00 47 b4 29 cf ef de 24 ea
    const SLOF_RESULT: &[u8] = &[
        0x40, 0xc3, 0x88, 0x00, 
        0x00, 0x00, 0x00, 0x00, 
        0x47, 0xb4, 0x29, 0xcf, 
        0xef, 0xde, 0x24, 0xea
    ];

    // #[test]
    // fn test_encode_linear() {
    //     let mut encoded = vec![0u8; LINEAR_RESULT.len()]; // Create a mutable Vec<u8> for encoding
    //     let result_length = encode_linear(&DATA, &mut encoded, 100000.0).unwrap(); // Call with correct args
        
    //     println!("Encoded: {:?}", &encoded[..result_length]);
    //     println!("Expected: {:?}", &LINEAR_RESULT);

    //     assert_eq!(&encoded[..result_length], &LINEAR_RESULT);
    //     assert_eq!(encoded.len(), LINEAR_RESULT.len());
        
    //     // // Additional tests for DATA_LONG
    //     // let mut long_encoded = vec![0u8; 30]; // Adjust size as needed
    //     // encode_linear(&DATA_LONG, &mut long_encoded, 5.0);
    //     // assert_eq!(long_encoded.len(), long_encoded.len());
    // }

    #[test]
    fn test_decode_linear() {

        // Call decode_linear with both required arguments
        let decoded = decode_linear(&LINEAR_RESULT).unwrap();

        println!("Decoded: {:?}", decoded);
        println!("Expected: {:?}", DATA);

        // Check that the decoded results match expected values
        assert_eq!(decoded.len(), DATA.len());
        assert!((decoded[0] - 100.0).abs() < 1e-5); // Use a small tolerance for floating-point comparison
        assert!((decoded[1] - 101.0).abs() < 1e-5);
        assert!((decoded[2] - 102.0).abs() < 1e-5);
        assert!((decoded[3] - 103.0).abs() < 1e-5);
    }

    #[test]
    fn test_decode_slof(){

        // Call decode_slof with both required arguments
        let decoded = decode_slof(&SLOF_RESULT).unwrap();

        println!("Decoded: {:?}", decoded);
        println!("Expected: {:?}", DATA_SLOF);

        // Check that the decoded results match expected values
        assert_eq!(decoded.len(), DATA_SLOF.len());

        // We expect some loss in precision due to the nature of the SLOF encoding
        assert!((decoded[0] - 100.0).abs() < 1.0); 
        assert!((decoded[1] - 200.0).abs() < 1.0);
        assert!((decoded[2] - 300.00005).abs() < 1.0);
        assert!((decoded[3] - 400.00010).abs() < 1.0);
    }
}
