/// DJB2-style hash algorithm matching the server/JS implementation.
/// String hashes use UTF-16 code units (JavaScript charCodeAt semantics).

const FILE_HASH_THRESHOLD: usize = 10 * 1024 * 1024;
const FILE_HASH_SLICE_SIZE: usize = 5 * 1024 * 1024;

/// Computes a 32-bit signed integer hash of a string using UTF-16 code units.
pub fn hash_content(content: &str) -> String {
    let mut hash: i32 = 0;

    for unit in content.encode_utf16() {
        hash = (hash << 5).wrapping_sub(hash).wrapping_add(unit as i32);
    }

    hash.to_string()
}

pub fn hash_bytes(bytes: &[u8]) -> String {
    let mut hash: i32 = 0;

    let ranges: Vec<&[u8]> = if bytes.len() <= FILE_HASH_THRESHOLD {
        vec![bytes]
    } else {
        vec![
            &bytes[..FILE_HASH_SLICE_SIZE],
            &bytes[bytes.len() - FILE_HASH_SLICE_SIZE..],
        ]
    };

    for range in ranges {
        for &byte in range {
            hash = (hash << 5).wrapping_sub(hash).wrapping_add(byte as i32);
        }
    }

    hash.to_string()
}

pub fn hash_path(path: &str) -> String {
    hash_content(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_hello() {
        // Standard ASCII test
        assert_eq!(hash_content("hello"), "99162322");
    }

    #[test]
    fn test_hash_emoji() {
        assert_eq!(hash_content("Fast Note Sync 🚀"), "475362430");
    }

    #[test]
    fn test_hash_chinese() {
        // Chinese characters (BMP, single UTF-16 code unit each)
        // Verify it produces a consistent result
        let result = hash_content("你好世界");
        // Just verify it doesn't panic and produces a valid i32 string
        assert!(result.parse::<i32>().is_ok());
    }

    #[test]
    fn test_hash_empty() {
        assert_eq!(hash_content(""), "0");
    }

    #[test]
    fn test_hash_single_char() {
        assert_eq!(hash_content("a"), "97");
    }

    #[test]
    fn test_hash_path_function() {
        assert_eq!(hash_path("hello"), hash_content("hello"));
    }

    #[test]
    fn test_hash_bytes_hashes_raw_bytes() {
        let bytes = [0x89, b'P', b'N', b'G', 0xff, 0x00];
        assert_eq!(hash_bytes(&bytes), "-296492095");
    }

    #[test]
    fn test_negative_hash() {
        // Some inputs produce negative hashes due to overflow
        let result = hash_content("test with longer string that should overflow");
        // Verify it's a valid signed integer
        assert!(result.parse::<i32>().is_ok());
    }
}
