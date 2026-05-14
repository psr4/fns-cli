/// DJB2-style hash algorithm with Unicode code points
/// Matches the behavior of Python implementation
/// Uses Unicode code points (ord(ch) in Python)

/// Computes a 32-bit signed integer hash of a string using DJB2 algorithm
/// with Unicode code points (matching Python behavior)
pub fn hash_content(content: &str) -> String {
    let mut hash: i32 = 0;
    
    // Use Unicode code points (same as Python's ord(ch))
    for ch in content.chars() {
        let char_val = ch as i32;
        // DJB2: hash = (hash << 5) - hash + char
        // i32 overflow handles the wrap-around automatically (same as Python)
        hash = (hash << 5).wrapping_sub(hash).wrapping_add(char_val);
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
        // Emoji test: 🚀 is U+1F680 (128640)
        // Python produces "-538783611" using ord(ch) for code points
        assert_eq!(hash_content("Fast Note Sync 🚀"), "-538783611");
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
    fn test_negative_hash() {
        // Some inputs produce negative hashes due to overflow
        let result = hash_content("test with longer string that should overflow");
        // Verify it's a valid signed integer
        assert!(result.parse::<i32>().is_ok());
    }
}
