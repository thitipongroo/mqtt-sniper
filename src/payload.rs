use std::time::{SystemTime, UNIX_EPOCH};

pub fn build_payload(template: &str, device_id: usize, seq: usize, min_size: usize) -> Vec<u8> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    // Replace placeholders in template with dynamic values
    let mut rendered = template
        .replace("{device_id}", &device_id.to_string())
        .replace("{seq}", &seq.to_string())
        .replace("{timestamp}", &timestamp.to_string());

    // Pad payload with 'x' bytes if minimum size is requested
    if min_size > rendered.len() {
        let padding_len = min_size - rendered.len();
        rendered.push_str(&"x".repeat(padding_len));
    }

    rendered.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_payload() {
        let template = r#"{"id":{device_id},"s":{seq}}"#;
        let payload = build_payload(template, 42, 7, 0);
        let s = String::from_utf8(payload).unwrap();
        assert_eq!(s, r#"{"id":42,"s":7}"#);

        // Test with minimum size padding
        let padded = build_payload(template, 42, 7, 25);
        assert_eq!(padded.len(), 25);
        let padded_str = String::from_utf8(padded).unwrap();
        assert!(padded_str.starts_with(r#"{"id":42,"s":7}"#));
        assert!(padded_str.ends_with("xxxxxxxxx"));
    }
}
