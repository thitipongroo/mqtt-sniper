use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static RNG_COUNTER: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);

fn next_random_u64(seed_extra: u64) -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let count = RNG_COUNTER.fetch_add(0xBF58_476D_1CE4_E5B9, Ordering::Relaxed);
    let mut z = nanos
        .wrapping_add(count)
        .wrapping_add(seed_extra)
        .wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn generate_uuid_v4(seed: u64) -> String {
    let hi = next_random_u64(seed);
    let lo = next_random_u64(seed.wrapping_add(1));

    let p1 = (hi >> 32) as u32;
    let p2 = ((hi >> 16) & 0xFFFF) as u16;
    let p3 = (((hi & 0x0FFF) | 0x4000) & 0xFFFF) as u16;
    let p4 = ((((lo >> 48) & 0x3FFF) | 0x8000) & 0xFFFF) as u16;
    let p5 = lo & 0x0000_FFFF_FFFF_FFFF;

    format!("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}", p1, p2, p3, p4, p5)
}

pub fn build_payload(template: &str, device_id: usize, seq: usize, min_size: usize) -> Vec<u8> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    let seed = ((device_id as u64) << 32) ^ (seq as u64);

    // Replace placeholders in template with dynamic values
    let mut rendered = template
        .replace("{device_id}", &device_id.to_string())
        .replace("{seq}", &seq.to_string())
        .replace("{timestamp}", &timestamp.to_string());

    if rendered.contains("{random_int}") {
        let rand_int = next_random_u64(seed) % 101;
        rendered = rendered.replace("{random_int}", &rand_int.to_string());
    }

    if rendered.contains("{random_float}") {
        let rand_float = 20.0 + ((next_random_u64(seed.wrapping_add(7)) % 2001) as f64 / 100.0);
        rendered = rendered.replace("{random_float}", &format!("{:.2}", rand_float));
    }

    if rendered.contains("{uuid}") {
        rendered = rendered.replace("{uuid}", &generate_uuid_v4(seed));
    }

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

    #[test]
    fn test_build_payload_dynamic_random_placeholders() {
        let template = r#"{"uuid":"{uuid}","int":{random_int},"float":{random_float}}"#;
        let payload = build_payload(template, 1, 2, 0);
        let parsed: serde_json::Value = serde_json::from_slice(&payload).unwrap();

        let uuid_str = parsed["uuid"].as_str().unwrap();
        assert_eq!(uuid_str.len(), 36);
        assert_eq!(&uuid_str[14..15], "4");

        let int_val = parsed["int"].as_u64().unwrap();
        assert!(int_val <= 100);

        let float_val = parsed["float"].as_f64().unwrap();
        assert!((20.0..=40.0).contains(&float_val));
    }
}
