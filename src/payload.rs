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

fn process_parametric_random_placeholders(mut text: String, seed: u64) -> String {
    let mut salt: u64 = 7;

    // Process {random_int} and {random_int:min:max}
    let mut cursor = 0;
    while let Some(rel_start) = text[cursor..].find("{random_int") {
        let start = cursor + rel_start;
        if let Some(end_rel) = text[start..].find('}') {
            let end = start + end_rel;
            let placeholder = &text[start..=end];
            let inner = &placeholder[1..placeholder.len() - 1];
            let parts: Vec<&str> = inner.split(':').collect();

            let replacement = match parts.as_slice() {
                ["random_int"] => {
                    let rand = (next_random_u64(seed.wrapping_add(salt)) % 101) as i64;
                    salt = salt.wrapping_add(17);
                    Some(rand.to_string())
                }
                ["random_int", min_str, max_str] => {
                    if let (Ok(min), Ok(max)) =
                        (min_str.trim().parse::<i64>(), max_str.trim().parse::<i64>())
                    {
                        let (low, high) = if min <= max { (min, max) } else { (max, min) };
                        let span = (high.saturating_sub(low).saturating_add(1)) as u64;
                        let rand = if span > 0 {
                            low.wrapping_add(
                                (next_random_u64(seed.wrapping_add(salt)) % span) as i64,
                            )
                        } else {
                            low
                        };
                        salt = salt.wrapping_add(17);
                        Some(rand.to_string())
                    } else {
                        None
                    }
                }
                _ => None,
            };

            if let Some(val) = replacement {
                text.replace_range(start..=end, &val);
                cursor = start + val.len();
            } else {
                cursor = end + 1;
            }
        } else {
            break;
        }
    }

    // Process {random_float}, {random_float:min:max}, and {random_float:min:max:precision}
    cursor = 0;
    while let Some(rel_start) = text[cursor..].find("{random_float") {
        let start = cursor + rel_start;
        if let Some(end_rel) = text[start..].find('}') {
            let end = start + end_rel;
            let placeholder = &text[start..=end];
            let inner = &placeholder[1..placeholder.len() - 1];
            let parts: Vec<&str> = inner.split(':').collect();

            let replacement = match parts.as_slice() {
                ["random_float"] => {
                    let rand_float =
                        20.0 + ((next_random_u64(seed.wrapping_add(salt)) % 2001) as f64 / 100.0);
                    salt = salt.wrapping_add(19);
                    Some(format!("{:.2}", rand_float))
                }
                ["random_float", min_str, max_str] => {
                    if let (Ok(min), Ok(max)) =
                        (min_str.trim().parse::<f64>(), max_str.trim().parse::<f64>())
                    {
                        let (low, high) = if min <= max { (min, max) } else { (max, min) };
                        let r =
                            (next_random_u64(seed.wrapping_add(salt)) % 100_001) as f64 / 100_000.0;
                        let val = low + r * (high - low);
                        salt = salt.wrapping_add(19);
                        Some(format!("{:.2}", val))
                    } else {
                        None
                    }
                }
                ["random_float", min_str, max_str, prec_str] => {
                    if let (Ok(min), Ok(max), Ok(prec)) = (
                        min_str.trim().parse::<f64>(),
                        max_str.trim().parse::<f64>(),
                        prec_str.trim().parse::<usize>(),
                    ) {
                        let (low, high) = if min <= max { (min, max) } else { (max, min) };
                        let r = (next_random_u64(seed.wrapping_add(salt)) % 1_000_001) as f64
                            / 1_000_000.0;
                        let val = low + r * (high - low);
                        salt = salt.wrapping_add(19);
                        Some(format!("{:.*}", prec.min(10), val))
                    } else {
                        None
                    }
                }
                _ => None,
            };

            if let Some(val) = replacement {
                text.replace_range(start..=end, &val);
                cursor = start + val.len();
            } else {
                cursor = end + 1;
            }
        } else {
            break;
        }
    }

    text
}

pub fn build_payload(template: &str, device_id: usize, seq: usize, min_size: usize) -> Vec<u8> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let timestamp_ms = now.as_millis();
    let timestamp_secs = now.as_secs();
    let timestamp_us = now.as_micros();

    let seed = ((device_id as u64) << 32) ^ (seq as u64);

    // Replace basic dynamic placeholders
    let mut rendered = template
        .replace("{device_id}", &device_id.to_string())
        .replace("{seq}", &seq.to_string())
        .replace("{timestamp}", &timestamp_ms.to_string())
        .replace("{timestamp_secs}", &timestamp_secs.to_string())
        .replace("{timestamp_us}", &timestamp_us.to_string());

    if rendered.contains("{random_bool}") {
        let rand_bool = (next_random_u64(seed.wrapping_add(13)) & 1) == 0;
        rendered = rendered.replace("{random_bool}", if rand_bool { "true" } else { "false" });
    }

    if rendered.contains("{uuid}") {
        rendered = rendered.replace("{uuid}", &generate_uuid_v4(seed));
    }

    // Process parametric random integers and floats ({random_int:min:max}, {random_float:min:max:prec})
    rendered = process_parametric_random_placeholders(rendered, seed);

    // Pad payload with 'x' bytes if minimum size is requested
    if min_size > rendered.len() {
        let padding_len = min_size - rendered.len();
        rendered.push_str(&"x".repeat(padding_len));
    }

    rendered.into_bytes()
}

pub fn build_payload_with_chaos(
    template: &str,
    device_id: usize,
    seq: usize,
    min_size: usize,
    malformed_rate: f64,
) -> (Vec<u8>, bool) {
    if malformed_rate > 0.0 {
        let seed = ((device_id as u64) << 32) ^ (seq as u64) ^ 0x517C_C1B7_2722_0A95;
        let sample = (next_random_u64(seed) % 10_000) as f64 / 10_000.0;
        if malformed_rate >= 1.0 || sample < malformed_rate {
            let malformed = format!("{{malformed_json_device_{}_seq_{}", device_id, seq);
            return (malformed.into_bytes(), true);
        }
    }
    (build_payload(template, device_id, seq, min_size), false)
}
