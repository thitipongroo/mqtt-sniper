pub use crate::cli::PayloadFormat;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static RNG_COUNTER: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);

/// Pseudo-random 64-bit integer generator using high-resolution clock and splitmix-style mixing.
pub fn next_random_u64(seed_extra: u64) -> u64 {
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

/// Deterministic pseudo-random packet drop evaluation for chaos injection testing.
pub fn should_drop_packet(packet_drop: f64, device_id: usize, seq: usize) -> bool {
    if packet_drop <= 0.0 {
        return false;
    }
    if packet_drop >= 1.0 {
        return true;
    }
    let seed = ((device_id as u64) << 32) ^ (seq as u64) ^ 0x6A09_E667_F3BC_C908;
    let sample = (next_random_u64(seed) % 1_000_000) as f64 / 1_000_000.0;
    sample < packet_drop
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

/// Encode an unsigned 64-bit integer into a variable-length byte sequence (varint).
pub fn encode_varint(mut val: u64, buf: &mut Vec<u8>) {
    while val >= 0x80 {
        buf.push(((val as u8) & 0x7F) | 0x80);
        val >>= 7;
    }
    buf.push(val as u8);
}

/// Decode a variable-length unsigned 64-bit integer (varint) from a byte slice at offset.
pub fn decode_varint(buf: &[u8], offset: &mut usize) -> Option<u64> {
    let mut result = 0u64;
    let mut shift = 0;
    while *offset < buf.len() {
        let byte = buf[*offset];
        *offset += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if (byte & 0x80) == 0 {
            return Some(result);
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

/// Encode a signed 64-bit integer using Apache Avro zigzag varint coding.
pub fn encode_zigzag_varint(n: i64, buf: &mut Vec<u8>) {
    let val = ((n << 1) ^ (n >> 63)) as u64;
    encode_varint(val, buf);
}

/// Decode a signed 64-bit integer from Apache Avro zigzag varint bytes.
pub fn decode_zigzag_varint(buf: &[u8], offset: &mut usize) -> Option<i64> {
    let val = decode_varint(buf, offset)?;
    let decoded = ((val >> 1) as i64) ^ (-((val & 1) as i64));
    Some(decoded)
}

/// Attempt to extract timestamp (field 3) from a Protocol Buffers compact binary stream.
pub fn extract_protobuf_timestamp(buf: &[u8]) -> Option<u64> {
    let mut offset = 0;
    while offset < buf.len() {
        let key = decode_varint(buf, &mut offset)?;
        let field_num = key >> 3;
        let wire_type = key & 0x07;
        match wire_type {
            0 => {
                let val = decode_varint(buf, &mut offset)?;
                if field_num == 3 {
                    return Some(val);
                }
            }
            1 => {
                if offset + 8 > buf.len() {
                    return None;
                }
                offset += 8;
            }
            2 => {
                let len = decode_varint(buf, &mut offset)? as usize;
                if offset + len > buf.len() {
                    return None;
                }
                offset += len;
            }
            5 => {
                if offset + 4 > buf.len() {
                    return None;
                }
                offset += 4;
            }
            _ => return None,
        }
    }
    None
}

/// Attempt to extract timestamp (field 3) from an Apache Avro compact binary stream.
pub fn extract_avro_timestamp(buf: &[u8]) -> Option<u64> {
    let mut offset = 0;
    let _device_id = decode_zigzag_varint(buf, &mut offset)?;
    let _seq = decode_zigzag_varint(buf, &mut offset)?;
    let ts = decode_zigzag_varint(buf, &mut offset)?;
    if ts >= 0 { Some(ts as u64) } else { None }
}

/// Render template string replacing device_id, sequence, timestamp, and parametric random variables.
pub fn render_template_text(
    template: &str,
    device_id: usize,
    seq: usize,
    timestamp_ms: u128,
    timestamp_secs: u64,
    timestamp_us: u128,
) -> String {
    let seed = ((device_id as u64) << 32) ^ (seq as u64);

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

    process_parametric_random_placeholders(rendered, seed)
}

/// Render a dynamic MQTT payload replacing device ID, sequence, timestamp, and parametric random placeholders.
pub fn build_payload(template: &str, device_id: usize, seq: usize, min_size: usize) -> Vec<u8> {
    build_payload_formatted(PayloadFormat::Json, template, device_id, seq, min_size)
}

/// Render a dynamic payload conforming to the requested `PayloadFormat` (JSON, Protobuf, Avro, Raw).
pub fn build_payload_formatted(
    format: PayloadFormat,
    template: &str,
    device_id: usize,
    seq: usize,
    min_size: usize,
) -> Vec<u8> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let timestamp_ms = now.as_millis();
    let timestamp_secs = now.as_secs();
    let timestamp_us = now.as_micros();

    let rendered = render_template_text(
        template,
        device_id,
        seq,
        timestamp_ms,
        timestamp_secs,
        timestamp_us,
    );

    match format {
        PayloadFormat::Json => {
            let mut s = rendered;
            if min_size > s.len() {
                let padding_len = min_size - s.len();
                s.push_str(&"x".repeat(padding_len));
            }
            s.into_bytes()
        }
        PayloadFormat::Raw => {
            let mut b = rendered.into_bytes();
            if min_size > b.len() {
                let pad = min_size - b.len();
                b.resize(b.len() + pad, b'x');
            }
            b
        }
        PayloadFormat::Protobuf => {
            let mut buf = Vec::with_capacity(min_size.max(64));
            // Field 1: uint64 device_id = 1 (tag 0x08)
            encode_varint(0x08, &mut buf);
            encode_varint(device_id as u64, &mut buf);
            // Field 2: uint64 seq = 2 (tag 0x10)
            encode_varint(0x10, &mut buf);
            encode_varint(seq as u64, &mut buf);
            // Field 3: uint64 timestamp = 3 (tag 0x18)
            encode_varint(0x18, &mut buf);
            encode_varint(timestamp_ms as u64, &mut buf);

            // Field 4: bytes payload_data = 4 (tag 0x22)
            let mut data = rendered.into_bytes();
            let est_overhead = buf.len() + 1 + 5;
            if min_size > est_overhead + data.len() {
                let pad = min_size - (est_overhead + data.len());
                data.resize(data.len() + pad, b'x');
            }
            encode_varint(0x22, &mut buf);
            encode_varint(data.len() as u64, &mut buf);
            buf.extend_from_slice(&data);

            if min_size > buf.len() {
                let pad = min_size - buf.len();
                buf.resize(buf.len() + pad, b'x');
            }
            buf
        }
        PayloadFormat::Avro => {
            let mut buf = Vec::with_capacity(min_size.max(64));
            // Record SensorReading
            // Field 1: device_id (long)
            encode_zigzag_varint(device_id as i64, &mut buf);
            // Field 2: seq (long)
            encode_zigzag_varint(seq as i64, &mut buf);
            // Field 3: timestamp (long)
            encode_zigzag_varint(timestamp_ms as i64, &mut buf);

            // Field 4: payload_bytes (bytes)
            let mut data = rendered.into_bytes();
            let est_overhead = buf.len() + 5;
            if min_size > est_overhead + data.len() {
                let pad = min_size - (est_overhead + data.len());
                data.resize(data.len() + pad, b'x');
            }
            encode_zigzag_varint(data.len() as i64, &mut buf);
            buf.extend_from_slice(&data);

            if min_size > buf.len() {
                let pad = min_size - buf.len();
                buf.resize(buf.len() + pad, b'x');
            }
            buf
        }
    }
}

/// Render a payload with optional deterministic pseudo-random chaos corruption for dead-letter-queue testing.
pub fn build_payload_with_chaos(
    template: &str,
    device_id: usize,
    seq: usize,
    min_size: usize,
    malformed_rate: f64,
) -> (Vec<u8>, bool) {
    build_payload_formatted_with_chaos(
        PayloadFormat::Json,
        template,
        device_id,
        seq,
        min_size,
        malformed_rate,
    )
}

/// Render a formatted payload with optional deterministic pseudo-random chaos corruption.
pub fn build_payload_formatted_with_chaos(
    format: PayloadFormat,
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
            let corrupted = match format {
                PayloadFormat::Json => {
                    format!("{{malformed_json_device_{}_seq_{}", device_id, seq).into_bytes()
                }
                PayloadFormat::Protobuf => {
                    vec![0xFF, 0xFF, 0xFF, 0x7F, 0xDE, 0xAD, 0xBE, 0xEF]
                }
                PayloadFormat::Avro => {
                    vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xDE, 0xAD]
                }
                PayloadFormat::Raw => {
                    format!("MALFORMED_RAW_DATA_DEVICE_{}_SEQ_{}", device_id, seq).into_bytes()
                }
            };
            return (corrupted, true);
        }
    }
    (
        build_payload_formatted(format, template, device_id, seq, min_size),
        false,
    )
}
