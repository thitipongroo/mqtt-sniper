use mqtt_sniper::cli::PayloadFormat;
use mqtt_sniper::payload::{
    build_payload, build_payload_formatted, build_payload_formatted_with_chaos,
    build_payload_with_chaos, decode_varint, decode_zigzag_varint, extract_avro_timestamp,
    extract_protobuf_timestamp, should_drop_packet,
};

#[test]
fn test_build_payload() {
    let template = r#"{"id":{device_id},"s":{seq}}"#;
    let payload = build_payload(template, 42, 7, 0);
    let s = String::from_utf8(payload).expect("valid utf8");
    assert_eq!(s, r#"{"id":42,"s":7}"#);

    // Test with minimum size padding
    let padded = build_payload(template, 42, 7, 25);
    assert_eq!(padded.len(), 25);
    let padded_str = String::from_utf8(padded).expect("valid utf8");
    assert!(padded_str.starts_with(r#"{"id":42,"s":7}"#));
    assert!(padded_str.ends_with("xxxxxxxxx"));
}

#[test]
fn test_build_payload_dynamic_random_placeholders() {
    let template = r#"{"uuid":"{uuid}","int":{random_int},"float":{random_float},"bool":{random_bool},"sec":{timestamp_secs},"us":{timestamp_us}}"#;
    let payload = build_payload(template, 1, 2, 0);
    let parsed: serde_json::Value = serde_json::from_slice(&payload).expect("valid json");

    let uuid_str = parsed["uuid"].as_str().expect("uuid string");
    assert_eq!(uuid_str.len(), 36);
    assert_eq!(&uuid_str[14..15], "4");

    let int_val = parsed["int"].as_i64().expect("int value");
    assert!((0..=100).contains(&int_val));

    let float_val = parsed["float"].as_f64().expect("float value");
    assert!((20.0..=40.0).contains(&float_val));

    assert!(parsed["bool"].is_boolean());
    assert!(parsed["sec"].as_u64().expect("sec u64") > 1_700_000_000);
    assert!(parsed["us"].as_u64().expect("us u64") > 1_700_000_000_000_000);
}

#[test]
fn test_parametric_random_placeholders() {
    let template = r#"{"current":{random_float:80.0:120.0},"rpm":{random_int:2000:5000},"offset":{random_int:-50:-10},"fine":{random_float:0.01:0.09:3}}"#;
    let payload = build_payload(template, 10, 5, 0);
    let parsed: serde_json::Value = serde_json::from_slice(&payload).expect("valid json");

    let current = parsed["current"].as_f64().expect("current float");
    assert!((80.0..=120.0).contains(&current));

    let rpm = parsed["rpm"].as_i64().expect("rpm i64");
    assert!((2000..=5000).contains(&rpm));

    let offset = parsed["offset"].as_i64().expect("offset i64");
    assert!((-50..=-10).contains(&offset));

    let fine = parsed["fine"].as_f64().expect("fine float");
    assert!((0.01..=0.09).contains(&fine));
}

#[test]
fn test_build_payload_with_chaos() {
    let template = r#"{"id":{device_id}}"#;
    let (valid_bytes, is_malformed) = build_payload_with_chaos(template, 1, 0, 0, 0.0);
    assert!(!is_malformed);
    assert!(serde_json::from_slice::<serde_json::Value>(&valid_bytes).is_ok());

    let (corrupt_bytes, is_malformed_all) = build_payload_with_chaos(template, 1, 0, 0, 1.0);
    assert!(is_malformed_all);
    assert!(serde_json::from_slice::<serde_json::Value>(&corrupt_bytes).is_err());
}

#[test]
fn test_protobuf_payload_encoding_and_timestamp() {
    let template = r#"{"sensor": "temp"}"#;
    let proto_bytes = build_payload_formatted(PayloadFormat::Protobuf, template, 42, 10, 64);
    assert!(proto_bytes.len() >= 64);

    // Verify protobuf fields
    let mut offset = 0;
    // Field 1 tag 0x08
    let tag1 = decode_varint(&proto_bytes, &mut offset).expect("tag 1");
    assert_eq!(tag1, 0x08);
    let dev_id = decode_varint(&proto_bytes, &mut offset).expect("dev id");
    assert_eq!(dev_id, 42);

    // Field 2 tag 0x10
    let tag2 = decode_varint(&proto_bytes, &mut offset).expect("tag 2");
    assert_eq!(tag2, 0x10);
    let seq = decode_varint(&proto_bytes, &mut offset).expect("seq");
    assert_eq!(seq, 10);

    // Field 3 tag 0x18
    let tag3 = decode_varint(&proto_bytes, &mut offset).expect("tag 3");
    assert_eq!(tag3, 0x18);
    let ts = decode_varint(&proto_bytes, &mut offset).expect("ts");
    assert!(ts > 1_700_000_000_000);

    // Check helper extract_protobuf_timestamp
    let extracted_ts = extract_protobuf_timestamp(&proto_bytes).expect("extracted ts");
    assert_eq!(extracted_ts, ts);
}

#[test]
fn test_avro_payload_encoding_and_timestamp() {
    let template = r#"{"sensor": "pressure"}"#;
    let avro_bytes = build_payload_formatted(PayloadFormat::Avro, template, 99, 5, 50);
    assert!(avro_bytes.len() >= 50);

    let mut offset = 0;
    let dev_id = decode_zigzag_varint(&avro_bytes, &mut offset).expect("dev id");
    assert_eq!(dev_id, 99);

    let seq = decode_zigzag_varint(&avro_bytes, &mut offset).expect("seq");
    assert_eq!(seq, 5);

    let ts = decode_zigzag_varint(&avro_bytes, &mut offset).expect("ts");
    assert!(ts > 1_700_000_000_000);

    let extracted_ts = extract_avro_timestamp(&avro_bytes).expect("extracted avro ts");
    assert_eq!(extracted_ts as i64, ts);
}

#[test]
fn test_raw_payload_encoding() {
    let template = "DEVICE:{device_id};SEQ:{seq}";
    let raw = build_payload_formatted(PayloadFormat::Raw, template, 7, 3, 30);
    assert_eq!(raw.len(), 30);
    let raw_str = String::from_utf8(raw).expect("utf8");
    assert!(raw_str.starts_with("DEVICE:7;SEQ:3"));
}

#[test]
fn test_should_drop_packet_distribution() {
    assert!(!should_drop_packet(0.0, 1, 1));
    assert!(should_drop_packet(1.0, 1, 1));

    // For rate 0.20 across 5000 messages, drop count should be ~1000
    let mut dropped = 0;
    let total = 5000;
    for seq in 0..total {
        if should_drop_packet(0.20, 1, seq) {
            dropped += 1;
        }
    }
    let drop_rate = dropped as f64 / total as f64;
    assert!(
        (0.17..=0.23).contains(&drop_rate),
        "Expected drop rate in 0.17..0.23, got {}",
        drop_rate
    );
}

#[test]
fn test_binary_chaos_corruption() {
    let (proto_bad, is_bad1) =
        build_payload_formatted_with_chaos(PayloadFormat::Protobuf, "{}", 1, 1, 0, 1.0);
    assert!(is_bad1);
    assert_eq!(extract_protobuf_timestamp(&proto_bad), None);

    let (avro_bad, is_bad2) =
        build_payload_formatted_with_chaos(PayloadFormat::Avro, "{}", 1, 1, 0, 1.0);
    assert!(is_bad2);
    assert_eq!(extract_avro_timestamp(&avro_bad), None);
}
