use mqtt_sniper::payload::{build_payload, build_payload_with_chaos};

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
    let template = r#"{"uuid":"{uuid}","int":{random_int},"float":{random_float},"bool":{random_bool},"sec":{timestamp_secs},"us":{timestamp_us}}"#;
    let payload = build_payload(template, 1, 2, 0);
    let parsed: serde_json::Value = serde_json::from_slice(&payload).unwrap();

    let uuid_str = parsed["uuid"].as_str().unwrap();
    assert_eq!(uuid_str.len(), 36);
    assert_eq!(&uuid_str[14..15], "4");

    let int_val = parsed["int"].as_i64().unwrap();
    assert!((0..=100).contains(&int_val));

    let float_val = parsed["float"].as_f64().unwrap();
    assert!((20.0..=40.0).contains(&float_val));

    assert!(parsed["bool"].is_boolean());
    assert!(parsed["sec"].as_u64().unwrap() > 1_700_000_000);
    assert!(parsed["us"].as_u64().unwrap() > 1_700_000_000_000_000);
}

#[test]
fn test_parametric_random_placeholders() {
    let template = r#"{"current":{random_float:80.0:120.0},"rpm":{random_int:2000:5000},"offset":{random_int:-50:-10},"fine":{random_float:0.01:0.09:3}}"#;
    let payload = build_payload(template, 10, 5, 0);
    let parsed: serde_json::Value = serde_json::from_slice(&payload).unwrap();

    let current = parsed["current"].as_f64().unwrap();
    assert!((80.0..=120.0).contains(&current));

    let rpm = parsed["rpm"].as_i64().unwrap();
    assert!((2000..=5000).contains(&rpm));

    let offset = parsed["offset"].as_i64().unwrap();
    assert!((-50..=-10).contains(&offset));

    let fine = parsed["fine"].as_f64().unwrap();
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
