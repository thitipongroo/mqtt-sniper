use clap::Parser;
use rumqttc::{AsyncClient, Event, MqttOptions, Outgoing, QoS};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task;

// 1. สร้าง Struct สำหรับรับค่าจาก Command Line
#[derive(Parser, Debug)]
#[command(author, version, about = "Blazing fast MQTT Load Tester")]
struct Args {
    #[arg(short, long, default_value = "localhost")]
    broker: String,

    #[arg(short, long, default_value_t = 1883)]
    port: u16,

    #[arg(short, long, default_value_t = 100)]
    clients: usize, // จำนวนอุปกรณ์ IoT จำลอง

    #[arg(short, long, default_value_t = 10)]
    messages: usize, // จำนวนข้อความต่อ 1 อุปกรณ์

    #[arg(short, long, default_value = "sensor/sniper")]
    topic: String,

    #[arg(short, long, default_value_t = 0, value_parser = clap::value_parser!(u8).range(0..=2))]
    qos: u8, // ระดับ QoS (0 = AtMostOnce, 1 = AtLeastOnce, 2 = ExactlyOnce)

    #[arg(long, default_value_t = 0)]
    ramp_up_ms: u64, // หน่วงเวลา (ms) ก่อนสร้าง Client ตัวถัดไป (ป้องกัน TCP SYN Flood)

    #[arg(short, long, default_value_t = 0)]
    interval_ms: u64, // หน่วงเวลา (ms) ระหว่างการส่งแต่ละข้อความของอุปกรณ์เดียวกัน

    #[arg(
        long,
        default_value = r#"{"device_id": {device_id}, "seq": {seq}, "ts": {timestamp}}"#
    )]
    payload: String, // รูปแบบข้อความ (รองรับตัวแปร {device_id}, {seq}, {timestamp})

    #[arg(long, default_value_t = 0)]
    payload_size: usize, // บังคับขนาด Payload ขั้นต่ำ (Bytes) ถ้าระบุ > 0 จะเติมข้อมูลให้ครบขนาด

    #[arg(short, long)]
    username: Option<String>,

    #[arg(long)]
    password: Option<String>,
}

fn parse_qos(qos: u8) -> QoS {
    match qos {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

fn build_payload(template: &str, device_id: usize, seq: usize, min_size: usize) -> Vec<u8> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    // แทนที่ Placeholder ใน Template ด้วยค่าจริง
    let mut rendered = template
        .replace("{device_id}", &device_id.to_string())
        .replace("{seq}", &seq.to_string())
        .replace("{timestamp}", &timestamp.to_string());

    // หากผู้ใช้กำหนด --payload-size ให้เติมตัวอักษร 'x' จนได้ขนาด Bytes ตามต้องการ
    if min_size > rendered.len() {
        let padding_len = min_size - rendered.len();
        rendered.push_str(&"x".repeat(padding_len));
    }

    rendered.into_bytes()
}

// 2. ฟังก์ชันหลัก (ครอบด้วย tokio::main เพื่อให้รัน Async ได้)
#[tokio::main]
async fn main() {
    let args = Args::parse();
    let qos = parse_qos(args.qos);

    println!("🚀 Starting MQTT Sniper...");
    println!("Broker: tcp://{}:{}", args.broker, args.port);
    println!(
        "Spawning {} concurrent clients ({} msgs/client, QoS {})...",
        args.clients, args.messages, args.qos
    );

    let start_time = Instant::now();
    let mut handles = Vec::with_capacity(args.clients);
    let published_count = Arc::new(AtomicUsize::new(0));
    let failed_clients = Arc::new(AtomicUsize::new(0));

    // 3. วนลูปสร้าง Client แบบ "พร้อมกัน" (Concurrent Tasks)
    for i in 0..args.clients {
        // ถ้าระบุ --ramp-up-ms มา ให้หน่วงเวลาก่อน spawn Client ตัวถัดไป
        if args.ramp_up_ms > 0 && i > 0 {
            tokio::time::sleep(Duration::from_millis(args.ramp_up_ms)).await;
        }

        let broker = args.broker.clone();
        let topic = args.topic.replace("{device_id}", &i.to_string());
        let payload_template = args.payload.clone();
        let payload_size = args.payload_size;
        let port = args.port;
        let messages = args.messages;
        let interval_ms = args.interval_ms;
        let username = args.username.clone();
        let password = args.password.clone();
        let published_counter = Arc::clone(&published_count);
        let failed_counter = Arc::clone(&failed_clients);

        // แยก Task ทำงานอิสระ (เหมือน Goroutine ใน Go)
        let handle = task::spawn(async move {
            let client_id = format!("sniper-client-{}", i);
            let mut mqttoptions = MqttOptions::new(client_id, broker, port);
            mqttoptions.set_keep_alive(Duration::from_secs(5));

            if let Some(ref user) = username {
                mqttoptions.set_credentials(user, password.as_deref().unwrap_or(""));
            }

            let (client, mut eventloop) = AsyncClient::new(mqttoptions, 50);

            // สาดข้อความ (Publish) ใน Task ย่อย เพื่อส่งคำสั่งเข้า Channel
            let publisher = tokio::spawn(async move {
                for j in 0..messages {
                    let payload_bytes = build_payload(&payload_template, i, j, payload_size);
                    if client
                        .publish(topic.as_str(), qos, false, payload_bytes)
                        .await
                        .is_err()
                    {
                        break;
                    }
                    if args.interval_ms > 0 && j + 1 < messages {
                        tokio::time::sleep(Duration::from_millis(args.interval_ms)).await;
                    }
                }
                let _ = client.disconnect().await;
            });

            // ต้องมี Loop ดึง Event ออกมา (กฎของ rumqttc) และรอจนกว่าจะส่งออกทาง TCP จริงๆ
            loop {
                match eventloop.poll().await {
                    Ok(Event::Outgoing(Outgoing::Publish(_))) => {
                        published_counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Event::Outgoing(Outgoing::Disconnect)) => {
                        break;
                    }
                    Ok(_) => {}
                    Err(_) => {
                        failed_counter.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }

            let _ = publisher.await;
        });

        handles.push(handle); // เก็บ Task ไว้รอดูตอนจบ
    }

    // 4. รอให้ทุก Client ยิงเสร็จ
    for handle in handles {
        let _ = handle.await;
    }

    // 5. สรุปผล
    let elapsed = start_time.elapsed();
    let target_messages = args.clients * args.messages;
    let actual_sent = published_count.load(Ordering::Relaxed);
    let errors = failed_clients.load(Ordering::Relaxed);

    println!("✅ Load test completed in {:.2?}", elapsed);
    println!(
        "📊 Total messages sent: {} / {} (Failed clients: {})",
        actual_sent, target_messages, errors
    );
    println!(
        "⚡ Throughput: {:.2} msgs/sec",
        (actual_sent as f64) / elapsed.as_secs_f64()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_qos() {
        assert_eq!(parse_qos(0), QoS::AtMostOnce);
        assert_eq!(parse_qos(1), QoS::AtLeastOnce);
        assert_eq!(parse_qos(2), QoS::ExactlyOnce);
        assert_eq!(parse_qos(99), QoS::AtMostOnce);
    }

    #[test]
    fn test_cli_default_args() {
        let args = Args::parse_from(["mqtt-sniper"]);
        assert_eq!(args.broker, "localhost");
        assert_eq!(args.port, 1883);
        assert_eq!(args.clients, 100);
        assert_eq!(args.messages, 10);
        assert_eq!(args.topic, "sensor/sniper");
        assert_eq!(args.qos, 0);
    }
}