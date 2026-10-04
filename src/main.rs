use clap::Parser;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::time::Instant;
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
}

// 2. ฟังก์ชันหลัก (ครอบด้วย tokio::main เพื่อให้รัน Async ได้)
#[tokio::main]
async fn main() {
    let args = Args::parse();
    
    println!("🚀 Starting MQTT Sniper...");
    println!("Broker: tcp://{}:{}", args.broker, args.port);
    println!("Spawning {} concurrent clients...", args.clients);
    
    let start_time = Instant::now();
    let mut handles = Vec::new();

    // 3. วนลูปสร้าง Client แบบ "พร้อมกัน" (Concurrent Tasks)
    for i in 0..args.clients {
        let broker = args.broker.clone();
        let topic = args.topic.clone();
        let port = args.port;
        let messages = args.messages;

        // แยก Task ทำงานอิสระ (เหมือน Goroutine ใน Go)
        let handle = task::spawn(async move {
            let client_id = format!("sniper-client-{}", i);
            let mut mqttoptions = MqttOptions::new(client_id, broker, port);
            mqttoptions.set_keep_alive(std::time::Duration::from_secs(5));
            
            let (client, mut eventloop) = AsyncClient::new(mqttoptions, 50);

            // ต้องมี Loop เปล่าๆ ดึง Event ออกมา (กฎของ rumqttc)
            tokio::spawn(async move {
                loop {
                    if eventloop.poll().await.is_err() { break; }
                }
            });

            // สาดข้อความ (Publish)
            for j in 0..messages {
                let payload = format!(r#"{{"device_id": {}, "seq": {}}}"#, i, j);
                let _ = client.publish(topic.clone(), QoS::AtMostOnce, false, payload.into_bytes()).await;
            }
            let _ = client.disconnect().await;
        });
        
        handles.push(handle); // เก็บ Task ไว้รอดูตอนจบ
    }

    // 4. รอให้ทุก Client ยิงเสร็จ
    for handle in handles {
        let _ = handle.await;
    }

    // 5. สรุปผล
    let elapsed = start_time.elapsed();
    let total_messages = args.clients * args.messages;
    
    println!("✅ Load test completed in {:.2?}", elapsed);
    println!("📊 Total messages sent: {}", total_messages);
    println!("⚡ Throughput: {:.2} msgs/sec", (total_messages as f64) / elapsed.as_secs_f64());
}