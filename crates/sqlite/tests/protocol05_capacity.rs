//! Opt-in measurement fed by exact plans exported from the PostgreSQL gate.
use axton_client::{Client, Schema, sync05::DeliveryQueue, v05};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
use std::{collections::BTreeMap, fs, time::Instant};

#[test]
#[ignore = "requires exact real PostgreSQL carriers from the capacity gate"]
fn real_postgres_delivery_capacity() {
    let directory = std::path::PathBuf::from(std::env::var("AXTON_CAPACITY_DIRECTORY").unwrap());
    let schema = Schema::from_value(
        serde_json::from_slice(&fs::read(directory.join("schema.json")).unwrap()).unwrap(),
    )
    .unwrap();
    let path = directory.join("client.db");
    let mut client = Client::open05(
        SqliteStore::open_exclusive05(&path, "User:alice").unwrap(),
        schema,
        "User:alice",
    )
    .unwrap();
    let context = client.request_context05().unwrap();
    if std::env::var("AXTON_CAPACITY_PHASE").unwrap() == "prepare" {
        let head = std::env::var("AXTON_CAPACITY_HEAD")
            .unwrap()
            .parse()
            .unwrap();
        client.initialize_stream05(head).unwrap();
        fs::write(
            directory.join("context.json"),
            serde_json::to_vec(&context).unwrap(),
        )
        .unwrap();
        return;
    }
    let expected: v05::RequestContext =
        serde_json::from_slice(&fs::read(directory.join("context.json")).unwrap()).unwrap();
    assert_eq!(context, expected);
    let mut files: Vec<_> = fs::read_dir(&directory)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("response-")
        })
        .collect();
    files.sort();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let mut queue = DeliveryQueue::new(16 * 1024 * 1024, 32);
    let mut decoded_ms = 0.0;
    let mut admission_ms = 0.0;
    let mut apply_ms = 0.0;
    let mut max_apply_ms: f64 = 0.0;
    let mut max_admission_ms: f64 = 0.0;
    let mut outstanding = BTreeMap::<u64, usize>::new();
    let mut peak_payload = 0;
    let mut applied_units = 0;
    let mut measured = Vec::new();
    for file in files {
        let bytes = fs::read(file).unwrap();
        let started = Instant::now();
        let response: v05::DeliveryResponse = serde_json::from_slice(&bytes).unwrap();
        decoded_ms += started.elapsed().as_secs_f64() * 1000.0;
        for part in &response.parts {
            *outstanding.entry(part.unit).or_default() += serde_json::to_vec(part).unwrap().len();
        }
        peak_payload = peak_payload.max(outstanding.values().sum::<usize>());
        let started = Instant::now();
        queue
            .receive(&response.header, &response.parts, &context, now)
            .unwrap();
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        admission_ms += elapsed;
        max_admission_ms = max_admission_ms.max(elapsed);
        let mut committed = 0;
        loop {
            let started = Instant::now();
            let report = client.apply_next_delivery05(&mut queue, now).unwrap();
            let elapsed = started.elapsed().as_secs_f64() * 1000.0;
            if report.is_none() {
                break;
            }
            apply_ms += elapsed;
            max_apply_ms = max_apply_ms.max(elapsed);
            applied_units += 1;
            committed += 1;
            outstanding.pop_first();
        }
        measured.push(json!({"responseBytes":bytes.len(),"headerBytes":serde_json::to_vec(&response.header).unwrap().len(),"admissionMs":elapsed,"committedUnits":committed}));
    }
    assert!(queue.is_empty());
    let head = std::env::var("AXTON_CAPACITY_HEAD")
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let status = client.store_status05().unwrap();
    assert_eq!(status.bootstrap_cursor, Some(head));
    assert_eq!(
        client
            .read_sql("SELECT COUNT(*) AS n FROM Entry", &[])
            .unwrap()[0]["n"],
        json!(head)
    );
    assert_eq!(
        client
            .read_sql("SELECT COUNT(*) AS n FROM axton_delivery_progress", &[])
            .unwrap()[0]["n"],
        json!(0)
    );
    let report: Value = json!({"measurement":"actualPostgresToNativeSqlite","n":head,"decodeMs":decoded_ms,"admissionMs":admission_ms,"maxAdmissionMs":max_admission_ms,"applyMs":apply_ms,"maxUnitApplyMs":max_apply_ms,"committedUnits":applied_units,"peakRetainedPartBytes":peak_payload,"queuePlansAfter":queue.len(),"durableProgressAfter":0,"fragments":measured});
    fs::write(
        directory.join("client-measurement.json"),
        serde_json::to_vec(&report).unwrap(),
    )
    .unwrap();
    println!("{report}");
}
