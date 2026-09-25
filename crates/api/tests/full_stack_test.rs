use api::{build_state, drain_ingestion_once, routes};
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use base64::Engine;
use infra::config::Settings;
use serde_json::{json, Value};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

#[tokio::test]
async fn full_gateway_flow_against_a_real_postgres() {
    let container = Postgres::default().start().await.expect("postgres container starts");
    let port = container.get_host_port_ipv4(5432).await.expect("postgres port");
    let db_url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let db_pool = sqlx::postgres::PgPoolOptions::new()
        .connect(&db_url)
        .await
        .expect("connects to test postgres");
    sqlx::migrate!("../../migrations")
        .run(&db_pool)
        .await
        .expect("migrations run against test postgres");

    let settings = Settings {
        database_url: db_url,
        jwt_secret: "test-secret".into(),
        gossip_bind_addr: "127.0.0.1:0".into(),
        gossip_seeds: vec![],
        http_bind_addr: "127.0.0.1:0".into(),
    };

    let state = build_state(settings, db_pool).await.expect("state builds");
    let app = routes::build_router(state.clone());

    // 1. register a new user
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/auth/register")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "username": "alice", "password": "hunter42" }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let token = serde_json::from_slice::<Value>(&body).unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();

    // 2. registering the same username again is a conflict
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/auth/register")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "username": "alice", "password": "hunter42" }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CONFLICT);

    // 3. login with the wrong password is rejected
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "username": "alice", "password": "wrong" }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // 4. ingest with no Authorization header is rejected
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "stream_id": 1, "timestamp_ms": 500, "key": "k", "payload_b64": "" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // 5. authenticated ingest succeeds
    let payload_b64 = base64::engine::general_purpose::STANDARD.encode(5.0f64.to_le_bytes());
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingest")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(
                    json!({
                        "stream_id": 1,
                        "timestamp_ms": 500,
                        "key": "k",
                        "payload_b64": payload_b64,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // manually drain rather than racing a background task with a sleep
    let drained = drain_ingestion_once(&state).await;
    assert_eq!(drained, 1);

    // 6. query reflects the ingested event, correctly attributed to stream_id 1
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/query?stream_id=1")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let windows: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(windows[0]["stream_id"], 1);
    assert_eq!(windows[0]["sum"], 5.0);

    // 7. checkpoint persistence round-trips through real Postgres
    let stats_json = json!({ "count": 1, "sum": 5.0, "mean": 5.0, "min": 5.0, "max": 5.0 });
    state.checkpoint_repo.upsert(1, 0, 1000, &stats_json).await.expect("checkpoint upserts");

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/checkpoints?stream_id=1")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let checkpoints: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(checkpoints.as_array().unwrap().len(), 1);
    assert_eq!(checkpoints[0]["stream_id"], 1);

    // 8. cluster status is reachable and reports this node
    let res = app
        .oneshot(Request::builder().method("GET").uri("/cluster/status").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}