use mercek::config::AppConfig;

#[tokio::test]
#[ignore = "set TEST_DATABASE_DSN to run"]
async fn integration_config_dsn_available() {
    let mut cfg = AppConfig::default();
    if let Ok(dsn) = std::env::var("TEST_DATABASE_DSN") {
        cfg.db.dsn = dsn;
    }
    assert!(!cfg.db.dsn.is_empty());
}
