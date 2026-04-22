# mercek

Tokio tabanlı async syslog sunucusu. RFC3164 ve RFC5424 formatlarını best-effort parse eder, normalize edilmiş event modelini PostgreSQL'e deadpool + tokio-postgres ile yazar.

## Özellikler

- UDP / TCP / TLS syslog dinleyicileri
- RFC3164 + RFC5424 otomatik tespit ve parse
- Parse hatalarında raw mesajı koruyarak `valid=false` kayıt
- Bounded queue, `drop_newest` veya `backpressure` politikası
- Batch + transaction tabanlı PostgreSQL yazımı
- `app_name/tag=nginx` olan kayıtları nginx access log alanlarına parse edip `nginx_access_logs` tablosuna da yazma
- Retry + dead-letter loglama
- Partitioned tablo şeması (`received_at` RANGE)
- `/healthz`, `/readyz`, `/metrics` endpointleri
- Graceful shutdown

## DB oluşturma
```psql
postgres=# create user mercek_usr;
postgres=# \password mercek_usr 
Enter new password for user "mercek_usr": 
Enter it again: 
postgres=# create database mercek with owner mercek_usr;
```

## Çalıştırma

```bash
export MERCEK__DB__DSN="host=127.0.0.1 user=mercek_usr password=secret dbname=mercek"
cargo run
```

Varsayılanlar:

- UDP: `0.0.0.0:5514`
- TCP: `0.0.0.0:5514`
- TLS: `0.0.0.0:6514` (kapalı)
- HTTP metrics: `0.0.0.0:9000`

## Konfigürasyon (env)

Environment prefix: `MERCEK__`

Örnekler:

```bash
MERCEK__DB__DSN="host=127.0.0.1 user=mercek_usr password=secret dbname=mercek"
MERCEK__LISTENERS__ENABLE_TLS=true
MERCEK__LISTENERS__TLS_CERT_PATH="/etc/mercek/tls.crt"
MERCEK__LISTENERS__TLS_KEY_PATH="/etc/mercek/tls.key"
MERCEK__QUEUE__POLICY="drop_newest" # veya backpressure
MERCEK__BATCHING__BATCH_SIZE=1000
MERCEK__BATCHING__FLUSH_INTERVAL_MS=200
MERCEK__RETRY__ATTEMPTS=5
MERCEK__RETRY__BACKOFF_MS=250
MERCEK__LIMITS__MAX_MESSAGE_BYTES=65536
```

## Veritabanı

Şema: `sql/schema.sql`

- Ana tablo: `syslog_events`
- Nginx access log tablosu: `nginx_access_logs`
- Partition key: `received_at`
- İndeksler: `received_at`, `host`, `severity`, `app_name`

Otomatik bootstrap açıkken uygulama başlarken tablo ve geçerli ay partition'ı oluşturur.

## Testler

```bash
cargo test
```

- Parser unit testleri mevcut
- PostgreSQL entegrasyon testi `#[ignore]`; çalıştırmak için `TEST_DATABASE_DSN` ver

## Operasyon

- Dockerfile: `deploy/Dockerfile`
- systemd unit: `deploy/mercek.service`
- Retention/rotation için PostgreSQL partition cleanup cron/job önerilir (aylık partition drop/archival).
