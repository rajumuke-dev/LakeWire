# CLI

Shaded jar classifier `bin` (A61). Launchers `bin/lakewire` and `bin/lakewire.cmd`.

```text
lakewire --help
lakewire dev [--home DIR] [--port 9092]
lakewire broker
lakewire worker
lakewire topics create orders -p 12 --retention 7d --bootstrap localhost:9092
lakewire topics list|describe|delete …
lakewire cost --gib-per-day 500 --zones 3 --retention 7d
lakewire verify --nemesis kill,pause --duration 3m --seed 42
lakewire inspect object <key | s3://bucket/key | path>
```

`inspect` decodes LKW1 without Postgres and prints one row per RecordBatch (base offset, max timestamp, absolute byte start, length). `verify` is the M9 harness.
