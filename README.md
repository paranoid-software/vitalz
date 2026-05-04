# vitalz

Docker-host vital signs → RabbitMQ. A small Go daemon that samples
this server's CPU / memory / swap / disk / network / load every N
seconds and publishes one JSON snapshot per tick to a RabbitMQ topic
exchange.

Designed to be **timestamp-correlated with container logs** (e.g.,
[karotten](https://github.com/paranoid-software/karotten)) so the
downstream consumer can answer questions like *"how much load can a
24 GB / 4 vCPU box like minion actually handle, based on observed
reality"* by joining vitalz snapshots and request logs by time
window.

Read-only against the kernel — gopsutil reads `/proc` and `/sys`.
No Docker socket, no process introspection beyond a count, no disk
writes from this daemon.

## Why a separate daemon and not just node_exporter

`node_exporter` exposes Prometheus metrics over HTTP — pull model,
needs Prometheus, separate stack. `vitalz` instead **pushes** the
same kind of host metrics into the same RabbitMQ broker the rest of
this stack already uses. One transport, one consumer pattern, no
extra moving parts.

It also lets the consumer correlate vitalz snapshots with log
events on the same broker by binding both `host-vitalz` and
`containers-logs` to a single capacity-analysis queue.

## Contract

- **Broker**: any RabbitMQ reachable from the host. Configured via env.
- **Exchange**: topic, must already exist (passive declare). Default
  name `host-vitalz`.
- **Routing key**: `<hostname>.snapshot`. Single host today; the
  shape supports multi-host setups subscribing `*.snapshot` for all
  hosts or `<host>.#` for one.
- **Body**: JSON envelope (one per tick). Cumulative-since-boot
  counters where applicable (Prometheus COUNTER convention) — the
  consumer subtracts consecutive snapshots to compute rates.

### Envelope shape

```json
{
  "timestamp": "2026-05-04T03:52:19.123Z",
  "host": "minion",
  "uptime_seconds": 6608943,
  "cpu": {
    "load_1": 0.08,
    "load_5": 0.11,
    "load_15": 0.09,
    "percent_total": 12.3,
    "percent_per_core": [10.0, 14.5, 8.0, 16.0]
  },
  "memory": {
    "total_bytes": 25145475072,
    "used_bytes": 3924738048,
    "free_bytes": 1116880896,
    "available_bytes": 20744826880,
    "used_percent": 15.6
  },
  "swap": {
    "total_bytes": 0,
    "used_bytes": 0,
    "used_percent": 0
  },
  "disk": {
    "mounts": [
      { "mount": "/", "fstype": "ext4", "total_bytes": 103859404800, "used_bytes": 42076848128, "free_bytes": 61765779456, "used_percent": 40.5 }
    ],
    "io": [
      { "device": "sda", "read_bytes": 128243305472, "write_bytes": 572505933824, "read_count": 1342051, "write_count": 29792093, "io_time_ms": 17721365 }
    ]
  },
  "network": [
    { "iface": "enp0s6", "bytes_sent": 16442975387, "bytes_recv": 41114315898, "packets_sent": 53549133, "packets_recv": 44393419, "errin": 0, "errout": 0 }
  ],
  "processes": { "total": 376 }
}
```

### Filtering rules

Kept deliberately narrow so the snapshot is small and meaningful for
capacity work:

- `disk.mounts`: real mounts only. Skips `/proc`, `/sys`, `/run`,
  `/dev`, `/var/lib/docker/overlay/*`, `/snap/*`, plus `tmpfs`,
  `overlay`, `squashfs`, etc. Real ext4/xfs/vfat mounts pass.
- `disk.io`: real block devices only. Skips `loop*`, `ram*`, `dm-*`.
- `network`: real NICs only. Skips `lo`, plus `docker*`, `br-*`,
  `veth*`, `cni*`, `flannel*` (counting docker bridges would inflate
  the totals).

## Build

```bash
go build -o vitalz .
```

Go 1.22 or newer.

## Configuration

| Var | Required | Default | Notes |
|---|---|---|---|
| `RABBITMQ_URL` | yes | — | e.g. `amqp://user:pass@host:5672/vhost` |
| `EXCHANGE` | no | `host-vitalz` | Must already exist on the broker as `topic`, durable. `vitalz` does a passive declare and won't create it. |
| `INTERVAL_SECONDS` | no | `30` | Tick frequency. |
| `HOSTNAME_OVERRIDE` | no | `os.Hostname()` | Override the host segment of the routing key (useful for testing or multi-instance setups on one box). |

## Deploying

Same pattern as karotten — operator writes their own systemd unit
on the host, the repo only ships the binary + a reference example.
A bare-bones unit:

```ini
# /etc/systemd/system/vitalz.service
[Unit]
Description=vitalz — host resource snapshots to RabbitMQ
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu                    # any user; no special perms needed
EnvironmentFile=/etc/default/vitalz
ExecStart=/usr/local/bin/vitalz
Restart=always
RestartSec=2
StandardOutput=journal
StandardError=journal
MemoryMax=64M
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true

[Install]
WantedBy=multi-user.target
```

with `/etc/default/vitalz` (`0640`, `root:<service-user>`):

```
RABBITMQ_URL=amqp://user:pass@broker:5672/vhost
EXCHANGE=host-vitalz
INTERVAL_SECONDS=30
```

Then:

```bash
sudo install -o root -g root -m 0755 ./vitalz /usr/local/bin/vitalz
sudo systemctl daemon-reload
sudo systemctl enable --now vitalz
```

## Resilience

- AMQP reconnect with backoff on broker drops. Snapshots queue up
  in a 256-msg in-memory buffer (≈ 2 hours at 30 s ticks); beyond
  that they are dropped, with a counter logged every 60 s.
- Each metric read is independent — if `disk.Usage(/some/mount)`
  fails (NFS hung, etc.) that mount is omitted from the snapshot
  and everything else still publishes.
- systemd `Restart=always`, `RestartSec=2` — any process crash gets
  restarted within two seconds.
- Memory cap (`MemoryMax=64M` in the example unit) is a guardrail;
  steady state is ~3 MB.

## What it does NOT do

- Does not declare or create queues. Consumer concern.
- Does not declare exchanges. Operator declares once at deploy
  time; `vitalz` does a *passive* declare and refuses to publish if
  the exchange is missing.
- Does not write anything to disk; only reads `/proc` + `/sys` and
  publishes over AMQP.
- Does not query Docker or any service besides the broker. Only
  kernel metrics.
- Does not do per-container or per-process metrics. Per-service
  correlation is meant to come from joining vitalz snapshots with
  container logs on the consumer side, not from labelling metrics
  inside this daemon.

## Operating

```bash
sudo systemctl status vitalz
sudo journalctl -u vitalz -f
sudo systemctl restart vitalz
```

To verify the broker is receiving publishes without binding a
permanent queue:

```bash
RMQ="docker exec <broker-container> rabbitmqadmin --vhost=<vhost> -u <user> -p <pass>"
$RMQ declare queue name=vitalz.tap durable=false
$RMQ declare binding source=host-vitalz destination=vitalz.tap routing_key="#"
sleep 35
$RMQ get queue=vitalz.tap count=1
$RMQ delete queue name=vitalz.tap
```

Should print one JSON snapshot matching the envelope above.
