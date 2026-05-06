// vitalz — host metrics → RabbitMQ snapshot daemon.
//
// Periodically samples this server's CPU/memory/swap/disk/network/load
// via gopsutil and publishes one JSON snapshot to RabbitMQ on every
// tick. Designed to be correlated with karotten container logs by
// timestamp window for capacity-planning analysis.
//
// Read-only against the kernel (gopsutil reads /proc and /sys). Does
// not touch Docker, processes, or any service besides the AMQP broker.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
)

const (
	defaultExchange        = "host-vitalz"
	defaultIntervalSeconds = 30
	publishBuffer          = 256
	timestampFmt           = time.RFC3339Nano
	amqpReconnectBackoff   = 2 * time.Second
)

// --- envelope ---

type envelope struct {
	Timestamp     string     `json:"timestamp"`
	UptimeSeconds uint64     `json:"uptime_seconds"`
	CPU           cpuStats   `json:"cpu"`
	Memory        memStats   `json:"memory"`
	Swap          swapStats  `json:"swap"`
	Disk          diskStats  `json:"disk"`
	Network       []netStats `json:"network"`
	Processes     procStats  `json:"processes"`
}

type cpuStats struct {
	Load1          float64   `json:"load_1"`
	Load5          float64   `json:"load_5"`
	Load15         float64   `json:"load_15"`
	PercentTotal   float64   `json:"percent_total"`
	PercentPerCore []float64 `json:"percent_per_core"`
}

type memStats struct {
	TotalBytes     uint64  `json:"total_bytes"`
	UsedBytes      uint64  `json:"used_bytes"`
	FreeBytes      uint64  `json:"free_bytes"`
	AvailableBytes uint64  `json:"available_bytes"`
	UsedPercent    float64 `json:"used_percent"`
}

type swapStats struct {
	TotalBytes  uint64  `json:"total_bytes"`
	UsedBytes   uint64  `json:"used_bytes"`
	UsedPercent float64 `json:"used_percent"`
}

type diskStats struct {
	Mounts []diskMount `json:"mounts"`
	IO     []diskIO    `json:"io"`
}

type diskMount struct {
	Mount       string  `json:"mount"`
	Fstype      string  `json:"fstype"`
	TotalBytes  uint64  `json:"total_bytes"`
	UsedBytes   uint64  `json:"used_bytes"`
	FreeBytes   uint64  `json:"free_bytes"`
	UsedPercent float64 `json:"used_percent"`
}

type diskIO struct {
	Device     string `json:"device"`
	ReadBytes  uint64 `json:"read_bytes"`
	WriteBytes uint64 `json:"write_bytes"`
	ReadCount  uint64 `json:"read_count"`
	WriteCount uint64 `json:"write_count"`
	IOTimeMS   uint64 `json:"io_time_ms"`
}

type netStats struct {
	Iface       string `json:"iface"`
	BytesSent   uint64 `json:"bytes_sent"`
	BytesRecv   uint64 `json:"bytes_recv"`
	PacketsSent uint64 `json:"packets_sent"`
	PacketsRecv uint64 `json:"packets_recv"`
	Errin       uint64 `json:"errin"`
	Errout      uint64 `json:"errout"`
}

type procStats struct {
	Total uint64 `json:"total"`
}

type message struct {
	routingKey string
	body       []byte
}

// --- main ---

func main() {
	rmqURL := mustEnv("RABBITMQ_URL")
	exchange := envDefault("EXCHANGE", defaultExchange)
	intervalSec := envIntDefault("INTERVAL_SECONDS", defaultIntervalSeconds)

	// Host identity is implicit in the vhost — the broker knows which
	// host this is (one vhost per host by convention). The envelope and
	// routing key both stay host-less; the consumer derives host from
	// the vhost it's bound to.
	const routingKey = "snapshot"

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	publishCh := make(chan message, publishBuffer)
	var dropped atomic.Uint64

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		runPublisher(ctx, rmqURL, exchange, publishCh)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runDropReporter(ctx, &dropped)
	}()

	// Warmup CPU sampling state so the first tick has a real delta to
	// report (cpu.Percent(0, ...) returns the % since the previous call;
	// without a previous call it returns 0).
	_, _ = cpu.PercentWithContext(ctx, 0, false)
	_, _ = cpu.PercentWithContext(ctx, 0, true)

	log.Printf("vitalz started: interval=%ds exchange=%q routing_key=%q",
		intervalSec, exchange, routingKey)

	tick := time.NewTicker(time.Duration(intervalSec) * time.Second)
	defer tick.Stop()

	// One immediate sample so the queue starts seeing data right away
	// instead of waiting `intervalSec` seconds.
	doSample(ctx, routingKey, publishCh, &dropped)

	for {
		select {
		case <-ctx.Done():
			log.Println("shutdown signal received")
			wg.Wait()
			return
		case <-tick.C:
			doSample(ctx, routingKey, publishCh, &dropped)
		}
	}
}

// --- sampling ---

func doSample(ctx context.Context, routingKey string, publishCh chan<- message, dropped *atomic.Uint64) {
	env := buildSnapshot(ctx)
	body, err := json.Marshal(env)
	if err != nil {
		log.Printf("marshal snapshot: %v", err)
		return
	}
	select {
	case publishCh <- message{routingKey: routingKey, body: body}:
	default:
		dropped.Add(1)
	}
}

func buildSnapshot(ctx context.Context) envelope {
	env := envelope{
		Timestamp: time.Now().UTC().Format(timestampFmt),
	}

	if info, err := host.InfoWithContext(ctx); err == nil {
		env.UptimeSeconds = info.Uptime
		env.Processes.Total = info.Procs
	} else {
		log.Printf("host.Info: %v", err)
	}

	if avg, err := load.AvgWithContext(ctx); err == nil {
		env.CPU.Load1 = avg.Load1
		env.CPU.Load5 = avg.Load5
		env.CPU.Load15 = avg.Load15
	} else {
		log.Printf("load.Avg: %v", err)
	}
	if perc, err := cpu.PercentWithContext(ctx, 0, false); err == nil && len(perc) > 0 {
		env.CPU.PercentTotal = perc[0]
	} else if err != nil {
		log.Printf("cpu.Percent total: %v", err)
	}
	if perc, err := cpu.PercentWithContext(ctx, 0, true); err == nil {
		env.CPU.PercentPerCore = perc
	} else {
		log.Printf("cpu.Percent per-core: %v", err)
	}

	if vmem, err := mem.VirtualMemoryWithContext(ctx); err == nil {
		env.Memory = memStats{
			TotalBytes:     vmem.Total,
			UsedBytes:      vmem.Used,
			FreeBytes:      vmem.Free,
			AvailableBytes: vmem.Available,
			UsedPercent:    vmem.UsedPercent,
		}
	} else {
		log.Printf("mem.VirtualMemory: %v", err)
	}

	if smem, err := mem.SwapMemoryWithContext(ctx); err == nil {
		env.Swap = swapStats{
			TotalBytes:  smem.Total,
			UsedBytes:   smem.Used,
			UsedPercent: smem.UsedPercent,
		}
	} else {
		log.Printf("mem.SwapMemory: %v", err)
	}

	if parts, err := disk.PartitionsWithContext(ctx, false); err == nil {
		for _, p := range parts {
			if !isRealMount(p.Mountpoint, p.Fstype) {
				continue
			}
			usage, uerr := disk.UsageWithContext(ctx, p.Mountpoint)
			if uerr != nil {
				log.Printf("disk.Usage(%s): %v", p.Mountpoint, uerr)
				continue
			}
			env.Disk.Mounts = append(env.Disk.Mounts, diskMount{
				Mount:       p.Mountpoint,
				Fstype:      p.Fstype,
				TotalBytes:  usage.Total,
				UsedBytes:   usage.Used,
				FreeBytes:   usage.Free,
				UsedPercent: usage.UsedPercent,
			})
		}
	} else {
		log.Printf("disk.Partitions: %v", err)
	}

	if iom, err := disk.IOCountersWithContext(ctx); err == nil {
		for dev, iostat := range iom {
			if !isRealBlockDevice(dev) {
				continue
			}
			env.Disk.IO = append(env.Disk.IO, diskIO{
				Device:     dev,
				ReadBytes:  iostat.ReadBytes,
				WriteBytes: iostat.WriteBytes,
				ReadCount:  iostat.ReadCount,
				WriteCount: iostat.WriteCount,
				IOTimeMS:   iostat.IoTime,
			})
		}
	} else {
		log.Printf("disk.IOCounters: %v", err)
	}

	if niostats, err := net.IOCountersWithContext(ctx, true); err == nil {
		for _, ni := range niostats {
			if !isRealInterface(ni.Name) {
				continue
			}
			env.Network = append(env.Network, netStats{
				Iface:       ni.Name,
				BytesSent:   ni.BytesSent,
				BytesRecv:   ni.BytesRecv,
				PacketsSent: ni.PacketsSent,
				PacketsRecv: ni.PacketsRecv,
				Errin:       ni.Errin,
				Errout:      ni.Errout,
			})
		}
	} else {
		log.Printf("net.IOCounters: %v", err)
	}

	return env
}

// --- selection rules (keep snapshot focused on real, useful entries) ---

func isRealMount(mountpoint, fstype string) bool {
	switch {
	case strings.HasPrefix(mountpoint, "/proc"),
		strings.HasPrefix(mountpoint, "/sys"),
		strings.HasPrefix(mountpoint, "/run"),
		strings.HasPrefix(mountpoint, "/dev"),
		strings.HasPrefix(mountpoint, "/var/lib/docker/overlay"),
		strings.HasPrefix(mountpoint, "/snap"):
		return false
	}
	switch fstype {
	case "tmpfs", "devtmpfs", "overlay", "squashfs", "fuse.snapfuse",
		"binfmt_misc", "autofs", "configfs", "debugfs", "fusectl",
		"hugetlbfs", "mqueue", "pstore", "ramfs", "rpc_pipefs",
		"securityfs", "selinuxfs", "sysfs", "tracefs", "cgroup",
		"cgroup2", "nsfs", "bpf":
		return false
	}
	return true
}

func isRealBlockDevice(name string) bool {
	if strings.HasPrefix(name, "loop") ||
		strings.HasPrefix(name, "ram") ||
		strings.HasPrefix(name, "dm-") {
		return false
	}
	return true
}

func isRealInterface(name string) bool {
	if name == "lo" {
		return false
	}
	if strings.HasPrefix(name, "docker") ||
		strings.HasPrefix(name, "br-") ||
		strings.HasPrefix(name, "veth") ||
		strings.HasPrefix(name, "cni") ||
		strings.HasPrefix(name, "flannel") {
		return false
	}
	return true
}

// --- AMQP publisher (mirrors the karotten pattern) ---

func runPublisher(ctx context.Context, url, exchange string, ch <-chan message) {
	for ctx.Err() == nil {
		conn, amqpCh, err := dialAMQP(url, exchange)
		if err != nil {
			log.Printf("amqp dial: %v; retrying in %v", err, amqpReconnectBackoff)
			select {
			case <-time.After(amqpReconnectBackoff):
			case <-ctx.Done():
				return
			}
			continue
		}
		log.Printf("amqp connected; publishing to exchange %q", exchange)
		publishLoop(ctx, exchange, amqpCh, ch)
		_ = amqpCh.Close()
		_ = conn.Close()
	}
}

func dialAMQP(url, exchange string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.DialConfig(url, amqp.Config{
		Heartbeat: 30 * time.Second,
		Locale:    "en_US",
	})
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if err := ch.ExchangeDeclarePassive(exchange, "topic", true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, fmt.Errorf("exchange %q passive declare: %w", exchange, err)
	}
	return conn, ch, nil
}

func publishLoop(ctx context.Context, exchange string, amqpCh *amqp.Channel, ch <-chan message) {
	closeNotify := amqpCh.NotifyClose(make(chan *amqp.Error, 1))
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-closeNotify:
			log.Printf("amqp channel closed: %v", err)
			return
		case m, ok := <-ch:
			if !ok {
				return
			}
			err := amqpCh.PublishWithContext(ctx, exchange, m.routingKey, false, false, amqp.Publishing{
				ContentType:  "application/json",
				Body:         m.body,
				DeliveryMode: amqp.Transient,
				Timestamp:    time.Now().UTC(),
			})
			if err != nil {
				log.Printf("amqp publish: %v", err)
				return
			}
		}
	}
}

// --- drop reporter ---

func runDropReporter(ctx context.Context, dropped *atomic.Uint64) {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	var last uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cur := dropped.Load()
			if cur != last {
				log.Printf("dropped snapshots (broker disconnected): +%d in last 60s, total %d", cur-last, cur)
				last = cur
			}
		}
	}
}

// --- env helpers ---

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("env %s required", k)
	}
	return v
}

func envDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envIntDefault(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		log.Printf("env %s=%q invalid; using default %d", k, v, def)
		return def
	}
	return n
}
