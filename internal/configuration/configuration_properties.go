package configuration

import (
	"net"
	"strconv"
	"strings"
	"time"
)

type Properties struct {
	App       AppConfigurationProperties       `yaml:"app"`
	Transport TransportConfigurationProperties `yaml:"transport"`
	Raft      RaftConfigurationProperties      `yaml:"raft"`
	Metrics   MetricsConfigurationProperties   `yaml:"metrics"`
}

type AppConfigurationProperties struct {
	Profile        string `yaml:"profile"`
	LogLevel       string `yaml:"log-level"`
	QuorumWaitTime int    `yaml:"quorum-wait-time"`
}

type RaftTransportConfigProperties struct {
	NumStreamWorkers     uint32 `yaml:"num-stream-workers"`
	MaxConcurrentStreams uint32 `yaml:"max-concurrent-streams"`
}

type ClientTransportConfigProperties struct {
	NumStreamWorkers     uint32 `yaml:"num-stream-workers"`
	MaxConcurrentStreams uint32 `yaml:"max-concurrent-streams"`
}

type TransportConfigurationProperties struct {
	Address               string                          `yaml:"address"`
	ClientPort            string                          `yaml:"client-port"`
	RaftPort              string                          `yaml:"raft-port"`
	Network               string                          `yaml:"network"`
	RaftTransportConfig   RaftTransportConfigProperties   `yaml:"raft"`
	ClientTransportConfig ClientTransportConfigProperties `yaml:"client"`
}

type EtcdConfigProperties struct {
	ElectionTick              int    `yaml:"election-tick"`
	HeartbeatTick             int    `yaml:"heartbeat-tick"`
	MaxSizePerMsg             uint64 `yaml:"max-size-per-msg"`
	MaxInflightMsgs           int    `yaml:"max-inflight-msgs"`
	MaxUncommittedEntriesSize uint64 `yaml:"max-uncommitted-entries-size"`
}

type WriteAheadLogProperties struct {
	NoSync bool `yaml:"no-sync"`
}

type RaftConfigurationProperties struct {
	NodeID                 uint64                  `yaml:"node-id"`
	RaftPeers              map[uint64]string       `yaml:"raft-peers"`
	RaftPeersRaw           string                  `yaml:"raft-peers-env"` // "2=ip:port,3=ip:port"
	StorageDir             string                  `yaml:"storage-dir"`
	TickInterval           time.Duration           `yaml:"tick-interval"`
	Timeout                time.Duration           `yaml:"timeout"`
	SnapCount              uint64                  `yaml:"snap-count"`
	BatchSize              int                     `yaml:"batch-size"`
	BatchMaxWait           time.Duration           `yaml:"batch-max-wait"`
	ElectionTick           int                     `yaml:"election-tick"`
	HeartbeatTick          int                     `yaml:"heartbeat-tick"`
	MaxSizePerMsg          uint64                  `yaml:"max-size-per-msg"`
	MaxInflight            int                     `yaml:"max-inflight"`
	SendQueueSize          int                     `yaml:"send-queue-size"`
	StepInboxSize          uint64                  `yaml:"step-inbox-size"`
	Etcd                   EtcdConfigProperties    `yaml:"etcd"`
	Wal                    WriteAheadLogProperties `yaml:"wal"`
	Join                   bool                    `yaml:"join"`
	PromotionThreshold     uint64                  `yaml:"promotion-threshold"`
	PromotionCheckInterval time.Duration           `yaml:"promotion-check-interval"`
	ServiceDrainTimeout    time.Duration           `yaml:"service-drain-timeout"`
	NodeDrainTimeout       time.Duration           `yaml:"node-drain-timeout"`
}

type MetricsConfigurationProperties struct {
	Enabled bool   `yaml:"enabled"`
	Address string `yaml:"address"`
	Port    string `yaml:"port"`
}

func (m *MetricsConfigurationProperties) Addr() string {
	return net.JoinHostPort(m.Address, m.Port)
}

func (c *TransportConfigurationProperties) RaftAddr() string {
	return c.Address + ":" + c.RaftPort
}

func ParsePeers(s string) map[uint64]string {
	peers := make(map[uint64]string)
	if s == "" {
		return peers
	}
	for _, pair := range strings.Split(s, ",") {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) != 2 {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil {
			continue
		}
		peers[id] = strings.TrimSpace(parts[1])
	}
	return peers
}

func (c *RaftConfigurationProperties) MergePeersFromEnv() {
	if c.RaftPeers == nil {
		c.RaftPeers = make(map[uint64]string)
	}

	for id, addr := range ParsePeers(c.RaftPeersRaw) {
		c.RaftPeers[id] = addr
	}
}
