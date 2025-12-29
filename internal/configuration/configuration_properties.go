package configuration

import (
	"time"
)

type Properties struct {
	App       AppConfigurationProperties       `yaml:"app"`
	Transport TransportConfigurationProperties `yaml:"transport"`
	Raft      RaftConfigurationProperties      `yaml:"raft"`
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
	Timeout               uint64                          `yaml:"timeout"`
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
	NodeID        uint64                  `yaml:"node-id"`
	RaftPeers     map[uint64]string       `yaml:"raft-peers"`
	ClientPeers   map[uint64]string       `yaml:"client-peers"`
	StorageDir    string                  `yaml:"store-dir"`
	TickInterval  time.Duration           `yaml:"tick-interval"`
	Timeout       uint64                  `yaml:"timeout"`
	SnapCount     uint64                  `yaml:"snap-count"`
	BatchSize     int                     `yaml:"batch-size"`
	BatchMaxWait  time.Duration           `yaml:"batch-max-wait"`
	ElectionTick  int                     `yaml:"election-tick"`
	HeartbeatTick int                     `yaml:"heartbeat-tick"`
	MaxSizePerMsg uint64                  `yaml:"max-size-per-msg"`
	MaxInflight   int                     `yaml:"max-inflight"`
	SendQueueSize int                     `yaml:"send-queue-size"`
	StepInboxSize uint64                  `yaml:"step-inbox-size"`
	Join          bool                    `yaml:"join"`
	Etcd          EtcdConfigProperties    `yaml:"etcd"`
	Wal           WriteAheadLogProperties `yaml:"wal"`
}

func (c *TransportConfigurationProperties) RaftAddr() string {
	return c.Address + ":" + c.RaftPort
}

func (c *TransportConfigurationProperties) ClientAddr() string {
	return c.Address + ":" + c.ClientPort
}

func (c *RaftConfigurationProperties) BatchTimeout() time.Duration {
	return c.BatchMaxWait * time.Millisecond
}

func (c *RaftConfigurationProperties) TickDuration() time.Duration {
	return c.TickInterval * time.Millisecond
}
