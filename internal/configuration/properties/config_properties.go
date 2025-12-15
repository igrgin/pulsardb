package properties

type ApplicationConfigProperties struct {
	Profile        string `yaml:"profile"`
	LogLevel       string `yaml:"log-level"`
	QuorumWaitTime uint64 `yaml:"quorum-wait-time"`
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

type RaftConfigProperties struct {
	NodeId         uint64                  `yaml:"node-id"`
	RaftPeers      map[uint64]string       `yaml:"raft-peers"`
	ClientPeers    map[uint64]string       `yaml:"client-peers"`
	StorageBaseDir string                  `yaml:"storage-base-dir"`
	TickInterval   uint64                  `yaml:"tick-interval"`
	SnapCount      uint64                  `yaml:"snap-count"`
	Timeout        uint64                  `yaml:"timeout"`
	StepInboxSize  uint64                  `yaml:"step-inbox-size"`
	Etcd           EtcdConfigProperties    `yaml:"etcd"`
	BatchSize      int                     `yaml:"batch-size"`
	BatchMaxWait   uint64                  `yaml:"batch-max-wait"`
	Wal            WriteAheadLogProperties `yaml:"wal"`
}

type TransportConfigProperties struct {
	Network              string `yaml:"network"`
	Address              string `yaml:"address"`
	ClientPort           string `yaml:"client-port"`
	RaftPort             string `yaml:"raft-port"`
	Timeout              uint64 `yaml:"timeout"`
	MaxConcurrentStreams uint32 `yaml:"max-concurrent-streams"`
}

type Config struct {
	Application ApplicationConfigProperties `yaml:"app"`
	Transport   TransportConfigProperties   `yaml:"transport"`
	Raft        RaftConfigProperties        `yaml:"raft"`
}
