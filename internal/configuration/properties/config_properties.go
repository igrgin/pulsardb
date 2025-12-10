package properties

type ApplicationConfigProperties struct {
	Profile  string `yaml:"profile"`
	LogLevel string `yaml:"log-level"`
}

type CommandConfigProperties struct {
	QueueSize int `yaml:"queue-size"`
}

type RaftConfigProperties struct {
	NodeId         uint64            `yaml:"node-id"`
	Peers          map[uint64]string `yaml:"peers"`
	StorageBaseDir string            `yaml:"storage-base-dir"`
	TickInterval   uint64            `yaml:"tick-interval"`
	SnapCount      uint64            `yaml:"snap-count"`
	Timeout        uint64            `yaml:"timeout"`
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
	Command     CommandConfigProperties     `yaml:"command"`
	Raft        RaftConfigProperties        `yaml:"raft"`
}
