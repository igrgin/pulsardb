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
}

type TransportConfigProperties struct {
	Network    string `yaml:"network"`
	Address    string `yaml:"address"`
	ClientPort string `yaml:"client-port"`
	RaftPort   string `yaml:"raft-port"`
	Timeout    int    `yaml:"timeout"`
}

type Config struct {
	Application ApplicationConfigProperties `yaml:"app"`
	Transport   TransportConfigProperties   `yaml:"transport"`
	Command     CommandConfigProperties     `yaml:"command"`
	Raft        RaftConfigProperties        `yaml:"raft"`
}
