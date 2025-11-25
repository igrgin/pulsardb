package properties

type ApplicationConfigProperties struct {
	Profile  string `yaml:"profile"`
	LogLevel string `yaml:"log-level"`
}

type TransportConfigProperties struct {
	Network string `yaml:"network"`
	Address string `yaml:"address"`
	Port    string `yaml:"port"`
	Timeout int    `yaml:"timeout"`
}

type CommandConfigProperties struct {
	QueueSize int `yaml:"queue-size"`
}

type Config struct {
	Application ApplicationConfigProperties `yaml:"app"`
	Transport   TransportConfigProperties   `yaml:"transport"`
	Command     CommandConfigProperties     `yaml:"command"`
}
