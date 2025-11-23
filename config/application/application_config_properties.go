package applicationConfig

type MetaConfig struct {
	Profile string `yaml:"profile"`
}

type ServerConfig struct {
	Network string `yaml:"network"`
	Port    string `yaml:"port"`
	Timeout int32  `yaml:"timeout"`
}

type DatabaseConfig struct {
	QueueSize int `yaml:"queue-size"`
}

type Config struct {
	Meta     MetaConfig     `yaml:"meta"`
	Server   ServerConfig   `yaml:"server"`
	Database DatabaseConfig `yaml:"database"`
}
