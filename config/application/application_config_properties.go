package applicationConfig

type MetaConfig struct {
	Profile string `yaml:"profile"`
}

type ServerConfig struct {
	Network string `yaml:"network"`
	Port    string `yaml:"port"`
}

type Config struct {
	Meta   MetaConfig   `yaml:"meta"`
	Server ServerConfig `yaml:"server"`
}
