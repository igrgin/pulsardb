package properties

type MetaConfig struct {
	Profile string `yaml:"profile"`
}

type Config struct {
	Meta MetaConfig `yaml:"meta"`
}
