package properties

type MetaConfig struct {
	ProfilesDir string `yaml:"profiles_dir"`
	Profile     string `yaml:"profile"`
}

type Config struct {
	Meta MetaConfig `yaml:"meta"`
}
