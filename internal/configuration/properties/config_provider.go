package properties

type ConfigProvider interface {
	GetApplication() *ApplicationConfigProperties
	GetTransport() *TransportConfigProperties
	GetRaft() *RaftConfigProperties
}

type AppConfigProvider struct {
	config *Config
}

func NewProvider(cfg *Config) *AppConfigProvider {
	return &AppConfigProvider{config: cfg}
}

func (c *AppConfigProvider) GetApplication() *ApplicationConfigProperties {
	return &c.config.Application
}

func (c *AppConfigProvider) GetTransport() *TransportConfigProperties {
	return &c.config.Transport
}

func (c *AppConfigProvider) GetRaft() *RaftConfigProperties {
	return &c.config.Raft
}
