package configuration

type ConfigProvider interface {
	GetApplication() *AppConfigurationProperties
	GetTransport() *TransportConfigurationProperties
	GetRaft() *RaftConfigurationProperties
}

type AppConfigProvider struct {
	config *Properties
}

func NewProvider(cfg *Properties) *AppConfigProvider {
	return &AppConfigProvider{config: cfg}
}

func (c *AppConfigProvider) GetApplication() *AppConfigurationProperties {
	return &c.config.App
}

func (c *AppConfigProvider) GetTransport() *TransportConfigurationProperties {
	return &c.config.Transport
}

func (c *AppConfigProvider) GetRaft() *RaftConfigurationProperties {
	return &c.config.Raft
}
