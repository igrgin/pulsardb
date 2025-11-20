package loader

import (
	"log/slog"
	"pulsardb/utility/logging"
)

func (cl *ConfigLoader) initializeLogger() error {
	cl.logger = logging.NewLogger(cl.loglevel)
	slog.SetDefault(cl.logger)
	return nil
}
