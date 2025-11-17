package main

import (
	"pulsardb/config/application"
)

func main() {
	appConfig, err := applicationConfig.LoadConfig("application", "config/application/base", "config/application/profiles")
	if err != nil {
		panic(err)
	}

	println(appConfig.Meta.Profile)

}
