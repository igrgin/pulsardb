package main

import "pulsardb/application-config"

func main() {
	appConfig, err := applicationConfig.LoadConfig()
	if err != nil {
		panic(err)
	}

	println(appConfig.Meta.Profile)

}
