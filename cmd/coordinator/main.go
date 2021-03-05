package main

import (
	"log"

	distpow "example.org/cpsc416/a2"
)

func main() {
	var config distpow.CoordinatorConfig
	err := distpow.ReadJSONConfig("config/coordinator_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(config)

	coordinator := distpow.NewCoordinator(config)

	err = coordinator.InitializeRPCs()
	if err != nil {
		log.Fatal(err)
	}
}
