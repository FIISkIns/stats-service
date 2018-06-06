package main

import "github.com/kelseyhightower/envconfig"

type ConfigurationSpec struct {
	Port                     int    `default:"8002"`
	CourseProgressServiceUrl string `default:"http://127.0.0.1:8003" envconfig:"PROGRESS_SERVICE_URL"`
	DatabaseUrl              string `default:"stats-service:mysqlpassword@tcp(127.0.0.1:3306)/statsdb"`
}

var config ConfigurationSpec

func initConfig() {
	envconfig.MustProcess("stats", &config)
}
