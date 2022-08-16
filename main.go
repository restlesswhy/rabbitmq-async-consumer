package main

import (
	"os"
	"os/signal"

	"rabbit/app"
	"rabbit/config"

	"github.com/sirupsen/logrus"
)

func main() {
	cfg := config.Load()

	consumer := app.New(cfg, &app.Params{
		Exchange:  "logs",
		RouteKeys: []string{"warn_key"},
	})

	grace := make(chan os.Signal, 1)
	signal.Notify(grace, os.Interrupt)

	logrus.Info("Consumer started! To exit press Ctrl+C!")
	<-grace

	if err := consumer.Close(); err != nil {
		logrus.Error(err)
	}

	logrus.Info("Gracefully closed!")
}
