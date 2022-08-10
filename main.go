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

	warnConsumer := app.New(cfg, &app.Params{
		Queue:    "WARN",
		RouteKey: []string{"warn_key"},
	})

	errorConsumer := app.New(cfg, &app.Params{
		Queue:    "ERROR",
		RouteKey: []string{"error_key"},
	})

	debugConsumer := app.New(cfg, &app.Params{
		Queue:    "DEBUG",
		RouteKey: []string{"debug_key"},
	})

	allConsumer := app.New(cfg, &app.Params{
		Queue:    "WARN",
		RouteKey: []string{"debug_key", "error_key", "warn_key"},
	})

	grace := make(chan os.Signal, 1)
	signal.Notify(grace, os.Interrupt)

	logrus.Info("Consumer started! To exit press Ctrl+C!")
	<-grace

	warnConsumer.Close()
	errorConsumer.Close()
	debugConsumer.Close()
	allConsumer.Close()

	logrus.Info("Gracefully closed!")
}
