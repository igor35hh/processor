package main

import (
	"flag"

	"github.com/igor35hh/processor/config"
	"github.com/igor35hh/processor/internal/app"
)

func main() {
	flag.Parse()
	cfg := config.NewConfig()
	app.NewApp(cfg).Run()
}
