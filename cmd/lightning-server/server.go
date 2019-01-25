/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

/*
lightning-server is a multi-binding cloud-event adapter server.

It receives cloud events from a source and forwards them to a sink,
and can mix HTTP, MQTT and AMQP sources and sinks.

Run lightning-server -h for details of usage
*/
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/alanconway/lightning/pkg/amqp"
	"github.com/alanconway/lightning/pkg/http"
	"github.com/alanconway/lightning/pkg/lightning"
	"github.com/alanconway/lightning/pkg/mqtt"
	"go.uber.org/zap"
)

// Load known bindings
var bindings = lightning.NewBindings(
	mqtt.NewBinding(), http.NewBinding(), amqp.NewBinding())

var source = flag.String("source", "", "Source configuration: filename, URL or JSON")
var sink = flag.String("sink", "", "Sink configuration: filename, URL or JSON")
var logLevel = flag.String("log", "info", "debug, info, warn, error, critical")

func usage() {
	fmt.Fprintf(os.Stderr, `
Usage: %v [flags]

Listen for cloud events on -source, forward them to -sink.
If -source or -sink flags are missing, use environment variables
LIGHTNING_SOURCE and LIGHTNING_SINK.

Known bindings: %v

Flags:
`, os.Args[0], strings.Join(bindings.Names(), ", "))
	flag.PrintDefaults()

	for _, n := range bindings.Names() {
		fmt.Println(bindings.Doc(n))
	}
	os.Exit(1)
}

func config(flag, env string, value *string) lightning.Config {
	if *value == "" {
		if *value = os.Getenv(env); *value == "" {
			log.Printf("no flag %v or environment variable %v", flag, env)
			usage()
		}
	}
	c, err := lightning.MakeConfig(*value)
	if err != nil {
		log.Fatalf("Configuration error: %v", err)
	}
	return c
}

func makeLogger() (logger *zap.Logger) {
	zc := zap.NewDevelopmentConfig()
	err := zc.Level.UnmarshalText([]byte(*logLevel))
	if err == nil {
		logger, err = zc.Build()
	}
	if err != nil {
		log.Fatalf("Cannot create logger: %v", err)
	}
	return
}

func main() {
	// Use log package for program usage errors only.
	_, exe := filepath.Split(os.Args[0])
	log.SetFlags(0)
	log.SetPrefix(fmt.Sprintf("%v: ", exe))

	flag.Usage = usage
	flag.Parse()

	if len(flag.Args()) > 0 {
		log.Printf("unexpected arguments: %v", strings.Join(flag.Args(), " "))
		usage()
	}

	bindings.SetLogger(makeLogger())
	defer bindings.Log.Sync()

	a, err := bindings.Adapter(
		config("-source", "LIGHTNING_SOURCE", source),
		config("-sink", "LIGHTNING_SINK", sink),
	)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		s := <-sig
		bindings.Log.Debug("Received signal " + s.String())
		a.Close()
	}()

	if err = a.Run(); err != nil {
		log.Fatal("Run error: ", err)
	}
}
