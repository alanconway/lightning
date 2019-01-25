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

package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/alanconway/lightning/internal/pkg/httptest"
	"github.com/alanconway/lightning/internal/pkg/mqtttest"
	"github.com/alanconway/lightning/internal/pkg/test"
	"github.com/alanconway/lightning/pkg/lightning"
)

func lightningServer(arg ...string) *exec.Cmd {
	cmd := exec.Command("go", append([]string{"run", "server.go"}, arg...)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd
}

func killWait(cmd *exec.Cmd) {
	// TODO aconway 2019-01-15: clean shutdown with os.Interrupt.
	// Need to run server executable directly, not via 'go run'
	cmd.Process.Signal(os.Kill)
	cmd.Wait()
}

func TestMQTTtoHTTP(tt *testing.T) {
	n := 10
	t := test.New(tt)

	mqtt := mqtttest.StartMQTTBroker(t)
	defer mqtt.Stop()

	hc := make(httptest.HTTPChan, n)
	http := httptest.StartHTTPServer(t, hc)
	defer http.Stop()

	cmd := lightningServer(
		"-sink", fmt.Sprintf(`{"binding":"http", "url": "%s"}`, "http://"+http.Addr),
		"-source", fmt.Sprintf(`{"binding":"mqtt", "url": "%s", "filters":{ "foo": 1 }}`, "//"+mqtt.Addr))
	t.RequireNil(cmd.Start())
	defer killWait(cmd)

	mc, err := mqtttest.NewMQTTClient(&url.URL{Host: mqtt.Addr})
	t.RequireNil(err)

	// Retry sending till the source is subscribed and we get a request from the sink
	var req *httptest.HTTPRequest
	for req == nil {
		mc.Send(lightning.Event{"specversion": "2.0", "data": "hello"}, "foo", 1)
		select {
		case req = <-hc:
			var e lightning.Event
			t.ExpectNil(json.Unmarshal(req.Body, &e))
			d, err := e.DataValue()
			t.ExpectNil(err)
			t.ExpectEqual("hello", d)
		case <-time.After(time.Second):
		}
	}
}
