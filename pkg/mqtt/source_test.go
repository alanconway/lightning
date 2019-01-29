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

package mqtt

import (
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/alanconway/lightning/internal/pkg/mqtttest"
	"github.com/alanconway/lightning/internal/pkg/test"
	"github.com/alanconway/lightning/pkg/lightning"
	paho "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
)

type sender struct{ client paho.Client }

func newSender(u *url.URL) (*sender, error) {
	username := u.User.Username()
	password, _ := u.User.Password()
	opts := paho.NewClientOptions().SetClientID("test-sender").AddBroker(u.Host)
	opts.SetUsername(username).SetPassword(password)
	s := &sender{client: paho.NewClient(opts)}
	return s, waitErr(s.client.Connect())
}

func (s *sender) send(topic string, v interface{}, qos byte) error {
	b, err := lightning.JSONFormat.Marshal(lightning.Event{"data": v, "specversion": "0.2"})
	if err != nil {
		return err
	}
	return waitErr(s.client.Publish(topic, qos, true, b))
}

func TestSource(tt *testing.T) {
	t := test.New(tt)
	broker := mqtttest.StartMQTTBroker(t)
	defer broker.Stop()

	topics := map[string]byte{"foo": 0, "bar": 1}
	sc := SourceConfig{Filters: topics, Capacity: 100, ClientID: os.Args[0]}
	sc.URL.URL = &url.URL{Host: broker.Addr, User: url.UserPassword("testuser", "testpassword")}
	source, err := NewSource(&sc, zap.NewNop())
	t.RequireNil(err)

	s, err := newSender(sc.URL.URL)
	t.RequireEqual(nil, err)

	// Initial messages to foo will be lost until MQTT source is subscribed
	incoming := make(chan lightning.Message)
	go func() {
		m, err := source.Receive()
		t.RequireNil(err)
		incoming <- m
	}()
	var msg lightning.Message
	for msg == nil {
		t.RequireNil(s.send("foo", "hello", 0))
		select {
		case msg = <-incoming:
			e, err := msg.Event()
			t.ExpectNil(err)
			t.ExpectEqual("hello", e["data"])
		case <-time.After(time.Second / 10):
		}
	}
	sent := []float64{0, 1, 2, 3, 4, 5} // Json decoder assumes all numbers are float64
	for _, v := range sent {
		t.RequireNil(s.send("bar", v, 1))
	}
	var received []float64
	for range sent {
		m, err := source.Receive()
		t.ExpectNil(err)
		e, err := m.Event()
		t.ExpectNil(err)
		if e["data"] != "hello" { // Ignore surplus "hello" messages
			received = append(received, e["data"].(float64))
		}
	}
	t.ExpectEqual(sent, received)
}
