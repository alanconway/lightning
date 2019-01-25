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

package mqtttest

import (
	"context"
	"net"
	"net/url"

	"github.com/DrmagicE/gmqtt"
	"github.com/alanconway/lightning/internal/pkg/test"
	"github.com/alanconway/lightning/pkg/lightning"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type MQTTBroker struct {
	server *gmqtt.Server
	Addr   string
}

func init() {
	// NOTE to enable test broker logging, uncomment here
	// gmqtt.SetLogger(logger.NewLogger(os.Stdout, "[mqtt broker]", log.LstdFlags))
}

func StartMQTTBroker(t test.TB) *MQTTBroker {
	b := &MQTTBroker{server: gmqtt.NewServer()}
	l, err := net.Listen("tcp", ":0")
	t.RequireNil(err)
	b.server.AddTCPListenner(l)
	b.Addr = l.Addr().String()
	b.server.Run()
	return b
}

func (b *MQTTBroker) Stop() {
	b.server.Stop(context.Background())
}

type MQTTClient struct{ client mqtt.Client }

func NewMQTTClient(u *url.URL) (*MQTTClient, error) {
	username := u.User.Username()
	password, _ := u.User.Password()
	opts := mqtt.NewClientOptions().SetClientID("test-sender").AddBroker(u.Host)
	opts.SetUsername(username).SetPassword(password)
	s := &MQTTClient{client: mqtt.NewClient(opts)}
	return s, waitErr(s.client.Connect())
}

func (c *MQTTClient) Send(e lightning.Event, topic string, qos byte) error {
	b, err := lightning.JSONFormat.Marshal(e)
	if err != nil {
		return err
	}
	return waitErr(c.client.Publish(topic, qos, true, b))
}

func waitErr(t mqtt.Token) error { t.Wait(); return t.Error() }
