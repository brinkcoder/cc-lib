// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-lib.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package ccconfig

import (
	"encoding/json"
	"sync"
	"testing"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	"github.com/ClusterCockpit/cc-lib/sinks"
)

type mainConfig struct {
	Interval string `json:"interval"`
}

func TestInit(t *testing.T) {
	cclog.Init("debug", true)
	fn := "./testdata/config.json"
	Init(fn)
	n := len(keys)
	if n != 4 {
		t.Errorf("Wrong number of config objects got: %d \nwant: 4", n)
	}

	rawConfig := GetPackageConfig("sinks")
	var sync sync.WaitGroup

	_, err := sinks.New(&sync, rawConfig)
	if err != nil {
		t.Errorf("Error in sink.New: %v ", err)
	}

	var mc mainConfig
	rawConfig = GetPackageConfig("main")
	err = json.Unmarshal(rawConfig, &mc)
	if err != nil {
		t.Errorf("Error in Unmarshal': %v ", err)
	}

	if mv := mc.Interval; mv != "10s" {
		t.Errorf("Wrong interval got: %s \nwant: 10s", mv)
	}
}

func TestInitAll(t *testing.T) {
	cclog.Init("debug", true)
	fn := "./testdata/configAll.json"
	Init(fn)
	n := len(keys)
	if n != 4 {
		t.Errorf("Wrong number of config objects got: %d \nwant: 4", n)
	}

	rawConfig := GetPackageConfig("sinks")
	var sync sync.WaitGroup

	_, err := sinks.New(&sync, rawConfig)
	if err != nil {
		t.Errorf("Error in sink.New: %v ", err)
	}

	var mc mainConfig
	rawConfig = GetPackageConfig("main")
	err = json.Unmarshal(rawConfig, &mc)
	if err != nil {
		t.Errorf("Error in Unmarshal': %v ", err)
	}

	if mv := mc.Interval; mv != "10s" {
		t.Errorf("Wrong interval got: %s \nwant: 10s", mv)
	}
}
