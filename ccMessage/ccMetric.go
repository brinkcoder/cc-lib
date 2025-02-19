// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-lib.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package ccmessage

import (
	"reflect"
	"time"
)

func NewMetric(name string,
	tags map[string]string,
	meta map[string]string,
	value interface{},
	tm time.Time,
) (CCMessage, error) {
	return NewMessage(name, tags, meta, map[string]interface{}{"value": value}, tm)
}

func (m *ccMessage) IsMetric() bool {
	if v, ok := m.GetField("value"); ok {
		if reflect.TypeOf(v) != reflect.TypeOf("string") {
			return true
		}
	}
	return false
}

func (m *ccMessage) GetMetricValue() interface{} {
	if m.IsMetric() {
		if v, ok := m.GetField("value"); ok {
			return v
		}
	}
	return nil
}
