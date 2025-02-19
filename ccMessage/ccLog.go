// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-lib.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package ccmessage

import (
	"reflect"
	"time"
)

func NewLog(name string,
	tags map[string]string,
	meta map[string]string,
	log string,
	tm time.Time,
) (CCMessage, error) {
	return NewMessage(name, tags, meta, map[string]interface{}{"log": log}, tm)
}

func (m *ccMessage) IsLog() bool {
	if v, ok := m.GetField("log"); ok {
		if reflect.TypeOf(v) == reflect.TypeOf("string") {
			return true
		}
	}
	return false
}

func (m *ccMessage) GetLogValue() string {
	if m.IsLog() {
		if v, ok := m.GetField("log"); ok {
			return v.(string)
		}
	}
	return ""
}
