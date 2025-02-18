// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-lib.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package ccmessage

import (
	"reflect"
	"time"
)

func NewEvent(name string,
	tags map[string]string,
	meta map[string]string,
	event string,
	tm time.Time,
) (CCMessage, error) {
	return NewMessage(name, tags, meta, map[string]interface{}{"event": event}, tm)
}

func (m *ccMessage) IsEvent() bool {
	if v, ok := m.GetField("event"); ok {
		if reflect.TypeOf(v) == reflect.TypeOf("string") {
			return true
		}
	}
	return false
}

func (m *ccMessage) GetEventValue() string {
	if m.IsEvent() {
		if v, ok := m.GetField("event"); ok {
			return v.(string)
		}
	}
	return ""
}
