/*
Copyright 2023 Nokia.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package slice

import "sync"

type RecordSliceEntryFn func(any)

type Slice interface {
	Add(v any)
	Get() []any
	Length() int
}

func New() Slice {
	return &slice{
		d: make([]any, 0),
	}
}

type slice struct {
	m sync.RWMutex
	d []any
}

func (r *slice) Add(v any) {
	r.m.Lock()
	defer r.m.Unlock()
	r.d = append(r.d, v)
}

func (r *slice) Get() []any {
	r.m.RLock()
	defer r.m.RUnlock()
	return r.d
}

func (r *slice) Length() int {
	r.m.RLock()
	defer r.m.RUnlock()
	return len(r.d)
}
