/*
Copyright 2013 Google Inc.

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

// Package consistenthash provides an implementation of a ring hash.
package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type Hash func(data []byte) uint32

type Map struct {
	hash     Hash
	replica  int
	keys     []int // Sorted
	hashMap  map[int]string
	hashs    map[string][]int
	replicas map[string]int
}

func New(replicas int, fn Hash) *Map {
	m := &Map{
		replica:  replicas,
		hash:     fn,
		replicas: make(map[string]int),
		hashs:    make(map[string][]int),
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Returns true if there are no items available.
func (m *Map) IsEmpty() bool {
	return len(m.replicas) == 0
}

// Adds some keys to the hash.
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		m.AddWithWeight(key, m.replica)
	}
}

// Adds a key with different replica to the hash.
func (m *Map) AddWithWeight(key string, replica int) {
	if replica < 1 {
		panic("replica should be positive")
	}
	old := m.replicas[key]
	if old != replica {
		m.keys = m.keys[:0]
		m.replicas[key] = replica
	}
}

// Remove a key from hash
func (m *Map) Remove(key string) {
	m.keys = m.keys[:0]
	delete(m.replicas, key)
	delete(m.hashs, key)
}

func (m *Map) calc(replicas map[string]int) {
	var total int
	for _, r := range replicas {
		total += r
	}
	m.keys = m.keys[:0]
	for key, r := range replicas {
		hs := m.hashs[key]
		for i := 0; i < r; i++ {
			var hash int
			if i < len(hs) {
				hash = hs[i]
			} else {
				hash = int(m.hash([]byte(strconv.Itoa(i) + key)))
				hs = append(hs, hash)
			}
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
		m.hashs[key] = hs
	}
	sort.Ints(m.keys)
}

// adjust the replica for keys
func (m *Map) adjust(tries int, scale float64) {
	if len(m.keys) != 0 {
		return
	}
	m.calc(m.replicas)
	if len(m.replicas) <= 1 || m.replica < 10 {
		return
	}
	var replicas int
	reps := make(map[string]int, len(m.replicas))
	for k, r := range m.replicas {
		reps[k] = r
		replicas += r
	}

	for t := 0; t < tries; t++ {
		stat := make(map[string]int, len(m.replicas))
		stat[m.hashMap[m.keys[0]]] = m.keys[0] + int(1<<32) - m.keys[len(m.keys)-1]
		for i, h := range m.keys[1:] {
			stat[m.hashMap[h]] += h - m.keys[i]
		}
		var changed bool
		for k, v := range stat {
			actual := float64(v) / float64(1<<32)
			rep := reps[k]
			expect := float64(m.replicas[k]) / float64(replicas)
			adjust := int(float64(rep) * (expect - actual) / float64(expect) * scale)
			if adjust > 1 || adjust < 1 {
				reps[k] += adjust
				changed = true
			}
		}
		if !changed {
			return
		}
		m.calc(reps)
	}
}

// Gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}
	m.adjust(5, 0.75)

	hash := int(m.hash([]byte(key)))

	// Binary search for appropriate replica.
	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })

	// Means we have cycled back to the first replica.
	if idx == len(m.keys) {
		idx = 0
	}

	return m.hashMap[m.keys[idx]]
}
