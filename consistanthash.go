package main

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type HashFn func(data []byte) uint32

type Ring struct {
	vnodes int
	hashFn HashFn

	mux   sync.RWMutex
	elems map[int]string
	keys  []int
}

func New(vnodes int, fn HashFn) *Ring {
	ring := Ring{
		vnodes: vnodes,
		elems:  make(map[int]string),
	}

	if fn != nil {
		ring.hashFn = fn
	} else {
		ring.hashFn = crc32.ChecksumIEEE
	}
	return &ring
}

func (r *Ring) hash(key string, vnode int) int {
	return int(r.hashFn([]byte(fmt.Sprintf(key + "_" + strconv.Itoa(vnode)))))
}

func (r *Ring) ghash(key string) int {
	return int(r.hashFn([]byte(key)))
}

func (r *Ring) Add(keys ...string) {
	r.mux.Lock()
	defer r.mux.Unlock()
	for _, key := range keys {
		for vnode := 0; vnode < r.vnodes; vnode++ {
			hkey := r.hash(key, vnode)
			r.keys = append(r.keys, hkey)
			r.elems[hkey] = key
		}
	}
	sort.Ints(r.keys) //sort keys for faster search
}

func (r *Ring) Get(keys ...string) []string {
	r.mux.RLock()
	defer r.mux.RUnlock()
	retVal := []string{}
	for _, key := range keys {
		keyHash := r.ghash(key)
		idx := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= keyHash })

		// if we get last element than we know that we should start from 0
		// to form a ring
		if idx == len(r.keys) {
			idx = 0
		}
		retVal = append(retVal, r.elems[r.keys[idx]])
	}
	return retVal
}
