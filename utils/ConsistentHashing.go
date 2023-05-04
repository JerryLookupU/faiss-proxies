package utils

import (
	"hash/crc32"
	"sort"
	"sync"
)

const DEFAULT_HASH_RING_REPLICAS = 100

type HashRing []uint32

func NewHashRing() HashRing {
	return make(HashRing, 0)
}

func (this HashRing) Len() int {
	return len(this)
}

func (this HashRing) Less(i, j int) bool {
	return this[i] < this[j]
}

func Swap(this HashRing, i, j int) {
	this[i], this[j] = this[j], this[i]
}

type HashRingNode struct {
	Id     int
	IP     string
	Port   int
	Weight int
}

func NewHashRingNode(id int, ip string, port int, weight int) *HashRingNode {
	return &HashRingNode{
		Id:     id,
		IP:     ip,
		Port:   port,
		Weight: weight,
	}
}

type ConsistentHashing struct {
	Nodes     map[uint32]HashRingNode
	reps      int
	resources map[int]bool
	ring      HashRing
	sync.RWMutex
}

func NewConsistentHashing() *ConsistentHashing {
	return &ConsistentHashing{
		Nodes:     make(map[uint32]HashRingNode),
		reps:      DEFAULT_HASH_RING_REPLICAS,
		resources: make(map[int]bool),
		ring:      NewHashRing(),
	}
}

func (c *ConsistentHashing) AddNode(node *HashRingNode) bool {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.resources[node.Id]; ok {
		return false
	}

	c.resources[node.Id] = true
	for i := 0; i < c.reps; i++ {
		hash := c.hashKey(c.getKey(node, i))
		c.Nodes[hash] = *node
		c.ring = append(c.ring, hash)
	}
	c.ring.Sort()
	return true
}

func (this HashRing) Sort() {
	for i := 0; i < this.Len(); i++ {
		for j := i; j < this.Len(); j++ {
			if this.Less(i, j) {
				Swap(this, i, j)
			}
		}
	}
}

func (c *ConsistentHashing) RemoveNode(node *HashRingNode) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.resources[node.Id]; !ok {
		return
	}

	delete(c.resources, node.Id)
	for i := 0; i < c.reps; i++ {
		hash := c.hashKey(c.getKey(node, i))
		delete(c.Nodes, hash)
	}
	c.ring.Sort()
}

func (c *ConsistentHashing) GetNode(key string) HashRingNode {
	c.RLock()
	defer c.RUnlock()

	hash := c.hashKey(key)
	i := c.search(hash)
	return c.Nodes[c.ring[i]]
}
func (c *ConsistentHashing) GetNodeById(id int) HashRingNode {
	c.RLock()
	defer c.RUnlock()

	for _, v := range c.Nodes {
		if v.Id == id {
			return v
		}
	}
	return HashRingNode{}
}
func (c *ConsistentHashing) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *ConsistentHashing) getKey(node *HashRingNode, idx int) string {
	return node.IP + "*" + string(node.Port) + "*" + string(node.Weight) + "*" + string(idx)
}
func (c *ConsistentHashing) search(hash uint32) int {
	i := sort.Search(len(c.ring), func(i int) bool { return c.ring[i] >= hash })
	if i < len(c.ring) {
		if i == len(c.ring)-1 {
			return 0
		} else {
			return i
		}
	} else {
		return len(c.ring) - 1
	}
}
