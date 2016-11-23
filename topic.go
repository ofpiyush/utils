package exchange

import (
	"io"
	"sync"

	"github.com/daakia/utils/distribution"
)

type Node interface {
	Subscribe(key []byte, id string, s io.Writer)
	UnSubscribe(key []byte, id string)
	Publish(key []byte, data []byte)
	//Remove(key[]byte)
}

type TopicConf struct {
	SingleWc byte
	MultiWc  byte
	Sys      byte
	Dist     func() distribution.Distribution
}

type TopicNode struct {
	id              byte
	Conf            *TopicConf
	SubscriberKeys  map[string]bool
	WSubscriberKeys map[string]bool

	subscribers  distribution.Distribution
	wsubscribers distribution.Distribution
	children     map[byte]*TopicNode
	mu           sync.Mutex
}

func (n *TopicNode) Publish(key []byte, data []byte) {
	if n.wsubscribers != nil {
		n.wsubscribers.Write(data)
	}
	// It is us!!! :D
	if len(key) == 1 {
		if n.subscribers == nil {
			// lolmax why write?
			// Maybe a bug? report later
			return
		}
		n.subscribers.Write(data)
		return
	}

	if n.children == nil {
		n.children = make(map[byte]*TopicNode)
	}
	if v, ok := n.children[key[1]]; ok {
		v.Publish(key[1:], data)
	}

	return
}

func (n *TopicNode) Subscribe(key []byte, id string, s io.Writer) {
	//It is us!
	if len(key) == 1 {
		if n.subscribers == nil {
			n.subscribers = n.Conf.Dist()
		}

		if n.SubscriberKeys == nil {
			n.SubscriberKeys = make(map[string]bool)
		}
		//if !n.SubscriberKeys[id] {
		n.subscribers.Attach(s)
		n.SubscriberKeys[id] = true
		//}
		return
	}
	if key[1] == n.Conf.MultiWc {
		if n.wsubscribers == nil {
			n.wsubscribers = n.Conf.Dist()
		}
		if n.WSubscriberKeys == nil {
			n.WSubscriberKeys = make(map[string]bool)
		}
		//if !n.WSubscriberKeys[id] {
		n.wsubscribers.Attach(s)
		n.WSubscriberKeys[id] = true
		//}
	}

	if key[1] == n.Conf.SingleWc {
		key = key[2:]
		n.mu.Lock()
		defer n.mu.Unlock()
		for _, v := range n.children {
			v.Subscribe(key, id, s)
		}
		return
	}
	n.mu.Lock()
	if n.children == nil {
		n.children = make(map[byte]*TopicNode)
	}
	if _, ok := n.children[key[1]]; !ok {
		n.children[key[1]] = &TopicNode{id: key[1], Conf: n.Conf}
	}
	n.mu.Unlock()
	n.children[key[1]].Subscribe(key[1:], id, s)
}

func (n *TopicNode) UnSubscribe(key []byte, id string) {
	// Do noting for now.
}
