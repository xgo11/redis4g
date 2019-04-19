package redis4g

import (
	"sync"
)

import (
	"github.com/xgo11/stdlog"
)

type configRegistry struct {
	sync.Mutex

	registry map[string]*ConnectionParameter
}
type connectorManager struct {
	sync.Mutex

	configs *configRegistry
	clients map[string]*WrapClient
}

var log = stdlog.Std
var mgr = &connectorManager{configs: &configRegistry{}}

func (r *configRegistry) GetConf(path string) *ConnectionParameter {
	path = fulfillPath(path)
	r.Lock()
	defer r.Unlock()

	if r.registry == nil {
		r.registry = map[string]*ConnectionParameter{}
	}

	var c = r.registry[path]
	if c == nil {
		if cp, err := NewConnectionParameter(path); err != nil {
			log.Errorf("Load config fail, path=%v, err=%v", path, err)
			return nil
		} else {
			c = &cp
			r.registry[c.Path()] = c
			log.Debugf("Load config ok, %v", c.String())
		}
	}
	return c
}

func (m *connectorManager) Connect(path string) *WrapClient {
	conf := m.configs.GetConf(path)
	if conf == nil {
		return nil
	}
	m.Lock()
	defer m.Unlock()

	if m.clients == nil {
		m.clients = map[string]*WrapClient{}
	}

	if c := m.clients[conf.Path()]; c != nil {
		return c
	}

	var c = &WrapClient{conf: conf}
	if err := c.tryConnect(); err != nil {
		log.Errorf("connect %v fail", conf.String())
		return nil
	} else {
		m.clients[conf.Path()] = c
		log.Debugf("connect %v ok", conf.String())
		return c
	}
}
