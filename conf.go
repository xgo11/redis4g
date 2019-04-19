package redis4g

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)
import (
	"github.com/xgo11/configuration"
	"github.com/xgo11/datetime"
	"github.com/xgo11/env"
)

const (
	pathPrefix  = "db/redis"
	defaultPort = 6379
)

type ConnectionParameter struct {
	Prefix         string `yaml:"prefix" json:"prefix"`
	Host           string `yaml:"host" json:"host"`
	Password       string `yaml:"password" json:"password"`
	Port           int    `yaml:"port" json:"port"`
	Db             int    `yaml:"db" json:"db"`
	MaxConnections int    `yaml:"max_connections" json:"max_connections"`

	path   string // config path
	file   string // config file
	lstmod int64  // latest modify time
}

func fulfillPath(path string) string {
	path = strings.Trim(path, "/")
	if strings.HasPrefix(path, pathPrefix) && path != pathPrefix {
		return path
	}
	return pathPrefix + "/" + path
}

func NewConnectionParameter(path string) (cfg ConnectionParameter, err error) {
	path = fulfillPath(path)
	var file = filepath.Join(env.ConfDir(), path+".yaml")
	if err = configuration.LoadYaml(file, &cfg); err != nil {
		return
	}
	if !cfg.checkValid() {
		err = fmt.Errorf("invalid config")
		return
	}
	if cfg.Prefix == "" {
		prefix := strings.ReplaceAll(strings.TrimLeft(strings.TrimLeft(path, pathPrefix), "/"), "/", ":")
		cfg.Prefix = prefix
	}

	var info os.FileInfo
	if info, err = os.Stat(file); err != nil {
		return
	}
	cfg.lstmod = info.ModTime().In(datetime.LocalTZ()).Unix()
	cfg.file = file
	cfg.path = path
	return
}

func (c *ConnectionParameter) checkValid() bool {
	if c.Host == "" {
		return false
	}
	if c.Port == 0 {
		c.Port = defaultPort
	}
	if c.Db < 0 || c.Db > 15 {
		return false
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = 50 * runtime.NumCPU()
	}
	return true
}

func (c *ConnectionParameter) File() string {
	return c.file
}

func (c *ConnectionParameter) Path() string {
	return c.path
}

func (c *ConnectionParameter) LstMod() int64 {
	return c.lstmod
}

func (c *ConnectionParameter) JSON(indent ...int) string {
	if len(indent) > 0 && indent[0] > 0 {
		bs, _ := json.MarshalIndent(c, "", strings.Repeat(" ", indent[0]))
		return string(bs)
	} else {
		bs, _ := json.Marshal(c)
		return string(bs)
	}
}

func (c *ConnectionParameter) String() string {
	return fmt.Sprintf("<%s> %v:%v/%v@%v", c.Path(), c.Host, c.Port, c.Db, c.LstMod())
}
