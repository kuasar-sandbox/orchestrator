// Package clustercfg loads the independent final configuration schema for each
// cluster-ctl role.
package clustercfg

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"
)

type MemberConfig struct {
	ID     string `yaml:"id"`
	Listen string `yaml:"listen"`
	TLS    TLS    `yaml:"tls"`
}

type IngressConfig struct {
	Listen string `yaml:"listen"`
	TLS    TLS    `yaml:"tls"`
}

type RouterAuth struct {
	APIKey    string `yaml:"api_key"`
	DataPlane string `yaml:"data_plane"`
	CacheTTL  string `yaml:"cache_ttl"`
}

type RouterCache struct {
	RouteTTL    string `yaml:"route_ttl"`
	IdleTimeout string `yaml:"idle_timeout"`
}

type PlacementConfig struct {
	Candidates      int           `yaml:"candidates"`
	ShuffleSharding []ShuffleRule `yaml:"shuffle_sharding"`
}

type GroupSourceConfig struct {
	SourceID   string `yaml:"source_id"`
	SourceType string `yaml:"source_type"`
	Path       string `yaml:"path,omitempty"`
}

type ShuffleRule struct {
	Selector map[string]string `yaml:"selector"`
	ShardBy  string            `yaml:"shard_by"`
	N        int               `yaml:"n"`
}

type TLS struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
	CA   string `yaml:"ca"`
}

func (t TLS) Enabled() bool { return t.Cert != "" && t.Key != "" }

func (t TLS) ServerConfig() (*tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(t.Cert, t.Key)
	if err != nil {
		return nil, err
	}
	config := &tls.Config{
		Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12,
		NextProtos: []string{"h2", "http/1.1"},
	}
	if t.CA != "" {
		pool, err := caPool(t.CA)
		if err != nil {
			return nil, err
		}
		config.ClientCAs = pool
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return config, nil
}

func (t TLS) ClientConfig(serverName string) (*tls.Config, error) {
	config := &tls.Config{
		MinVersion: tls.VersionTLS12, NextProtos: []string{"h2"}, ServerName: serverName,
	}
	if t.Cert != "" && t.Key != "" {
		certificate, err := tls.LoadX509KeyPair(t.Cert, t.Key)
		if err != nil {
			return nil, err
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	if t.CA != "" {
		pool, err := caPool(t.CA)
		if err != nil {
			return nil, err
		}
		config.RootCAs = pool
	}
	return config, nil
}

func caPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("clustercfg: no certificates in %s", path)
	}
	return pool, nil
}

func validateDurations(values map[string]string) error {
	for name, raw := range values {
		duration, err := time.ParseDuration(raw)
		if err != nil || duration <= 0 {
			return fmt.Errorf("clustercfg: %s must be a positive duration", name)
		}
	}
	return nil
}
