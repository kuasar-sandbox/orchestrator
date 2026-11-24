package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func statusCmd(args []string, output io.Writer) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	endpoint := fs.String("registry", "", "Registry control-plane URL")
	group := fs.String("group", "", "Group name")
	nodeID := fs.String("node-id", "", "Node ID to inspect")
	templateID := fs.String("template-id", "", "explicit Create template (default: Group template)")
	ca := fs.String("ca", "", "CA certificate PEM (default: system trust)")
	cert := fs.String("cert", "", "client certificate PEM for mTLS")
	key := fs.String("key", "", "client private key PEM for mTLS")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *endpoint == "" || *nodeID == "" || nodegroup.ValidateName(*group) != nil {
		return fmt.Errorf("status requires --registry URL --group GROUP --node-id NODE")
	}
	if (*cert == "") != (*key == "") {
		return fmt.Errorf("--cert and --key must be supplied together")
	}
	u, err := url.Parse(*endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("--registry must be an http(s) origin without credentials, path, query or fragment")
	}
	if u.Scheme != "https" && (*ca != "" || *cert != "") {
		return fmt.Errorf("TLS options require an https Registry URL")
	}
	if *templateID != "" {
		if _, err := types.ParseTemplateID(*templateID); err != nil {
			return fmt.Errorf("invalid --template-id")
		}
	}
	apiKey := os.Getenv("E2B_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("E2B_API_KEY is required")
	}
	tlsConfig, err := (clustercfg.TLS{CA: *ca, Cert: *cert, Key: *key}).ClientConfig("")
	if err != nil {
		return err
	}
	u.Path = registry.RouteLinkGroupStatusPath
	q := url.Values{"group": {*group}, "node_id": {*nodeID}}
	if *templateID != "" {
		q.Set("template_id", *templateID)
	}
	u.RawQuery = q.Encode()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second,
		// A diagnostic must not forward its Group key to a redirect target.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-API-KEY", apiKey)
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		// Do not echo an untrusted response body containing private context.
		return fmt.Errorf("Group status request failed (HTTP %d)", response.StatusCode)
	}
	var status registry.GroupStatus
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<10))
	if err := decoder.Decode(&status); err != nil {
		return fmt.Errorf("invalid Group status response")
	}
	if status.Group != *group || status.NodeID != *nodeID || !status.StorageReadable {
		return fmt.Errorf("Group status response does not match the requested identity")
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(status)
}
