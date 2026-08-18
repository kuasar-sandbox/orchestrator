package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

func builderCmd(args []string) int {
	if len(args) < 1 || args[0] != "status" {
		fmt.Fprintln(os.Stderr, "usage: node-ctl builder status [--socket <path>]")
		return 2
	}
	return builderStatusCmd(args[1:])
}

func builderStatusCmd(args []string) int {
	fs := flag.NewFlagSet("builder status", flag.ContinueOnError)
	socket := fs.String("socket", "", "orchestrator control socket (or NODE_CTL_SOCKET env)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	code, body, err := udsDo(resolveSocket(*socket), http.MethodGet, configsock.PathAdminBuilderAdmission, nil, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if code != http.StatusOK {
		fmt.Fprintf(os.Stderr, "builder status: HTTP %d: %s\n", code, apiMessage(body))
		return 1
	}
	var status configsock.BuilderAdmissionStatus
	if err := json.Unmarshal(body, &status); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	out, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Println(string(out))
	return 0
}
