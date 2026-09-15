package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

func builderCmd(args []string) int {
	if len(args) > 0 {
		switch args[0] {
		case "status":
			return builderStatusCmd(args[1:])
		case "cancel", "delete":
			return builderActionCmd(args[0], args[1:])
		}
	}
	fmt.Fprintln(os.Stderr, "usage: node-ctl builder {status|cancel <build-id>|delete <transient-template-id> [--cancel]} [--socket <path>]")
	return 2
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

func builderActionCmd(action string, args []string) int {
	fs := flag.NewFlagSet("builder "+action, flag.ContinueOnError)
	socket := fs.String("socket", "", "orchestrator control socket (or NODE_CTL_SOCKET env)")
	cancel := false
	if action == "delete" {
		fs.BoolVar(&cancel, "cancel", false, "cancel execution and delete after local cleanup")
	}
	// Permit the documented identity-before-options spelling with Go's flag parser.
	var identity string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		identity, args = args[0], args[1:]
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if identity == "" && fs.NArg() == 1 {
		identity = fs.Arg(0)
	} else if fs.NArg() != 0 {
		return 2
	}
	if identity == "" {
		fmt.Fprintln(os.Stderr, "build identity required")
		return 2
	}
	method, path := http.MethodPost, strings.Replace(configsock.PathAdminBuildCancel, "{bid}", url.PathEscape(identity), 1)
	if action == "delete" {
		method, path = http.MethodDelete, strings.Replace(configsock.PathAdminBuildDelete, "{tid}", url.PathEscape(identity), 1)
		if cancel {
			path += "?cancel=true"
		}
	}
	code, body, err := udsDo(resolveSocket(*socket), method, path, nil, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if code != http.StatusAccepted && code != http.StatusNoContent {
		fmt.Fprintf(os.Stderr, "builder %s: HTTP %d: %s\n", action, code, apiMessage(body))
		return 1
	}
	fmt.Printf("HTTP %d\n", code)
	return 0
}
