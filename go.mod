module github.com/kuasar-sandbox/sandbox-sentinel

go 1.24.0

require (
	github.com/kuasar-sandbox/sandbox-runtime v0.0.0
	gopkg.in/yaml.v3 v3.0.1
)

replace (
	github.com/kuasar-sandbox/sandbox-accelerator => ../sandbox-accelerator
	github.com/kuasar-sandbox/sandbox-builder => ../sandbox-builder
	github.com/kuasar-sandbox/sandbox-runtime => ../sandbox-runtime
	github.com/kuasar-sandbox/sandbox-vswitch => ../sandbox-vswitch
)
