module github.com/kuasar-sandbox/orchestrator

go 1.24.0

require (
	cel.dev/cel-go v0.32.0
	github.com/aws/aws-sdk-go-v2 v1.41.7
	github.com/aws/aws-sdk-go-v2/config v1.32.17
	github.com/aws/aws-sdk-go-v2/credentials v1.19.16
	github.com/aws/aws-sdk-go-v2/service/s3 v1.100.1
	github.com/aws/smithy-go v1.25.1
	github.com/coreos/go-systemd/v22 v22.6.0
	github.com/godbus/dbus/v5 v5.1.0
	github.com/golang/snappy v1.0.0
	github.com/google/go-containerregistry v0.20.6
	github.com/google/uuid v1.6.0
	github.com/hashicorp/memberlist v0.5.4
	github.com/kuasar-sandbox/accelerator v0.1.3
	github.com/kuasar-sandbox/connector v0.1.2
	github.com/kuasar-sandbox/sandboxer v0.1.3
	github.com/oklog/ulid/v2 v2.1.1
	github.com/open-telemetry/opentelemetry-collector-contrib/connector/routingconnector v0.145.0
	github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter v0.145.0
	github.com/open-telemetry/opentelemetry-collector-contrib/exporter/prometheusremotewriteexporter v0.145.0
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckextension v0.145.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/resourcetotelemetry v0.145.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheusremotewrite v0.145.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor v0.145.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor v0.145.0
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/otlptranslator v1.0.0
	github.com/prometheus/prometheus v0.309.2-0.20260113170727-c7bc56cf6c8f
	go.opentelemetry.io/collector/component v1.51.0
	go.opentelemetry.io/collector/confmap v1.51.0
	go.opentelemetry.io/collector/confmap/provider/envprovider v1.51.0
	go.opentelemetry.io/collector/confmap/provider/fileprovider v1.51.0
	go.opentelemetry.io/collector/confmap/provider/yamlprovider v1.51.0
	go.opentelemetry.io/collector/confmap/xconfmap v0.145.0
	go.opentelemetry.io/collector/connector v0.145.0
	go.opentelemetry.io/collector/consumer v1.51.0
	go.opentelemetry.io/collector/consumer/consumererror v0.145.0
	go.opentelemetry.io/collector/exporter v1.51.0
	go.opentelemetry.io/collector/exporter/debugexporter v0.145.0
	go.opentelemetry.io/collector/exporter/otlpexporter v0.145.0
	go.opentelemetry.io/collector/exporter/otlphttpexporter v0.145.0
	go.opentelemetry.io/collector/extension v1.51.0
	go.opentelemetry.io/collector/otelcol v0.145.0
	go.opentelemetry.io/collector/pdata v1.51.0
	go.opentelemetry.io/collector/processor v1.51.0
	go.opentelemetry.io/collector/processor/batchprocessor v0.145.0
	go.opentelemetry.io/collector/processor/memorylimiterprocessor v0.145.0
	go.opentelemetry.io/collector/receiver v1.51.0
	go.opentelemetry.io/collector/receiver/otlpreceiver v0.145.0
	go.opentelemetry.io/collector/service v0.145.0
	go.uber.org/zap v1.27.1
	golang.org/x/net v0.49.0
	golang.org/x/sync v0.19.0
	golang.org/x/sys v0.40.0
	google.golang.org/genproto/googleapis/api v0.0.0-20260120221211-b8f7ae30c516
	google.golang.org/grpc v1.80.0
	google.golang.org/protobuf v1.36.11
	gopkg.in/yaml.v3 v3.0.1
	modernc.org/sqlite v1.30.0
)

// Cross-repo local resolution (single-repo offline build; go.work covers the
// workspace). orchestrator imports sandboxer/pkg/config to build sandbox.yaml
// from one source of truth; that transitively needs accelerator.
replace (
	github.com/kuasar-sandbox/accelerator => ../accelerator
	github.com/kuasar-sandbox/connector => ../connector
	github.com/kuasar-sandbox/sandboxer => ../sandboxer
)

require (
	cel.dev/expr v0.25.1 // indirect
	cloud.google.com/go/auth v0.17.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.20.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.13.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.11.2 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.6.0 // indirect
	github.com/ClickHouse/ch-go v0.69.0 // indirect
	github.com/ClickHouse/clickhouse-go/v2 v2.42.0 // indirect
	github.com/alecthomas/participle/v2 v2.1.4 // indirect
	github.com/alecthomas/units v0.0.0-20240927000941-0f3dac36c52b // indirect
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/antchfx/xmlquery v1.5.0 // indirect
	github.com/antchfx/xpath v1.3.5 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.10 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.23 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.23 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.23 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.24 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.42.1 // indirect
	github.com/bboreham/go-loser v0.0.0-20230920113527-fcc2c21820a3 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cilium/ebpf v0.20.0 // indirect
	github.com/containerd/stargz-snapshotter/estargz v0.16.3 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dennwc/varint v1.0.0 // indirect
	github.com/docker/cli v28.2.2+incompatible // indirect
	github.com/docker/distribution v2.8.3+incompatible // indirect
	github.com/docker/docker-credential-helpers v0.9.3 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/ebitengine/purego v0.9.1 // indirect
	github.com/elastic/go-grok v0.3.1 // indirect
	github.com/elastic/lunes v0.2.0 // indirect
	github.com/expr-lang/expr v1.17.7 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/foxboron/go-tpm-keyfiles v0.0.0-20251226215517-609e4778396f // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/go-tpm v0.9.8 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.7 // indirect
	github.com/googleapis/gax-go/v2 v2.15.0 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.4 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-metrics v0.5.4 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.5 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-sockaddr v1.0.7 // indirect
	github.com/hashicorp/go-version v1.8.0 // indirect
	github.com/hashicorp/golang-lru v0.6.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/iancoleman/strcase v0.3.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/insomniacslk/dhcp v0.0.0-20250109001534-8abf58130905 // indirect
	github.com/josharian/native v1.1.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.3 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/knadh/koanf/maps v0.1.2 // indirect
	github.com/knadh/koanf/providers/confmap v1.0.0 // indirect
	github.com/knadh/koanf/v2 v2.3.2 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/lightstep/go-expohisto v1.0.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20251013123823-9fd1530e3ec3 // indirect
	github.com/magefile/mage v1.15.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mdlayher/packet v1.1.2 // indirect
	github.com/mdlayher/socket v0.4.1 // indirect
	github.com/miekg/dns v1.1.69 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/mostynb/go-grpc-compression v1.2.3 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/common v0.145.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal v0.145.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter v0.145.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/healthcheck v0.145.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/pdatautil v0.145.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl v0.145.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil v0.145.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/status v0.145.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheus v0.145.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/paulmach/orb v0.12.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.23 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/client_golang/exp v0.0.0-20260101091701-2cd067eb23c9 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.67.5 // indirect
	github.com/prometheus/procfs v0.17.0 // indirect
	github.com/prometheus/sigv4 v0.3.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rs/cors v1.11.1 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/shirou/gopsutil/v4 v4.25.12 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spf13/cobra v1.10.2 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	github.com/tidwall/gjson v1.10.2 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tidwall/tinylru v1.1.0 // indirect
	github.com/tidwall/wal v1.2.1 // indirect
	github.com/tklauser/go-sysconf v0.3.16 // indirect
	github.com/tklauser/numcpus v0.11.0 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/u-root/uio v0.0.0-20230220225925-ffce2a382923 // indirect
	github.com/ua-parser/uap-go v0.0.0-20240611065828-3a4781585db6 // indirect
	github.com/vbatts/tar-split v0.12.1 // indirect
	github.com/vishvananda/netlink v1.3.1 // indirect
	github.com/vishvananda/netns v0.0.5 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/collector v0.145.0 // indirect
	go.opentelemetry.io/collector/client v1.51.0 // indirect
	go.opentelemetry.io/collector/component/componentstatus v0.145.0 // indirect
	go.opentelemetry.io/collector/component/componenttest v0.145.0 // indirect
	go.opentelemetry.io/collector/config/configauth v1.51.0 // indirect
	go.opentelemetry.io/collector/config/configcompression v1.51.0 // indirect
	go.opentelemetry.io/collector/config/configgrpc v0.145.0 // indirect
	go.opentelemetry.io/collector/config/confighttp v0.145.0 // indirect
	go.opentelemetry.io/collector/config/configmiddleware v1.51.0 // indirect
	go.opentelemetry.io/collector/config/confignet v1.51.0 // indirect
	go.opentelemetry.io/collector/config/configopaque v1.51.0 // indirect
	go.opentelemetry.io/collector/config/configoptional v1.51.0 // indirect
	go.opentelemetry.io/collector/config/configretry v1.51.0 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.145.0 // indirect
	go.opentelemetry.io/collector/config/configtls v1.51.0 // indirect
	go.opentelemetry.io/collector/connector/connectortest v0.145.0 // indirect
	go.opentelemetry.io/collector/connector/xconnector v0.145.0 // indirect
	go.opentelemetry.io/collector/consumer/consumererror/xconsumererror v0.145.0 // indirect
	go.opentelemetry.io/collector/consumer/consumertest v0.145.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.145.0 // indirect
	go.opentelemetry.io/collector/exporter/exporterhelper v0.145.0 // indirect
	go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper v0.145.0 // indirect
	go.opentelemetry.io/collector/exporter/exportertest v0.145.0 // indirect
	go.opentelemetry.io/collector/exporter/xexporter v0.145.0 // indirect
	go.opentelemetry.io/collector/extension/extensionauth v1.51.0 // indirect
	go.opentelemetry.io/collector/extension/extensioncapabilities v0.145.0 // indirect
	go.opentelemetry.io/collector/extension/extensionmiddleware v0.145.0 // indirect
	go.opentelemetry.io/collector/extension/extensiontest v0.145.0 // indirect
	go.opentelemetry.io/collector/extension/xextension v0.145.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.51.0 // indirect
	go.opentelemetry.io/collector/internal/componentalias v0.145.0 // indirect
	go.opentelemetry.io/collector/internal/fanoutconsumer v0.145.0 // indirect
	go.opentelemetry.io/collector/internal/memorylimiter v0.145.0 // indirect
	go.opentelemetry.io/collector/internal/sharedcomponent v0.145.0 // indirect
	go.opentelemetry.io/collector/internal/telemetry v0.145.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.145.0 // indirect
	go.opentelemetry.io/collector/pdata/testdata v0.145.0 // indirect
	go.opentelemetry.io/collector/pdata/xpdata v0.145.0 // indirect
	go.opentelemetry.io/collector/pipeline v1.51.0 // indirect
	go.opentelemetry.io/collector/pipeline/xpipeline v0.145.0 // indirect
	go.opentelemetry.io/collector/processor/processorhelper v0.145.0 // indirect
	go.opentelemetry.io/collector/processor/processorhelper/xprocessorhelper v0.145.0 // indirect
	go.opentelemetry.io/collector/processor/processortest v0.145.0 // indirect
	go.opentelemetry.io/collector/processor/xprocessor v0.145.0 // indirect
	go.opentelemetry.io/collector/receiver/receiverhelper v0.145.0 // indirect
	go.opentelemetry.io/collector/receiver/receivertest v0.145.0 // indirect
	go.opentelemetry.io/collector/receiver/xreceiver v0.145.0 // indirect
	go.opentelemetry.io/collector/semconv v0.128.0 // indirect
	go.opentelemetry.io/collector/service/hostcapabilities v0.145.0 // indirect
	go.opentelemetry.io/contrib/bridges/otelzap v0.13.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.63.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.64.0 // indirect
	go.opentelemetry.io/contrib/otelconf v0.18.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.38.0 // indirect
	go.opentelemetry.io/otel v1.39.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc v0.14.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp v0.14.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.38.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.38.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.39.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.39.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.39.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.60.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutlog v0.14.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.38.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.38.0 // indirect
	go.opentelemetry.io/otel/log v0.15.0 // indirect
	go.opentelemetry.io/otel/metric v1.39.0 // indirect
	go.opentelemetry.io/otel/sdk v1.39.0 // indirect
	go.opentelemetry.io/otel/sdk/log v0.14.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.39.0 // indirect
	go.opentelemetry.io/otel/trace v1.39.0 // indirect
	go.opentelemetry.io/proto/otlp v1.9.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/goleak v1.3.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/crypto v0.47.0 // indirect
	golang.org/x/exp v0.0.0-20250808145144-a408d31f581a // indirect
	golang.org/x/mod v0.31.0 // indirect
	golang.org/x/oauth2 v0.34.0 // indirect
	golang.org/x/text v0.33.0 // indirect
	golang.org/x/time v0.14.0 // indirect
	golang.org/x/tools v0.40.0 // indirect
	gonum.org/v1/gonum v0.17.0 // indirect
	google.golang.org/api v0.258.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260120221211-b8f7ae30c516 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	k8s.io/apimachinery v0.34.3 // indirect
	k8s.io/client-go v0.34.3 // indirect
	k8s.io/klog/v2 v2.130.1 // indirect
	k8s.io/utils v0.0.0-20250604170112-4c0f3b243397 // indirect
	modernc.org/gc/v3 v3.0.0-20240107210532-573471604cb6 // indirect
	modernc.org/libc v1.50.9 // indirect
	modernc.org/mathutil v1.6.0 // indirect
	modernc.org/memory v1.8.0 // indirect
	modernc.org/strutil v1.2.0 // indirect
	modernc.org/token v1.1.0 // indirect
)
