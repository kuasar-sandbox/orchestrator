// Package config is the internal compatibility facade for the public
// github.com/kuasar-sandbox/orchestrator/config schema. New code should import
// the public package directly; these aliases keep the existing core migration
// mechanical while preserving a single source of truth.
package config

import publicconfig "github.com/kuasar-sandbox/orchestrator/config"

const (
	ProxyInternal = publicconfig.ProxyInternal
	ProxyExternal = publicconfig.ProxyExternal
	ProxyOff      = publicconfig.ProxyOff

	AuthOff     = publicconfig.AuthOff
	AuthLog     = publicconfig.AuthLog
	AuthEnforce = publicconfig.AuthEnforce

	CheckpointLocal  = publicconfig.CheckpointLocal
	CheckpointBundle = publicconfig.CheckpointBundle

	BinSandboxCtl   = "sandbox-ctl"
	BinConnectorCtl = "connector-ctl"
	BinFlattenCtl   = "flatten-ctl"
	BinNodeCtl      = "node-ctl"
)

type Config = publicconfig.Conductor
type ProxyFileConfig = publicconfig.Proxy
type ResourceListenConfig = publicconfig.ResourceListenConfig
type ResourceHostConfig = publicconfig.ResourceHostConfig
type ResourceHostReserved = publicconfig.ResourceHostReserved
type ResourceWatermarksConfig = publicconfig.ResourceWatermarksConfig
type ResourceRateLimitsConfig = publicconfig.ResourceRateLimitsConfig
type ResourceAdmissionConfig = publicconfig.ResourceAdmissionConfig
type ClusterConfig = publicconfig.ClusterConfig
type ClusterNodeLink = publicconfig.ClusterNodeLink
type TLSMaterial = publicconfig.TLSMaterial
type APIConfig = publicconfig.APIConfig
type TLSConfig = publicconfig.TLSConfig
type ProxyConfig = publicconfig.ProxyConfig
type MMDSConfig = publicconfig.MMDSConfig
type MMDSRoutesConfig = publicconfig.MMDSRoutesConfig
type MMDSServiceRegistryEntry = publicconfig.MMDSServiceRegistryEntry
type PathsConfig = publicconfig.PathsConfig
type UnitsConfig = publicconfig.UnitsConfig
type SandboxConfig = publicconfig.SandboxConfig
type ResourcesConfig = publicconfig.ResourcesConfig
type ResourceCapacity = publicconfig.ResourceCapacity
type ResourceAllocatable = publicconfig.ResourceAllocatable
type ResourceStartup = publicconfig.ResourceStartup
type ResourceOverhead = publicconfig.ResourceOverhead
type ResourceWatermarkHigh = publicconfig.ResourceWatermarkHigh
type NetworkConfig = publicconfig.NetworkConfig
type ProfileNet = publicconfig.ProfileNet
type BootConfig = publicconfig.BootConfig
type BuilderConfig = publicconfig.BuilderConfig
type BuilderAdmissionConfig = publicconfig.BuilderAdmissionConfig
type BuildAdmissionLimitConfig = publicconfig.BuildAdmissionLimitConfig
type BuildAdmissionResourcesConfig = publicconfig.BuildAdmissionResourcesConfig
type CPUCores = publicconfig.CPUCores
type BuilderRefererConfig = publicconfig.BuilderRefererConfig
type FilesStorageConfig = publicconfig.FilesStorageConfig
type CheckpointConfig = publicconfig.CheckpointConfig
type CheckpointRemoteConfig = publicconfig.CheckpointRemoteConfig
type ProxyPathsConfig = publicconfig.ProxyPathsConfig

func Load(path string) (*Config, error)               { return publicconfig.LoadConductor(path) }
func LoadProxy(path string) (*ProxyFileConfig, error) { return publicconfig.LoadProxy(path) }
