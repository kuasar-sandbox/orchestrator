#!/usr/bin/env bash
write_builder_config() {
cat > "$WORK/config.yaml" <<EOF
api: { domain: $DOMAIN, listen: ":$PORT" }
proxy: { auth: enforce }
mmds:
  enabled: true
  listen: "$MGMT_VIP:$MMDS_PORT"
  routes:
    enabled: true
encryption_key: "$ENC"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/run, base_root: $WORK/lib, config_socket: $WORK/node-ctl.socket }
units:
  dir: $UNIT_DIR
  # Shared template, different idle targets, and identical independent entries.
  # Existing global admission, phase and live-recovery checks exercise all slots.
  builder_pools:
    - {unit: sandbox-builder@.service, size: 1}
    - {unit: sandbox-builder@.service, size: 0}
    - {unit: sandbox-builder@.service, size: 1}
sandbox:
  resources:
    capacity: { cpu: 2, memory: 2GiB }
    allocatable: { cpu: 1, memory: 256MiB }
  network:
    switch: $SWITCH
    tapfd_socket: $TAPFD_SOCKET
  boot:
    kernel: $BIN/vmlinux
    runtime: $BIN/sandbox-runtime.bundle
    overlay_diff_template: $OVL
builder:
  admission:
    registration:
      max_builds: 16
      resources: { cpu: $BUILDER_REGISTRATION_CPU, memory: 96GiB, storage: 128GiB }
    execution:
      max_builds: 2
      resources: { cpu: $BUILDER_EXECUTION_CPU, memory: 12GiB, storage: 16GiB }
  registration_ttl: 1h
  queue_ttl: 30m
  terminal_ttl: 5s
  insecure_registry: true
  diff_template: $BLDDIFF
  pull_timeout_sec: 300
  step_timeout_sec: 180
  ready_timeout_sec: 60
  total_timeout_sec: 1200
$FILES_STORAGE_YAML
resource_listen:
  enabled: true
  socket: $WORK/sandbox-resource.sock
  resources:
    physical_memory: auto
    physical_cpu: auto
    host_reserved: { memory: 1GiB, cpu: 0.5 }
  admission: { rate: 50, burst: 50, startup_ttl: 180s, queue_ttl: 30s, queue_max_depth: 256 }
checkpoint: { mode: bundle }
EOF
}
