# Working-set snapshot A/B/C/D performance report

This is a descriptive first report; it defines no pass/fail performance threshold. Percentiles use the nearest-rank method over independent restores of the same immutable B.

| Scenario | drop-caches | merge-ref | prefetch | result | N |
|---|---:|---:|---|---|---:|
| A/off | true | true | off | disabled | 30 |
| B/off | false | true | off | disabled | 30 |
| C/off | true | false | off | disabled | 30 |
| D/off | false | false | off | disabled | 30 |
| D/memory | false | false | memory | completed | 30 |

## Artifact, capture, publication, and restore

| Metric (p50/p95/p99) | A/off | B/off | C/off | D/off | D/memory |
|---|---:|---:|---:|---:|---:|
| B memory logical | 96.4 MiB/96.4 MiB/96.4 MiB | 96.4 MiB/96.4 MiB/96.4 MiB | 96.4 MiB/96.4 MiB/96.4 MiB | 96.4 MiB/96.4 MiB/96.4 MiB | 96.4 MiB/96.4 MiB/96.4 MiB |
| B memory allocated | 96.4 MiB/96.4 MiB/96.4 MiB | 96.4 MiB/96.4 MiB/96.4 MiB | 96.4 MiB/96.4 MiB/96.4 MiB | 96.4 MiB/96.4 MiB/96.4 MiB | 96.4 MiB/96.4 MiB/96.4 MiB |
| W self logical | 98.0 MiB/98.0 MiB/98.0 MiB | 98.0 MiB/98.0 MiB/98.0 MiB | 71.8 MiB/72.9 MiB/72.9 MiB | 71.9 MiB/72.9 MiB/72.9 MiB | 71.9 MiB/72.9 MiB/72.9 MiB |
| W self allocated | 98.0 MiB/98.0 MiB/98.0 MiB | 98.0 MiB/98.0 MiB/98.0 MiB | 71.9 MiB/72.9 MiB/72.9 MiB | 71.9 MiB/72.9 MiB/72.9 MiB | 71.9 MiB/72.9 MiB/72.9 MiB |
| W disk tops logical | 19.6 MiB/19.6 MiB/19.6 MiB | 19.6 MiB/19.6 MiB/19.6 MiB | 19.6 MiB/19.6 MiB/19.6 MiB | 19.6 MiB/19.6 MiB/19.6 MiB | 19.6 MiB/19.6 MiB/19.6 MiB |
| W disk tops allocated | 19.6 MiB/19.6 MiB/19.6 MiB | 19.6 MiB/19.6 MiB/19.6 MiB | 19.6 MiB/19.6 MiB/19.6 MiB | 19.6 MiB/19.6 MiB/19.6 MiB | 19.6 MiB/19.6 MiB/19.6 MiB |
| MemoryResident | 98.0 MiB/98.0 MiB/98.0 MiB | 98.0 MiB/98.0 MiB/98.0 MiB | 71.8 MiB/72.9 MiB/72.9 MiB | 71.9 MiB/72.8 MiB/72.8 MiB | 71.9 MiB/72.8 MiB/72.8 MiB |
| MemoryResident ratio | 19.1%/19.1%/19.1% | 19.1%/19.1%/19.1% | 14.0%/14.2%/14.2% | 14.0%/14.2%/14.2% | 14.0%/14.2%/14.2% |
| snapshot pause | 1.00 ms/1.00 ms/1.00 ms | 1.00 ms/1.00 ms/1.00 ms | 1.00 ms/1.00 ms/1.00 ms | 1.00 ms/1.00 ms/1.00 ms | 1.00 ms/1.00 ms/1.00 ms |
| snapshot dump | 398 ms/400 ms/400 ms | 399 ms/400 ms/400 ms | 312 ms/315 ms/315 ms | 311 ms/315 ms/315 ms | 311 ms/315 ms/315 ms |
| local capture wall | 434 ms/439 ms/441 ms | 435 ms/442 ms/443 ms | 348 ms/354 ms/355 ms | 348 ms/353 ms/353 ms | 348 ms/353 ms/353 ms |
| manifest publish wall | 1038 ms/1059 ms/1070 ms | 1046 ms/1076 ms/1077 ms | 1414 ms/1437 ms/1452 ms | 1410 ms/1444 ms/1455 ms | 1410 ms/1444 ms/1455 ms |
| restore-to-ack | 495 ms/522 ms/525 ms | 473 ms/502 ms/510 ms | 478 ms/510 ms/511 ms | 443 ms/469 ms/474 ms | 432 ms/451 ms/456 ms |
| application ready | 1083 ms/1270 ms/1285 ms | 1085 ms/1120 ms/1186 ms | 948 ms/974 ms/975 ms | 942 ms/967 ms/993 ms | 899 ms/925 ms/935 ms |
| first representative request | 17705 ms/18517 ms/18784 ms | 340 ms/472 ms/486 ms | 17683 ms/18664 ms/18674 ms | 323 ms/488 ms/505 ms | 299 ms/462 ms/497 ms |
| UFFD faults | 209/265/271 | 279/292/292 | 210/257/265 | 275/289/299 | 272/290/296 |
| UFFD pages copied | 15761/17984/18032 | 18999/20554/20637 | 15769/17984/17993 | 19141/20534/20597 | 19008/20542/20599 |
| UFFD pages zeroed | 0.00/1.00/1.00 | 0.00/1.00/1.00 | 0.00/1.00/1.00 | 0.00/1.00/1.00 | 0.00/0.00/1.00 |
| lazy-load ratio | 12.0%/13.7%/13.8% | 14.5%/15.7%/15.7% | 12.0%/13.7%/13.7% | 14.6%/15.7%/15.7% | 14.5%/15.7%/15.7% |
| cache origin requests | 142/158/158 | 139/148/169 | 219/258/258 | 235/250/252 | 233/242/245 |
| cache/store origin transferred bytes | N/A/N/A/N/A | N/A/N/A/N/A | N/A/N/A/N/A | N/A/N/A/N/A | N/A/N/A/N/A |
| prefetch started after restore T0 | N/A/N/A/N/A | N/A/N/A/N/A | N/A/N/A/N/A | N/A/N/A/N/A | 66.7 ms/71.2 ms/75.6 ms |
| prefetch completion duration | N/A/N/A/N/A | N/A/N/A/N/A | N/A/N/A/N/A | N/A/N/A/N/A | 367 ms/387 ms/399 ms |

## Disk reads

| Scenario | Backend | Logical disk role | read bytes p50/p95/p99 | per-run read latency p50 median | per-run read latency p99 median |
|---|---|---|---:|---:|---:|
| A/off | blk0 | root.base | 499712 B/966656 B/966656 B | 3079 µs | 5630 µs |
| A/off | blk1 | root.top | 4096 B/65536 B/65536 B | 1200 µs | 1436 µs |
| A/off | blk2 | scratch.top | 16785408 B/16785408 B/16785408 B | 12727 µs | 144511 µs |
| A/off | blk3 | dataset.base | 0.00 B/0.00 B/0.00 B | 0.00 µs | 0.00 µs |
| A/off | blk4 | dataset.top | 4096 B/4096 B/4096 B | 428 µs | 428 µs |
| B/off | blk0 | root.base | 184320 B/647168 B/647168 B | 3217 µs | 5354 µs |
| B/off | blk1 | root.top | 4096 B/12288 B/12288 B | 1084 µs | 1500 µs |
| B/off | blk2 | scratch.top | 4096 B/4096 B/4096 B | 394 µs | 394 µs |
| B/off | blk3 | dataset.base | 0.00 B/0.00 B/0.00 B | 0.00 µs | 0.00 µs |
| B/off | blk4 | dataset.top | 4096 B/4096 B/4096 B | 357 µs | 357 µs |
| C/off | blk0 | root.base | 499712 B/966656 B/966656 B | 3077 µs | 5483 µs |
| C/off | blk1 | root.top | 4096 B/65536 B/65536 B | 1217 µs | 1485 µs |
| C/off | blk2 | scratch.top | 16785408 B/16785408 B/16785408 B | 12672 µs | 145260 µs |
| C/off | blk3 | dataset.base | 0.00 B/0.00 B/0.00 B | 0.00 µs | 0.00 µs |
| C/off | blk4 | dataset.top | 4096 B/4096 B/4096 B | 419 µs | 419 µs |
| D/off | blk0 | root.base | 184320 B/647168 B/647168 B | 3222 µs | 5791 µs |
| D/off | blk1 | root.top | 4096 B/12288 B/12288 B | 1026 µs | 1424 µs |
| D/off | blk2 | scratch.top | 4096 B/4096 B/4096 B | 390 µs | 390 µs |
| D/off | blk3 | dataset.base | 0.00 B/0.00 B/0.00 B | 0.00 µs | 0.00 µs |
| D/off | blk4 | dataset.top | 4096 B/4096 B/4096 B | 346 µs | 346 µs |
| D/memory | blk0 | root.base | 184320 B/647168 B/647168 B | 3222 µs | 5454 µs |
| D/memory | blk1 | root.top | 4096 B/12288 B/12288 B | 1087 µs | 1135 µs |
| D/memory | blk2 | scratch.top | 4096 B/4096 B/4096 B | 394 µs | 394 µs |
| D/memory | blk3 | dataset.base | 0.00 B/0.00 B/0.00 B | 0.00 µs | 0.00 µs |
| D/memory | blk4 | dataset.top | 4096 B/4096 B/4096 B | 368 µs | 368 µs |

## Prefetch and transfer notes

- The A/B/C/D comparison fixes restore prefetch to `off`; `D/memory` is the paired self-prefetch observation.
- `cache_origin_requests` comes from cache-ctl's existing tiered origin counters.
- Cache/store transferred bytes are `N/A`: the existing info surface exposes request counters but not byte totals; no new metrics protocol is introduced.
- Each `D/memory` raw row records prefetch start/completion offsets and result. Parent and disk prefetch are rejected by the harness's log assertions.

## Environment

```json
{
  "artifacts": {
    "binaries_sha256": {
      "cache-ctl": "fe264e0ca36444558899dd806841e987bbfd36b74333a0eae68274f16856c2d4",
      "cloud-hypervisor": "6d4ec63d6b210386d697807f78f85f2c242a5b68deff31d3d1b122c4a2818ed3",
      "flatten-ctl": "ba6d777e8f8d43c6ff9c227a125e9a07867f60450867f1608c8233c38047a596",
      "manifest-ctl": "b3f6c65679ca1f56d9764e53c7e21793e348c18e4157adfd96b035b4cbd3ea2e",
      "mkfs.erofs": "43c9f84658030ae94f134c13bfb4e534f6a4c6331556904163fc288e806e40fd",
      "sandbox-ctl": "9a16c302ce4eaebecd0418c5182c754c92f432319f8554892be9c1112a8f566f",
      "sandbox-init": "66a280f6c41da7107f9f433f13cbfa48d59d2a0ffe8aa46c11471e6a3e418afd",
      "sandbox-runtime.bundle": "45aa26f27689dc19ddf3479c3095be66e26115ccb71d3d6feb00e9eeb1f0e8ff",
      "store-ctl": "49be8426d35a2426707c2730e746973a78b9170189a6d0d41f136f5f60170154"
    },
    "cloud_hypervisor_version": "cloud-hypervisor v51.1.0",
    "kernel_sha256": "5765a0bf009daac7157cad92e142442b2c7f83872294a5b09534cc3f55bee99d"
  },
  "generated_at": "2026-08-04T12:07:28.628954+00:00",
  "host": {
    "cpu_count": 88,
    "cpu_model": "Intel(R) Xeon(R) Gold 6266C CPU @ 3.00GHz",
    "kvm": "/dev/kvm",
    "mem_total_kib": 394165904,
    "uname": {
      "machine": "x86_64",
      "node": "kuasar-ci-6",
      "processor": "x86_64",
      "release": "6.6.0-159.4.6.157.20260713.a4e2472763b2.oe2403sp4.x86_64",
      "system": "Linux",
      "version": "#1 SMP Tue Jul 14 10:17:51 CST 2026"
    }
  },
  "inputs": {
    "groups": [
      "A",
      "B",
      "C",
      "D"
    ],
    "guest_cpu": 1,
    "guest_memory": "512MiB",
    "host_cache_reset": "targeted-posix-fadvise-dontneed",
    "image": "python:3.12-slim",
    "image_id": "sha256:25c5b8011a3425a140bf5fa73be0feabd3c0d5b323eecb19dc02437a368ae075",
    "iterations": 30,
    "main_prefetch": "off",
    "manifest_cache_reset": "restart cache-ctl with an empty RocksDB before each B and portable restore",
    "manifest_chunk_mode": "cdc",
    "manifest_encryption": {
      "chunk": "aes",
      "manifest": "aes"
    },
    "manifest_store_reset": "restart store-ctl from the immutable root/dataset baseline before each sample",
    "paired_prefetch": "D/memory",
    "warm_bytes": 16777216
  },
  "repositories": {
    "accelerator": {
      "dirty": false,
      "requested_ref": "main",
      "role": "sibling",
      "sha": "1f5f7ba78e2856a0dc11ddbdb4670ce4c9fd375b",
      "source": "revision-manifest"
    },
    "connector": {
      "dirty": false,
      "requested_ref": "main",
      "role": "sibling",
      "sha": "2735b76c47e318bfeaee20c6a5f1b6c6b53979f1",
      "source": "revision-manifest"
    },
    "guest-runtime": {
      "dirty": false,
      "requested_ref": "main",
      "role": "sibling",
      "sha": "db0c8920836cda37b4eff7e681a7b300d440617e",
      "source": "revision-manifest"
    },
    "orchestrator": {
      "dirty": false,
      "requested_ref": "d5c77bacaaf303852047ddb627e1b1ccc86cad24",
      "role": "candidate",
      "sha": "d5c77bacaaf303852047ddb627e1b1ccc86cad24",
      "source": "revision-manifest"
    },
    "sandboxer": {
      "dirty": false,
      "requested_ref": "main",
      "role": "sibling",
      "sha": "3af9e03a646b91d78ffe6ffd0af00e5d65892b02",
      "source": "revision-manifest"
    }
  }
}
```

Raw samples: `samples.jsonl`
