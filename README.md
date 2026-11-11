# Cloud Architectures — Capstone: distributed key-value store

KPI course "Cloud Architectures", 2026/27. A simplified distributed key-value store built over four milestones.

| Milestone | Topic | Code | Git tag |
|-----------|-------|------|---------|
| 1 | Infrastructure: IAM, VPC (CloudFormation) | [`milestone_1/`](milestone_1/) | — |
| 2 | Sharding: coordinator, consistent hashing, deploy to EC2 | [`milestone_2/`](milestone_2/) | — |
| 3 | Replication: 3 replicas per shard, Kafka log, quorums | [`kv-store/`](kv-store/) | `m3` |
| 4 | Monitoring, failures, MCP | [`kv-store/`](kv-store/) | `m4` |

Milestones 3 and 4 evolve the same application in `kv-store/`. The state of the code at each defense is fixed by a tag:

```sh
git checkout m3               # the code as defended for milestone 3
git diff m3 m4 -- kv-store/   # what milestone 4 changed
```

CI: [`ci.yml`](.github/workflows/ci.yml) lints, tests and smoke-tests `kv-store/`. [`deploy-lab2.yml`](.github/workflows/deploy-lab2.yml) deploys `milestone_2/`.
