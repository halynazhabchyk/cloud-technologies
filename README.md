# Cloud Architectures — Capstone: distributed key-value store

KPI course "Cloud Architectures", 2026/27. A simplified distributed key-value store built over four milestones.

| Milestone | Topic | Code | Git tag |
|-----------|-------|------|---------|
| 1 | Infrastructure: IAM, VPC (CloudFormation) | [`milestone_1/`](milestone_1/) | — |
| 2 | Sharding: coordinator, consistent hashing, deploy to EC2 | [`milestone_2/`](milestone_2/) | `m2` |
| 3 | Replication: 3 replicas per shard, Kafka log, quorums | [`lab-3-4/`](lab-3-4/) | `m3` |
| 4 | Monitoring, failures, MCP | [`lab-3-4/`](lab-3-4/) | `m4` |

Milestones 3 and 4 evolve the same application in `lab-3-4/`. The state of the code at each defense is fixed by a tag:

```sh
git checkout m3               # the code as defended for milestone 3
git diff m3 m4 -- lab-3-4/    # what milestone 4 changed
```

CI: [`ci.yml`](.github/workflows/ci.yml) lints, tests and smoke-tests `lab-3-4/`. [`deploy-lab2.yml`](.github/workflows/deploy-lab2.yml) deployed `milestone_2/` to EC2 via SSM; it is kept for reference and disabled, since the AWS environment is no longer running.
