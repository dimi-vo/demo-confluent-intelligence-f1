# F1 Pit Wall AI: River Racing at Silverstone

[![Sign up for Confluent Cloud](https://img.shields.io/badge/Sign%20up%20for%20Confluent%20Cloud-007BFF?style=for-the-badge&logo=apachekafka&logoColor=white)](https://www.confluent.io/confluent-cloud/tryfree/?utm_campaign=tm.plg_cd.techxchange2026&utm_source=lab&utm_medium=github)

Build a real-time AI pit-strategy system on Confluent Cloud. Live telemetry and race standings flow into Kafka, Flink SQL detects a tire anomaly, and a Streaming Agent recommends when you, River Racing, should pit.

<div align="center">
  <img src="./docs/assets/architecture.png" alt="F1 Pit Wall Confluent Intelligence architecture" style="width:100%;max-width:1400px;">
</div>

## Choose your path

> [!IMPORTANT]
> **Did your company's platform team create a Confluent Cloud environment for you?**
>
> Use the **[pre-provisioned walkthrough](docs/tracks/PRE-PROVISIONED.md)**. 
> 
> Sign in with your work account, connect to the workshop your platform team built in an environment dedicated to you, and run the race on your computer.

| If this describes you | Start here |
|---|---|
| **I'm attending a self-service workshop.** I'll use my own Confluent Cloud account, provision my environment, and run the race on my computer. | [Self-service workshop walkthrough](docs/tracks/SELF-SERVICE.md) |
| **My instructor gave me a workshop login.** The environment and race are already running for me. | [Hosted workshop walkthrough](docs/tracks/HOSTED-WORKSHOP.md) |
| 🟢 **My platform team created an environment for me** in my company's Confluent Cloud, with the workshop already built in it. I'll connect to it and run the race on my computer. | [Pre-provisioned walkthrough](docs/tracks/PRE-PROVISIONED.md) |
| **I'm on that platform team.** I need to build the workshop in one environment per developer in our organization. | [Platform team guide](platform-team/README.md) |
| **I'm running the workshop.** I need to provision attendee environments and control the race. | [Organizer guide](docs/organizer/README.md) |
| **I'm running the demo by myself.** I want the full AWS-backed deployment used by the hosted workshop. | [Standalone AWS walkthrough](docs/tracks/STANDALONE-DEMO.md) |

## What you'll build

The simulator writes `car_telemetry` and `race_standings`. Historical race data lands in `driver_race_history`. During the labs, you'll build `car_state`, detect a front-left tire anomaly, create a streaming pit-strategy agent, and write its recommendations to `pit_decisions`.

## Repository map

```text
demo-confluent-intelligence-f1/
├── README.md                        # Choose the right path
├── docs/
│   ├── README.md                     # Documentation index
│   ├── tracks/SELF-SERVICE.md       # Self-service workshop
│   ├── tracks/PRE-PROVISIONED.md    # Setup in your platform team's environment
│   ├── tracks/HOSTED-WORKSHOP.md    # Hosted workshop attendee labs
│   ├── organizer/                   # Hosted workshop setup and run of show
│   └── maintainers/                 # Constraints
├── platform-team/                   # Customer platform team: one workshop environment per developer
├── scripts/                         # Workshop and self-service commands
└── terraform/                       # Shared and per-attendee infrastructure
```
