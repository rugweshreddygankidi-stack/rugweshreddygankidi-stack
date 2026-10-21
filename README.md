<div align="center">

<img src="https://capsule-render.vercel.app/api?type=waving&color=0:0f172a,100:0ea5e9&height=190&section=header&text=Rugwesh%20Reddy%20Gankidi&fontColor=ffffff&fontSize=44&fontAlignY=38&desc=Data%20Engineer%20%C2%B7%20Streaming%20%26%20Lakehouse%20%C2%B7%20RAG%20Data%20Layers&descAlignY=60&descSize=17" alt="Rugwesh Reddy Gankidi - Data Engineer" width="100%"/>

[![LinkedIn](https://img.shields.io/badge/LinkedIn-rugwesh--reddy--gankidi-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/rugwesh-reddy-gankidi/)
[![GitHub](https://img.shields.io/badge/GitHub-rugweshreddygankidi--stack-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/rugweshreddygankidi-stack)
![Location](https://img.shields.io/badge/Massachusetts-USA-0ea5e9?style=for-the-badge&logo=googlemaps&logoColor=white)
![Focus](https://img.shields.io/badge/Open%20to-Data%20Engineering%20roles-16a34a?style=for-the-badge)

</div>

---

## 👋 About

I'm a **Data Engineer** who builds batch and streaming pipelines, lakehouse storage layers, and the data foundations behind **retrieval-augmented generation (RAG)** systems. I care about the unglamorous parts that make data trustworthy: **data contracts, idempotent writes, dead-letter handling, quality gates, and measurable evaluation**.

- 🌊 **Streaming & lakehouse:** Kafka, Spark Structured Streaming, Apache Iceberg, Kinesis, Lambda, Athena
- 🧠 **Gen AI data layers:** ingestion, PII redaction, deduplication, embeddings, pgvector, retrieval evaluation (recall@k, MRR)
- 🏗️ **Infrastructure as code & CI:** Terraform, Docker, GitHub Actions, pytest-first development
- 🎓 **Machine learning:** fine-tuning and rigorous evaluation of language models (Master's coursework, UMass Dartmouth)

---

## 🧰 Tech Stack

<div align="center">

| | |
|:--|:--|
| **Languages** | ![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white) ![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat-square&logo=postgresql&logoColor=white) ![Bash](https://img.shields.io/badge/Bash-4EAA25?style=flat-square&logo=gnubash&logoColor=white) |
| **Processing & Streaming** | ![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white) ![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white) ![Kinesis](https://img.shields.io/badge/AWS%20Kinesis-FF9900?style=flat-square&logo=amazonaws&logoColor=white) ![Pandas](https://img.shields.io/badge/pandas-150458?style=flat-square&logo=pandas&logoColor=white) |
| **Warehouse & Lakehouse** | ![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat-square&logo=snowflake&logoColor=white) ![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-3b82f6?style=flat-square) ![Athena](https://img.shields.io/badge/Amazon%20Athena-8C4FFF?style=flat-square&logo=amazonaws&logoColor=white) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL%20%2B%20pgvector-336791?style=flat-square&logo=postgresql&logoColor=white) |
| **Orchestration & Transformation** | ![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white) ![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat-square&logo=dbt&logoColor=white) ![Great Expectations](https://img.shields.io/badge/Great%20Expectations-FF6310?style=flat-square) |
| **Cloud & DevOps** | ![AWS](https://img.shields.io/badge/AWS-232F3E?style=flat-square&logo=amazonaws&logoColor=white) ![Terraform](https://img.shields.io/badge/Terraform-844FBA?style=flat-square&logo=terraform&logoColor=white) ![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white) ![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-2088FF?style=flat-square&logo=githubactions&logoColor=white) |
| **AI / ML** | ![PyTorch](https://img.shields.io/badge/PyTorch-EE4C2C?style=flat-square&logo=pytorch&logoColor=white) ![Hugging Face](https://img.shields.io/badge/Hugging%20Face-FFD21E?style=flat-square&logo=huggingface&logoColor=black) ![RAG](https://img.shields.io/badge/RAG%20Data%20Layers-0ea5e9?style=flat-square) ![LLM APIs](https://img.shields.io/badge/LLM%20APIs-7c3aed?style=flat-square) |

</div>

---

## 🚀 Featured Projects

### 🌊 [Real-Time Claims Event Pipeline](https://github.com/rugweshreddygankidi-stack/realtime-claims-event-pipeline)
**Kafka → Spark Structured Streaming → Apache Iceberg**, with the data contract enforced by Confluent Schema Registry.

```mermaid
flowchart LR
    P["Producer<br/>Avro"] --> K[("Kafka")]
    SR["Schema Registry<br/>BACKWARD compat"] -.-> P
    K --> S["Spark Structured<br/>Streaming"]
    S -->|valid · idempotent MERGE| I[("Iceberg")]
    S -->|invalid + reason| D[("Dead-letter topic")]
```

- Schema evolution demo: a compatible change is accepted, a breaking change is rejected by the registry
- Every record checked for wire format, schema ID, Avro decode and **9 business rules**; failures are replayable from the dead-letter topic
- **Idempotent `MERGE`** on `claim_event_id`, so replays and restarts never duplicate rows
- 86 unit tests plus a parity test between the Python and Spark rule implementations
- `Kafka` `Spark` `Iceberg` `Avro` `Schema Registry` `Docker`

### 🏔️ [AWS Serverless Streaming Lakehouse](https://github.com/rugweshreddygankidi-stack/aws-serverless-streaming-lakehouse)
**Kinesis → Lambda → Apache Iceberg on S3 → Athena**, fully defined in Terraform.

```mermaid
flowchart LR
    P[Producer] --> K[Kinesis]
    K -->|batched| L["Lambda<br/>validate + dedupe"]
    L --> I[("Iceberg on S3<br/>Glue catalog")]
    L --> D[("S3 dead-letter")]
    K -.->|failed batches| Q[SQS]
    I --> A["Athena<br/>dedup view"]
```

- Batched writes (5,000 records or 60 s) to control cost and small-file counts
- Bad rows dead-lettered to S3, poison batches isolated with bisect-on-error and an SQS failure destination
- `events_dedup` view guarantees one row per `event_id` despite at-least-once delivery
- Athena workgroup with a bytes-scanned cutoff as a cost guardrail; 58 unit tests and CI
- `AWS` `Terraform` `Iceberg` `Athena` `Glue` `pytest`

### 🧠 [RAG Data Pipeline](https://github.com/rugweshreddygankidi-stack/rag-data-pipeline)
**The data-engineering half of RAG:** Airflow + PostgreSQL/pgvector, focused on making retrieval trustworthy.

```mermaid
flowchart LR
    S[Inbox] --> R{Changed?}
    R -->|yes| P["PII redaction"] --> C["Chunk + overlap"] --> D["Dedupe"] --> E["Embed"] --> V[("pgvector HNSW")]
    V --> Q["Great Expectations<br/>+ freshness"]
    V --> EV["recall@5 / MRR"]
```

- Hourly **incremental** ingestion by document hash; atomic per-document replacement
- PII redaction (emails, SSNs, phones, Luhn-valid cards) before chunking and embedding
- Great Expectations gates, freshness checks, and a daily **retrieval evaluation** DAG (recall@5, MRR)
- `Airflow` `pgvector` `Great Expectations` `Docker` `fastembed`

### 🔬 [ML-DriftNet: Domain Adaptation of GPT-2](https://github.com/rugweshreddygankidi-stack/ml-driftnet)
Master's team project (DSC 550, UMass Dartmouth, Spring 2026), project lead. GPT-2 Small fine-tuned on 1,493 ML Q&A pairs and evaluated against the baseline with a five-metric framework.

| Metric | GPT-2 baseline | Fine-tuned |
|:--|:--:|:--:|
| Perplexity on held-out Q&A | 72.85 | **31.83** (−56.1%) |
| Multiple-choice answer ranking | 26.7% | **86.7%** |
| Log-probability per token | −4.48 | **−2.92** |

- Documents honestly which metrics reuse training questions, so the evidence for generalization is not overstated
- `PyTorch` `Hugging Face` `Colab` `Evaluation design`

### 🎯 [Job Finder](https://github.com/rugweshreddygankidi-stack/job-finder) *(source private, case study public)*
A daily pipeline that collects postings from ~480 career sites and job boards, filters and deduplicates them, scores them against a resume with LLM APIs under a daily budget planner, and learns from apply/remove decisions.

- First 4 days of real use: **9,830 postings → 91% removed by rule filters → 168 shown** in ranked daily lists
- ~7,400 lines of Python; an offline end-to-end test checks 51 behaviors in CI
- `Python` `SQLite` `pandas` `LLM APIs` `GitHub Actions`

---

## 🧭 Engineering Principles

| Principle | In practice |
|:--|:--|
| **Contracts before code** | Schema Registry with backward compatibility; breaking changes fail fast |
| **Never drop, never duplicate** | Dead-letter queues for bad data; idempotent writes for replays |
| **Quality is a gate, not a report** | Expectations and freshness checks fail the pipeline |
| **Measure it** | Retrieval metrics, throughput logs, and results that state their own limits |
| **Reproducible** | Terraform, Docker, Makefiles, and CI so anyone can run it |

---

## 📫 Let's Connect

I'm looking for **Data Engineering** opportunities (batch, streaming, lakehouse, and Gen AI data platforms).

[![LinkedIn](https://img.shields.io/badge/Connect%20on%20LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/rugwesh-reddy-gankidi/)

<div align="center">

<img src="https://capsule-render.vercel.app/api?type=waving&color=0:0ea5e9,100:0f172a&height=100&section=footer" width="100%" alt=""/>

</div>
