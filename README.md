<div align="center">

<img src="assets/header.svg" alt="Rugwesh Reddy Gankidi - Data Engineer. Streaming, lakehouse and RAG data layers." width="100%"/>

<img src="assets/roles.svg" alt="Building streaming pipelines, designing lakehouses, making RAG retrieval trustworthy, evaluating ML models" width="720"/>

<br/>

[![LinkedIn](https://img.shields.io/badge/LinkedIn-rugwesh--reddy--gankidi-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/rugwesh-reddy-gankidi/)
[![GitHub](https://img.shields.io/badge/GitHub-rugweshreddygankidi--stack-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/rugweshreddygankidi-stack)
![Location](https://img.shields.io/badge/Massachusetts-USA-0ea5e9?style=for-the-badge&logo=googlemaps&logoColor=white)
![Focus](https://img.shields.io/badge/Open%20to-Data%20Engineering%20roles-16a34a?style=for-the-badge)

**[About](#about)** &nbsp;·&nbsp; **[At a glance](#at-a-glance)** &nbsp;·&nbsp; **[How I build](#how-i-build-pipelines)** &nbsp;·&nbsp; **[Projects](#featured-projects)** &nbsp;·&nbsp; **[Skills](#skills)** &nbsp;·&nbsp; **[Next](#whats-next)** &nbsp;·&nbsp; **[Connect](#lets-connect)**

</div>

---

## About

I'm a **Data Engineer** focused on batch and streaming pipelines, lakehouse storage, and the data foundations behind **retrieval-augmented generation (RAG)** systems. I care about the parts that make data trustworthy in production: **data contracts, idempotent writes, dead-letter handling, quality gates, and measurable evaluation**.

My projects are built to be run, not just read. Each one has tests, CI or a reproducible local setup, and a results section that says what was measured and what was only estimated.

<table>
<tr>
<td width="50%" valign="top">

**What I work on**
- Streaming ingestion with Kafka, Kinesis and Spark Structured Streaming
- Open table formats (Apache Iceberg) on S3 and local catalogs
- Data layers for RAG: PII redaction, dedup, embeddings, pgvector
- Infrastructure as code and CI for data pipelines

</td>
<td width="50%" valign="top">

**Ask me about**
- Exactly-once outcomes from at-least-once delivery
- Schema evolution and registry compatibility rules
- Retrieval evaluation with recall@k and MRR
- Evaluating fine-tuned language models honestly

</td>
</tr>
</table>

---

## At a glance

<div align="center">

<table>
<tr>
<td align="center" width="25%"><h2>5</h2><sub>end-to-end projects<br/>streaming, lakehouse, RAG, ML, LLM ranking</sub></td>
<td align="center" width="25%"><h2>144</h2><sub>unit tests across the<br/>two streaming pipelines</sub></td>
<td align="center" width="25%"><h2>9,830</h2><sub>job postings processed in the<br/>first 4 days of Job Finder</sub></td>
<td align="center" width="25%"><h2>−56%</h2><sub>held-out perplexity after<br/>fine-tuning GPT-2 Small</sub></td>
</tr>
</table>

</div>

---

## How I build pipelines

The same reliability pattern shows up across my streaming and RAG projects. This is the reference design I reuse:

```mermaid
flowchart LR
    SRC["Source<br/>events / documents"] --> CON{"Contract<br/>schema + rules"}
    CON -- valid --> TX["Transform<br/>dedupe · redact · enrich"]
    CON -- invalid --> DLQ[("Dead-letter<br/>replayable, with reason")]
    TX --> W["Idempotent write<br/>MERGE / atomic swap"]
    W --> STORE[("Iceberg · pgvector")]
    STORE --> QG["Quality gates<br/>expectations + freshness"]
    STORE --> EV["Evaluation<br/>recall@k · MRR · counts"]
    QG --> OBS["Metrics + alerts"]
    EV --> OBS
```

| Principle | How it shows up | Where |
|:--|:--|:--|
| **Contracts before code** | Avro + Schema Registry with BACKWARD compatibility; breaking changes are rejected | Claims pipeline |
| **Never drop, never duplicate** | Dead-letter topic/bucket for bad data; idempotent `MERGE` and dedup views for replays | Claims, Lakehouse |
| **Quality is a gate, not a report** | Great Expectations and freshness checks fail the DAG | RAG pipeline |
| **Cost-aware by design** | Batched writes, partition pruning, Athena bytes-scanned cutoff | Lakehouse |
| **Measure it, and say what it means** | Retrieval metrics, labeled caveats on which results generalize | RAG, ML-DriftNet |
| **Reproducible** | Terraform, Docker Compose, Makefiles, CI | All |

---

## Featured projects

> Click a project to expand its architecture and highlights.

<details open>
<summary><b>🌊 Real-Time Claims Event Pipeline</b> &nbsp;·&nbsp; Kafka → Spark Structured Streaming → Iceberg</summary>

<br/>

A streaming pipeline for health-insurance claim events with the data contract enforced by Confluent Schema Registry. Runs locally in Docker on synthetic data.

```mermaid
flowchart LR
    P["Producer<br/>Avro"] --> K[("Kafka")]
    SR["Schema Registry<br/>BACKWARD compat"] -.-> P
    K --> S["Spark Structured<br/>Streaming"]
    S -->|valid · idempotent MERGE| I[("Iceberg")]
    S -->|invalid + reason| D[("Dead-letter topic")]
    S -.-> M["progress.jsonl<br/>throughput metrics"]
```

- **Data contract:** a compatible schema change is accepted; a breaking one is rejected by the registry
- **Bad data handling:** every record is checked for wire format, schema ID, Avro decode and 9 business rules; failures keep their original bytes and an error code so they can be replayed
- **Delivery:** at-least-once checkpoints plus `MERGE ... WHEN NOT MATCHED` on `claim_event_id`, so replays never duplicate rows
- **Testing:** 86 unit tests and a parity test that checks Spark rule expressions against the Python rules

`Kafka` `Spark Structured Streaming` `Apache Iceberg` `Avro` `Schema Registry` `Docker`

**[View repository →](https://github.com/rugweshreddygankidi-stack/realtime-claims-event-pipeline)**

</details>

<details open>
<summary><b>🏔️ AWS Serverless Streaming Lakehouse</b> &nbsp;·&nbsp; Kinesis → Lambda → Iceberg on S3 → Athena</summary>

<br/>

A fully serverless clickstream pipeline with all infrastructure defined in Terraform.

```mermaid
flowchart LR
    P[Producer] --> K[Kinesis]
    K -->|"≤5,000 records / 60 s"| L["Lambda<br/>validate + dedupe"]
    L --> I[("Iceberg on S3<br/>Glue catalog")]
    L --> D[("S3 dead-letter")]
    K -.->|failed batches| Q[SQS]
    I --> A["Athena<br/>dedup view"]
```

- **Cost control:** one Athena `INSERT` per batch instead of per record, partitioning by `event_date`, and a workgroup bytes-scanned cutoff
- **Failure isolation:** bisect-on-error plus an SQS failure destination so a poison record cannot block a shard
- **Correctness:** an `events_dedup` view guarantees one row per `event_id` despite at-least-once delivery
- **Engineering:** 58 pytest cases, GitHub Actions CI, cost-allocation tags on every resource

`AWS Kinesis` `Lambda` `Iceberg` `Athena` `Glue` `SQS` `Terraform` `pytest`

**[View repository →](https://github.com/rugweshreddygankidi-stack/aws-serverless-streaming-lakehouse)**

</details>

<details>
<summary><b>🧠 RAG Data Pipeline</b> &nbsp;·&nbsp; Airflow + PostgreSQL/pgvector</summary>

<br/>

The data-engineering half of a RAG system. It does not call an LLM; it makes the retrieval layer trustworthy.

```mermaid
flowchart LR
    S[Inbox] --> R{Changed?<br/>doc hash}
    R -- no --> X[Skip]
    R -- yes --> P["PII redaction"] --> C["Chunk + overlap"] --> D["Dedupe"] --> E["Embed"] --> V[("pgvector<br/>HNSW")]
    V --> Q["Great Expectations<br/>+ freshness"]
    V --> EV["recall@5 / MRR"]
```

- **Incremental:** hourly DAG processes only documents whose hash changed; changed documents are replaced in one transaction
- **Privacy:** emails, SSNs, phone numbers and Luhn-valid card numbers are redacted before chunking and embedding
- **Quality gates:** Great Expectations checks plus a freshness check that fails the DAG
- **Evaluation:** a daily DAG computes recall@5 and MRR on a labeled known-item benchmark and can fail below a threshold

`Airflow` `pgvector` `PostgreSQL` `Great Expectations` `fastembed` `Docker`

**[View repository →](https://github.com/rugweshreddygankidi-stack/rag-data-pipeline)**

</details>

<details>
<summary><b>🔬 ML-DriftNet</b> &nbsp;·&nbsp; Domain adaptation of GPT-2 (Master's team project, project lead)</summary>

<br/>

UMass Dartmouth, DSC 550, Spring 2026. Does fine-tuning GPT-2 Small (124M parameters) on 1,493 structured ML Q&A pairs produce measurable domain adaptation? Evaluated against the baseline with a five-metric framework.

| Metric | GPT-2 baseline | Fine-tuned |
|:--|:--:|:--:|
| Perplexity on held-out Q&A | 72.85 | **31.83** (−56.1%) |
| Multiple-choice answer ranking | 26.7% | **86.7%** |
| Log-probability per token | −4.48 | **−2.92** |

- Held-out perplexity is the cleanest evidence: lower on all 20 pairs never seen in training
- The write-up states which metrics reuse training questions, so the generalization claim is not overstated
- Trained on a Colab T4 GPU in about 4.5 minutes with fp16 autocast and gradient checkpointing

`PyTorch` `Hugging Face` `Colab` `Evaluation design`

**[View repository →](https://github.com/rugweshreddygankidi-stack/ml-driftnet)** &nbsp;·&nbsp; **[Interactive results report →](https://rugweshreddygankidi-stack.github.io/ml-driftnet/results/research_report.html)**

</details>

<details>
<summary><b>🎯 Job Finder</b> &nbsp;·&nbsp; Multi-source collection + LLM ranking (source private, case study public)</summary>

<br/>

A daily pipeline that collects postings from about 480 career sites and job boards, deduplicates and filters them, scores them against a resume with LLM APIs under a daily budget planner, and learns from apply/remove decisions.

```mermaid
flowchart LR
    A["~480 sources"] --> B["Normalize + dedupe"]
    B --> C["Rule + employer filters"]
    C --> D["Free local pre-score"]
    D --> E["LLM scoring<br/>within daily budget"]
    E --> F["Rank + ghost-job signals"]
    F --> G["HTML list + Excel tracker"]
    G -.->|apply / remove| F
```

- **First 4 days of real use:** 9,830 postings from 4,064 companies → 91% removed by rule filters → 168 shown in ranked lists
- **Engineering:** about 7,400 lines of Python; an offline end-to-end test checks 51 behaviors in CI
- **Budgeting:** a planner splits free LLM API quotas across runs so evening runs are never starved

`Python` `SQLite` `pandas` `LLM APIs` `GitHub Actions`

**[View case study →](https://github.com/rugweshreddygankidi-stack/job-finder)** &nbsp;·&nbsp; **[Demo results page →](https://rugweshreddygankidi-stack.github.io/job-finder/results/demo_Todays_Jobs.html)**

</details>

---

## Skills

<details open>
<summary><b>Data engineering core</b></summary>

<br/>

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat-square&logo=postgresql&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Spark Structured Streaming](https://img.shields.io/badge/Structured%20Streaming-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat-square&logo=dbt&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat-square&logo=snowflake&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-150458?style=flat-square&logo=pandas&logoColor=white)

</details>

<details open>
<summary><b>Lakehouse, storage and formats</b></summary>

<br/>

![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-3b82f6?style=flat-square)
![Amazon S3](https://img.shields.io/badge/Amazon%20S3-569A31?style=flat-square&logo=amazons3&logoColor=white)
![AWS Glue](https://img.shields.io/badge/AWS%20Glue-FF9900?style=flat-square&logo=amazonaws&logoColor=white)
![Amazon Athena](https://img.shields.io/badge/Amazon%20Athena-8C4FFF?style=flat-square&logo=amazonaws&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=flat-square&logo=postgresql&logoColor=white)
![pgvector](https://img.shields.io/badge/pgvector-336791?style=flat-square&logo=postgresql&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-003B57?style=flat-square&logo=sqlite&logoColor=white)
![Avro](https://img.shields.io/badge/Apache%20Avro-0A66C2?style=flat-square)
![Schema Registry](https://img.shields.io/badge/Confluent%20Schema%20Registry-173361?style=flat-square)

</details>

<details open>
<summary><b>Cloud, infrastructure and delivery</b></summary>

<br/>

![AWS](https://img.shields.io/badge/AWS-232F3E?style=flat-square&logo=amazonaws&logoColor=white)
![AWS Lambda](https://img.shields.io/badge/AWS%20Lambda-FF9900?style=flat-square&logo=awslambda&logoColor=white)
![Amazon Kinesis](https://img.shields.io/badge/Amazon%20Kinesis-FF9900?style=flat-square&logo=amazonaws&logoColor=white)
![Amazon SQS](https://img.shields.io/badge/Amazon%20SQS-FF4F8B?style=flat-square&logo=amazonsqs&logoColor=white)
![IAM](https://img.shields.io/badge/AWS%20IAM-DD344C?style=flat-square&logo=amazonaws&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-844FBA?style=flat-square&logo=terraform&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-2088FF?style=flat-square&logo=githubactions&logoColor=white)
![Git](https://img.shields.io/badge/Git-F05032?style=flat-square&logo=git&logoColor=white)
![Make](https://img.shields.io/badge/Make-6D00CC?style=flat-square&logo=gnu&logoColor=white)
![Bash](https://img.shields.io/badge/Bash-4EAA25?style=flat-square&logo=gnubash&logoColor=white)

</details>

<details open>
<summary><b>Data quality and testing</b></summary>

<br/>

![Great Expectations](https://img.shields.io/badge/Great%20Expectations-FF6310?style=flat-square)
![pytest](https://img.shields.io/badge/pytest-0A9EDC?style=flat-square&logo=pytest&logoColor=white)
![Data contracts](https://img.shields.io/badge/Data%20Contracts-0ea5e9?style=flat-square)
![Idempotency](https://img.shields.io/badge/Idempotent%20Writes-0ea5e9?style=flat-square)
![Dead-letter handling](https://img.shields.io/badge/Dead--Letter%20Handling-0ea5e9?style=flat-square)
![PII redaction](https://img.shields.io/badge/PII%20Redaction-0ea5e9?style=flat-square)

</details>

<details open>
<summary><b>Gen AI and machine learning</b></summary>

<br/>

![PyTorch](https://img.shields.io/badge/PyTorch-EE4C2C?style=flat-square&logo=pytorch&logoColor=white)
![Hugging Face](https://img.shields.io/badge/Hugging%20Face-FFD21E?style=flat-square&logo=huggingface&logoColor=black)
![Jupyter](https://img.shields.io/badge/Jupyter%20%2F%20Colab-F37626?style=flat-square&logo=jupyter&logoColor=white)
![RAG](https://img.shields.io/badge/RAG%20Data%20Layers-7c3aed?style=flat-square)
![Embeddings](https://img.shields.io/badge/Embeddings%20%2B%20HNSW-7c3aed?style=flat-square)
![Retrieval evaluation](https://img.shields.io/badge/recall%40k%20%2F%20MRR-7c3aed?style=flat-square)
![Fine-tuning](https://img.shields.io/badge/Fine--Tuning%20GPT--2-7c3aed?style=flat-square)
![LLM APIs](https://img.shields.io/badge/LLM%20APIs-7c3aed?style=flat-square)

</details>

<details>
<summary><b>Where each skill is proven</b> (click to expand)</summary>

<br/>

| Skill | Evidence |
|:--|:--|
| Kafka, Schema Registry, Avro | Claims pipeline: contract enforcement, schema-evolution demo, dead-letter topic |
| Spark Structured Streaming | Claims pipeline: decode, validate, idempotent Iceberg `MERGE`, progress listener |
| Iceberg, Glue, Athena, S3 | Lakehouse: partitioned tables, dedup view, bytes-scanned guardrail |
| Terraform, IAM, SQS, Lambda, Kinesis | Lakehouse: complete infrastructure as code with failure isolation |
| Airflow, pgvector, Great Expectations | RAG pipeline: incremental DAG, HNSW index, quality and freshness gates |
| Retrieval evaluation | RAG pipeline: recall@5 and MRR on a labeled benchmark |
| PyTorch, Hugging Face, fine-tuning | ML-DriftNet: GPT-2 Small fine-tune with five-metric evaluation |
| LLM APIs, SQLite, pandas, CI | Job Finder: budget-planned LLM scoring, history database, offline end-to-end test |

</details>

---

## What's next

Mostly taken from the "next steps" and "limitations" lists in my own repositories, plus one learning goal:

- [ ] Scheduled Iceberg maintenance (`OPTIMIZE ... REWRITE DATA USING BIN_PACK`, `VACUUM`) and CloudWatch alarms on the lakehouse
- [ ] Schema-id-aware decoding so one Kafka topic can safely carry mixed schema versions
- [ ] Move the claims pipeline from a local catalog to an object-store catalog (Glue, Nessie or REST)
- [ ] LoRA / QLoRA comparison and a larger held-out set for ML-DriftNet
- [ ] Snowflake and dbt modeling layer on top of the lakehouse outputs

---

## Let's connect

I'm looking for **Data Engineering** roles in batch, streaming, lakehouse and Gen AI data platforms.

<div align="center">

[![LinkedIn](https://img.shields.io/badge/Connect%20on%20LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/rugwesh-reddy-gankidi/)
[![Repositories](https://img.shields.io/badge/Browse%20my%20repositories-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/rugweshreddygankidi-stack?tab=repositories)

<sub>Massachusetts, USA</sub>

</div>
