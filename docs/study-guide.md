# Arktrace: The 3-Week Intensive Study Guide

This guide is a 21-day curriculum for engineers and analysts to master the Arktrace codebase, the underlying maritime domain, and the causal inference mathematics required for the Cap Vista submission.

---

## Week 1: Background and Data Analytics

### Day 1: The Shadow Fleet Problem & Evasion Tactics
The "Shadow Fleet" (or Dark Fleet) represents one of the most significant challenges in modern maritime security. These are vessels—often older, under-insured, and owned by opaque shell companies—that operate specifically to bypass international sanctions. They move restricted cargo like oil or coal while actively evading detection by national authorities.

Common evasion tactics include "Going Dark" (switching off AIS transponders), GPS spoofing (broadcasting false positions), and frequent "Flag Hopping" (changing the country of registration to reset inspection history). These aren't just technical glitches; they are deliberate operational choices designed to create a "Grey Zone" where illicit trade can flourish undetected by standard port-state controls.

Traditional monitoring systems rely on anomaly detection—spotting behavior that looks "unusual." However, in the maritime world, "unusual" is common. A fishing boat may switch off its AIS to hide its spots from competitors; a storm might cause a legitimate transmission gap. This creates a massive "False Positive" problem for analysts.

Arktrace moves beyond simple anomalies to **Causal Intent**. Instead of asking "Does this vessel look strange?", we ask "Did this vessel's behavior change *specifically* because of a sanction announcement?". By correlating behavior with geopolitical events, we can separate genuine evaders from noisy background activity, allowing patrol resources to be dispatched with much higher confidence.

- **Internal Docs:** [docs/background.md](background.md), [docs/scenarios.md](scenarios.md).
- **External Docs:** [OFAC Sanctions Search](https://sanctionssearch.ofac.treas.gov/), [Atlantic Council: Russia's Shadow Fleet](https://www.atlanticcouncil.org/blogs/econographics/russias-shadow-fleet-is-growing/).
- **Internal Source:** `src/ingest/sanctions.py`, `src/ingest/vessel_registry.py`.

### Day 2: The Cap Vista Challenge & Mission Alignment
Arktrace is specifically designed to address the **Cap Vista Solicitation 5.0 (Challenge 1: Maritime Security Data Analytics)**. The challenge asks for solutions that can identify "sophisticated AIS spoofing, frequent name/flag changes, and illicit ship-to-ship (STS) transfers" to bypass international sanctions.

The primary goal is to provide **Intelligence Analysts** and **Operational Commanders** with a 60–90 day lead time before a vessel is officially designated on a sanctions list. By catching these threats early, security agencies can intervene before the vessel enters sensitive waters or completes its illicit cargo transfer.

A key requirement of the challenge is that the solution must have a "low computational cost to maximize edge deployments." This means the tool must be able to run on modest hardware at a Port Operations Center or even on a patrol vessel, rather than requiring a massive, expensive cloud-based supercomputer.

Understanding the "Annex A" submission requirements is critical. We frame Arktrace as a **TRL 6 baseline** system that is ready for a 7-week trial in the Singapore Strait. Every feature we build must map back to the specific "Shadow Fleet" behaviors named in the Cap Vista solicitation.

- **Internal Docs:** [docs/trial-specification.md](trial-specification.md).
- **Internal Source:** `../arktrace-commercial/inputs/challenge-statements.md`.

### Day 3: Tactical Edge Vision & Architecture
The architecture of Arktrace is driven by the constraints of the **Tactical Edge**. In maritime security, analysts and patrol officers often operate in environments with limited, expensive, or zero internet connectivity. Relying on a massive cloud-based "Data Lake" is not an option when you are 200 miles offshore on a patrol vessel.

Arktrace is built on a **"Local-First"** philosophy. This means that the entire stack—from the analytical database and the feature engineering engine to the LLM-powered brief generator—must run on a single, detached laptop or shipboard server. This ensures that the system is fully operational even when air-gapped from the global network.

By choosing in-process tools like DuckDB and Polars, we eliminate the complexity and overhead of managing client-server architectures like PostgreSQL. This doesn't just improve portability; it dramatically increases performance. Because the data and the computation live in the same memory space, we can process millions of AIS points in seconds, which would take minutes or hours over a network connection.

Finally, this architecture provides total **Data Sovereignty**. In national security contexts, moving sensitive vessel tracking data to a third-party cloud provider is often a compliance nightmare. With Arktrace, the data stays on the device. All scores, graphs, and LLM-generated summaries are computed locally, satisfying the strictest security and privacy requirements.

- **Internal Docs:** [docs/architecture.md](architecture.md), [docs/technical-solution.md](technical-solution.md).
- **External Docs:** [Local-First Software (Ink & Switch)](https://www.inkandswitch.com/local-first/).
- **Internal Source:** `src/storage/config.py`, `docker-compose.yml`.

### Day 4: Data Sources & Ingestion (AIS, Sanctions, GDELT)
Arktrace fuses multiple independent data streams to build its "Common Operating Picture." The most critical stream is **AIS (Automatic Identification System)**, which provides real-time and historical vessel positions. We ingest this via WebSockets (from aisstream.io) or from historical Parquet archives (like Marine Cadastre).

We supplement AIS with **Sanctions Lists** (OFAC, EU, UN) and **Vessel Registries** (Equasis/ITU). This allows us to know not just *where* a vessel is, but *who* owns it and whether they have a history of regulatory violations. This fusion of "Behavioral" and "Identity" data is what enables our multi-dimensional scoring.

A unique feature of Arktrace is the ingestion of **GDELT (Global Database of Events, Language, and Tone)**. GDELT provides a real-time feed of geopolitical events—sanction announcements, maritime conflicts, and corporate designations. We use these event dates as the "Treatment" triggers for our Causal Inference model.

Ingestion in Arktrace is designed to be **Source-Agnostic**. Whether the data comes from a live satellite feed, a CSV drop, or a legacy database, it is normalized into a standard schema in our analytical store. This ensures that the rest of the pipeline can function identically regardless of where the data originated.

- **Internal Docs:** [docs/pipeline-operations.md](pipeline-operations.md).
- **External Docs:** [GDELT Project](https://www.gdeltproject.org/), [aisstream.io API](https://aisstream.io/documentation).
- **Internal Source:** `src/ingest/`, `src/ingest/gdelt.py`, `src/ingest/sanctions.py`.

### Day 5: Analytical Storage with DuckDB
At the heart of Arktrace is **DuckDB**, an analytical (OLAP) database engine that is widely known as the "SQLite for Analytics." Unlike traditional databases that store data in rows, DuckDB uses a columnar format. This is critical for maritime analytics where we often need to scan millions of AIS positions for a few specific columns (like MMSI and Timestamp) to detect gaps or jumps.

The ingestion pipeline handles a variety of data sources: live AIS WebSocket streams, historical Parquet files, and custom CSV drops. DuckDB's ability to "query Parquet files natively" is a game-changer for us. It means we don't have to "load" data into a database before using it; we can just point DuckDB at a folder of files and start running SQL, making the pipeline incredibly agile.

We also use DuckDB to manage our **Analytical Store** (`mpol.duckdb`). This file persists our engineered features, sanction lists, and trade statistics. Because it's a single file on disk, it's easy to back up, share with other analysts, or move from a development machine to an operational patrol laptop without any "database migration" overhead.

For engineers, mastering the Arktrace ingestion layer means understanding how to use DuckDB's SQL extensions for time-series and geospatial data. We rely heavily on window functions and spatial joins to detect Ship-to-Ship (STS) candidates—identifying pairs of vessels that loitered in close proximity for several hours far from any port.

- **Internal Docs:** [docs/pipeline-operations.md](pipeline-operations.md).
- **External Docs:** [DuckDB Documentation](https://duckdb.org/docs/).
- **Internal Source:** `src/api/db.py`, `src/ingest/ais_csv.py`, `src/ingest/ais_stream.py`.

### Day 6: Feature Engineering I: Movement & Spoofing
Turning raw AIS points into actionable intelligence requires **Feature Engineering**. Arktrace calculates 19 distinct signals for every vessel. On Day 6, we focus on **Movement Signals**—the bread and butter of vessel tracking.

A core focus of our engineering is detecting **AIS Spoofing**. If a vessel broadcasts two positions that are 100 miles apart but only 10 minutes have passed, it has an "implied speed" of 600 knots. Since tankers don't fly, we know one of those coordinates is fake. We track these as `position_jump_count`, which is a high-value signal for identifying vessels trying to hide their actual location.

We also calculate "AIS Gaps" (Dark periods). A gap is simply a period where a vessel stops transmitting its location. While gaps can be caused by poor satellite coverage, frequent or lengthy gaps near sensitive maritime hubs are a primary indicator of illicit ship-to-ship (STS) transfers or unauthorized port calls.

To handle this at scale, we use **Polars**, a lightning-fast DataFrame library written in Rust. Traditional tools like Pandas can be slow and memory-intensive when processing millions of records. Polars uses "Lazy Evaluation," meaning it builds an optimized plan for all your calculations and executes them in parallel across every CPU core, making it the perfect engine for our edge architecture.

- **Internal Docs:** [docs/feature-engineering.md](feature-engineering.md).
- **External Docs:** [Polars: User Guide](https://docs.pola.rs/user-guide/index.html).
- **Internal Source:** `src/features/movement.py`, `src/features/sts.py`.

### Day 7: Feature Engineering II: Identity & Trade
While movement tells us *what* a vessel is doing, **Identity and Trade Signals** tell us *who* is doing it and *why*. Day 7 focuses on "Identity Churn"—the frequent changing of vessel names, flags, and ownership to break the "paper trail" for inspectors.

We calculate signals like `name_changes_2y` and `flag_changes_2y`. A legitimate vessel might change its name once in its decade-long life; a shadow fleet vessel might change its name three times in 18 months. We also look at the "High-Risk Flag Ratio"—vessels registered in countries known for lax regulatory oversight (e.g., Comoros, Gabon).

We also incorporate **Trade Flow Data** from sources like UN Comtrade. By correlating a vessel's declared cargo (from port records) against its estimated capacity and its actual movement history, we can detect "Trade Mismatches." If a vessel claims to be carrying 50,000 tons of crude oil but its AIS draught suggests it is empty, we have a major red flag.

The final output of Week 1 is the **Feature Matrix**. This is a flattened table where each row represents a vessel and each column represents one of our 19 signals. This matrix is the "fuel" for the more advanced Data Science and Math models we will study in Week 2.

- **Internal Docs:** [docs/feature-engineering.md](feature-engineering.md).
- **External Docs:** [UN Comtrade Database](https://comtradeplus.un.org/).
- **Internal Source:** `src/features/identity.py`, `src/features/trade_mismatch.py`.

---

## Week 2: Data Science and Math

### Day 8: Ownership Graphs & Network Proximity
Illicit vessels rarely operate in isolation. They are usually part of a broader network of shell companies designed to hide the "Beneficial Owner." If one vessel in a company is caught evading sanctions, it is highly likely that other vessels owned by the same group are also high-risk, even if they haven't been caught yet.

We use **LanceDB** to store and traverse this **Ownership Graph**. LanceDB is an embedded database that excels at managing both vector data (for LLM searches) and relationship data (for graph traversal). It allows us to perform sub-second "Graph Walks" to find the connection between a specific vessel and any known sanctioned entity in our database.

The primary signal here is `sanctions_distance`. This is a BFS (Breadth-First Search) hop count. A distance of 0 means the vessel is directly on a sanctions list; a distance of 1 means its direct owner is sanctioned; a distance of 2 means it's connected through a parent holding company. This network-based approach allows us to "propagate" risk signals from known evaders to hidden ones.

For a self-learner, the key is understanding how we use **Lance Graph** to store millions of nodes and edges as columnar files on disk. This allows us to run graph-wide "Backtracking" (`scripts/run_backtracking.py`)—once a vessel is confirmed as an evader by a patrol team, we instantly re-score every other vessel in its corporate network to surface the next set of likely threats.

- **Internal Docs:** [docs/scoring-model.md](scoring-model.md) (Graph section).
- **External Docs:** [LanceDB Documentation](https://lancedb.github.io/lancedb/).
- **Internal Source:** `src/features/ownership_graph.py`, `src/graph/store.py`.

### Day 9: Unsupervised Learning I: Pattern of Life (HDBSCAN)
In maritime security, we often don't have "labels"—we don't know for sure which vessels are evading sanctions until they are caught. This is why we rely on **Unsupervised Learning**. We use the data itself to define what is "normal" and then flag everything that stands out.

We use **HDBSCAN** to define the **Maritime Pattern of Life (MPOL)**. HDBSCAN is a density-based clustering algorithm. It looks at the feature vectors of all vessels and groups them into "herds" of normal behavior (e.g., container ships on a regular weekly route). Any vessel that doesn't fit into a dense cluster is classified as "noise" and receives a baseline anomaly boost.

Unlike traditional K-Means clustering, HDBSCAN doesn't require us to specify the number of clusters beforehand. It "discovers" the number of clusters based on the density of the data. This is critical for maritime data where the "normal" patterns can shift depending on the season, the region, or global economic conditions.

For the developer, the key is understanding how we use the `HDBSCAN` implementation from `scikit-learn` to process our 19-dimensional feature matrix. The resulting "Cluster ID" is not just a label; it is a reference point that allows us to say: "This vessel is not acting like a normal tanker in the Singapore Strait."

- **Internal Docs:** [docs/scoring-model.md](scoring-model.md) (Algorithms section).
- **External Docs:** [HDBSCAN: How it works](https://hdbscan.readthedocs.io/en/latest/how_hdbscan_works.html).
- **Internal Source:** `src/score/composite.py`.

### Day 10: Unsupervised Learning II: Anomaly Detection (Isolation Forest)
Once we have defined the "herds" of normal behavior with HDBSCAN, we use **Isolation Forest** to detect specific anomalies. While HDBSCAN tells us what is "normal," Isolation Forest specifically looks for points that are "few and different" and thus easiest to isolate from the rest of the fleet.

The intuition behind Isolation Forest is simple: if you randomly pick a feature and a split-point, an anomalous vessel (one with extreme values) will be isolated from the herd very quickly. A "normal" vessel, buried deep in a herd, will take many more splits to isolate. The faster a vessel is isolated, the higher its anomaly score.

Isolation Forest is perfect for our edge architecture because it is computationally efficient and doesn't require a massive training set. It is an "outlier-first" algorithm that maps directly to our mission of finding the "needle in the haystack."

For the learner, the key parameter to learn is "contamination"—this is our estimate of how many vessels in the fleet are truly anomalous. We calibrate this parameter against our historical validation sets to ensure we are finding genuine threats without overwhelming analysts with false positives.

- **Internal Docs:** [docs/scoring-model.md](scoring-model.md) (Algorithms section).
- **External Docs:** [Scikit-Learn: Isolation Forest](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html).
- **Internal Source:** `src/score/anomaly.py`, `src/score/composite.py`.

### Day 11: Explainability & Game Theory (SHAP)
In defense and intelligence applications, "Black Box" AI is a failure. An analyst cannot dispatch a multi-million dollar patrol vessel simply because an algorithm says "Risk: 0.87." They need to know **why**. We call this "Closing the Trust Gap." If the AI cannot explain its reasoning, the human operator will eventually ignore it.

We solve this using **SHAP (Shapley Additive Explanations)**. SHAP is a method from cooperative Game Theory that "fairly" distributes the credit for a score among all the input features. For every vessel on our watchlist, SHAP identifies exactly which signals pushed the score up and which pushed it down.

When an analyst clicks on a vessel, they don't just see a score; they see a **Signal Breakdown**: "+0.34 from AIS Gaps," "+0.21 from 1-hop Sanctions Link," "-0.05 from long Name Change history." This allows the analyst to verify the AI's logic against their own domain expertise. If the "reasons" make sense, the "score" becomes actionable evidence.

For the self-learner, look at the `explain_vessel` function in `src/score/composite.py`. We use a "TreeExplainer" which is optimized for our Isolation Forest model. This allows us to generate these detailed explanations in under 100 milliseconds, ensuring the dashboard remains fast and responsive for the operator.

- **Internal Docs:** [docs/technical-solution.md](technical-solution.md) (Explainability section).
- **External Docs:** [SHAP: Game Theory explainability](https://shap.readthedocs.io/en/latest/overviews.html).
- **Internal Source:** `src/score/composite.py` (`explain_vessel` function).

### Day 12: Causal Inference I: Quasi-Experimental Design (DiD)
The primary innovation of Arktrace is moving from "What looks unusual?" to **"What is causally responding to sanctions?"**. To do this, we use a statistical technique called **Difference-in-Differences (DiD)**. This is a quasi-experimental design borrowed from economics and public policy research.

Imagine a sanction announcement is a "Treatment" applied to a certain group of vessels (those with a graph link to the sanctioned country). We compare the behavior of this **Treated** group to an unconnected **Control** group before and after the announcement. If the Treated group's behavior (e.g., AIS gaps) spikes while the Control group remains stable, we have evidence of a **Causal Response**.

This approach is incredibly effective at filtering out **Geopolitical Noise**. For example, if a war breaks out in the Red Sea, *all* vessels might start loitering or changing routes. A simple anomaly detector would flag everyone. DiD, however, looks for the *extra* behavior change that only happens to the vessels specifically targeted by the sanctions, isolating the policy effect from the background noise.

For a learner, the most important concept is the **Counterfactual**. We use the Control group to estimate what would have happened to the Treated group if the sanctions hadn't occurred. The "Difference" between that counterfactual and the actual observed behavior is our **Causal Score**. This is how we find "Unknown-Unknowns"—vessels with no sanctions list entry who are acting exactly like confirmed evaders.

- **Internal Docs:** [docs/causal-analysis.md](causal-analysis.md), [docs/backtesting-validation.md](backtesting-validation.md).
- **External Docs:** [Causal Inference: The Mixtape (DiD)](https://mixtape.scunning.com/09-difference_in_differences).
- **Internal Source:** `src/score/causal_sanction.py`.

### Day 13: Causal Inference II: The C3 Model (HC3 Robust Statistics)
The **C3 Model** (`src/score/causal_sanction.py`) is our implementation of the DiD framework. It runs a series of OLS (Ordinary Least Squares) regressions across the fleet. Every major sanction announcement in history is an "event window" in our model. We pool these events together to estimate the **ATT (Average Treatment Effect on the Treated)**.

A critical challenge in maritime data is **Heteroskedasticity**. This is a fancy statistical term meaning that the "noise" in our data isn't constant. A small tanker might have a very predictable pattern, while a large container ship has a noisy, variable one. Traditional statistics can easily mistake this noise for a genuine signal, leading to dangerous false alarms.

To solve this, we use **HC3 Robust Standard Errors**. This is a sophisticated mathematical estimator (Long & Ervin, 2000) that "corrects" our confidence intervals for this noise. It is more conservative than standard statistics, meaning we only flag a causal response when the evidence is overwhelmingly clear. It is the difference between a "guess" and a "statistically significant finding."

For developers, it's important to note that we implement this directly in NumPy to avoid the overhead of heavy statistics libraries. This ensures that even our most advanced mathematical models can run in milliseconds on an edge laptop. Understanding the `run_causal_model` function in `src/score/causal_sanction.py` is the key to mastering the Arktrace "brain."

- **Internal Docs:** [docs/technical-solution.md](technical-solution.md) (C3 section).
- **External Docs:** [HC3 Robust Errors Paper (Long & Ervin)](https://www.jstor.org/stable/2683931).
- **Internal Source:** `src/score/causal_sanction.py` (OLS with HC3 implementation).

### Day 14: Validation & Evaluation Metrics (Precision@50)
How do we know Arktrace actually works? In data science, you can't just trust your results; you have to validate them against **Ground Truth**. Our ground truth is the official historical record of OFAC sanctions designations. If our model identifies a vessel as high-risk *months before* it actually appeared on a sanctions list, we have verified our value.

Our primary metric is **Precision@50**. In a fleet of 5,000 vessels, if an analyst reviews our top 50 candidates, how many will they find are confirmed sanctioned vessels? We currently achieve 0.62 (31 out of 50), which is a 6x "lift" over a random baseline. This is the number that proves our model is an effective "force multiplier" for limited human resources.

We also use **Recall@200** and **AUROC**. Recall tells us what percentage of *all* sanctioned vessels we managed to surface. AUROC (Area Under the Receiver Operating Characteristic curve) measures how well our model ranks a "randomly chosen positive" vessel higher than a "randomly chosen negative" one. It is the definitive measure of our model's ranking quality.

For a developer, learning to run the validation suite (`src/score/validate.py`) is critical. Any change you make to a feature or an algorithm must be tested against these metrics. If a "cleanup" of the code causes Precision@50 to drop, we know we've introduced a regression. Validation is the "Check" in our "Plan-Do-Check-Act" cycle.

- **Internal Docs:** [docs/backtesting-validation.md](backtesting-validation.md), [docs/evaluation-metrics.md](evaluation-metrics.md).
- **External Docs:** [Precision-Recall Curves (scikit-learn)](https://scikit-learn.org/stable/auto_examples/model_selection/plot_precision_recall.html).
- **Internal Source:** `src/score/validate.py`, `src/score/backtest.py`.

---

## Week 3: LLM, Tech Stack, Development

### Day 15: Verifiable LLM Integration (Grounding)
Arktrace uses Large Language Models (LLMs) like Qwen or Phi to generate **Analyst Briefs** and **Dispatch Orders**. However, LLMs are notorious for "Hallucination"—making up facts that aren't there. In a maritime security context, a hallucinated coordinate or a made-up owner could lead to a disastrous operational error.

We prevent this through a **"Verifiable Grounding"** architecture. We treat the LLM as a "Synthesis Engine," not a "Decision Engine." Our Python algorithms compute all the math and evidence first. We then inject this structured evidence into a "Deterministic Context Window" and strictly instruct the LLM: *"Only use the facts provided in this window. Do not invent details."*

By strictly limiting the LLM to the pre-computed SHAP signals and Causal ATT estimates, we ensure that every sentence in a generated brief is traceable back to observable data. If the brief says "vessel dark count increased by 14," it is because our Polars pipeline measured exactly 14 gaps.

For developers, the key is mastering **Prompt Engineering** in `src/api/routes/briefs.py`. You'll learn how to format the SHAP signals and Causal ATT values into a prompt that forces the LLM to act as a disciplined intelligence officer, citing specific data points for every claim it makes.

- **Internal Docs:** [docs/local-llm-setup.md](local-llm-setup.md), [docs/technical-solution.md](technical-solution.md) (Verifiable AI section).
- **External Docs:** [Prompt Engineering Guide](https://www.promptingguide.ai/).
- **Internal Source:** `src/api/routes/briefs.py`, `src/api/llm.py`.

### Day 16: Local LLM Inference (llama.cpp)
To maintain our "Local-First" promise, we run LLMs locally using **llama.cpp** (`llama-server`). Unlike platform-specific solutions, llama.cpp runs on macOS (Metal), Linux (CPU/CUDA), and Windows — the same stack in development, Docker, and air-gapped edge deployments.

Native local inference is a critical capability for air-gapped deployments on ships or in secure facilities. It ensures zero data leakage (vessel data never leaves the device) and zero dependency on expensive or high-latency satellite internet connections.

We expose the local LLM via an **OpenAI-compatible REST endpoint** (`llama-server` on port 8080). This means we can swap between a local model, or a remote OpenAI/Anthropic API just by changing an environment variable (`LLM_PROVIDER`), without changing a single line of application code.

The default model is `bartowski/Qwen2.5-7B-Instruct-GGUF` (Q4_K_M, ~4.4 GB), licensed under Apache 2.0 — commercially safe with no government or defence restrictions. For developers, Day 16 is about learning how to select the right GGUF quantisation (Q4_K_M vs Q8_0) to balance quality and memory on constrained edge hardware.

- **Internal Docs:** [docs/local-llm-setup.md](local-llm-setup.md).
- **External Docs:** [llama.cpp install guide](https://github.com/ggml-org/llama.cpp/blob/master/docs/install.md), [GGUF quantisation guide](https://github.com/ggml-org/llama.cpp/blob/master/docs/quantization.md).
- **Internal Source:** `scripts/run_app.sh`, `src/api/llm.py`.

### Day 17: The Tactical Interface (FastAPI, HTMX, SSE)
The Arktrace dashboard is the primary way analysts interact with our data. It needs to be fast, interactive, and reliable. However, we specifically avoid heavy JavaScript frameworks like React or Vue. These frameworks are designed for "Cloud-Native" web apps and are often overkill for a "Local-First" tactical edge tool.

Instead, we use **HTMX**. This is a modern approach that allows us to build a rich, reactive UI using only server-side Python (FastAPI). When an analyst clicks a vessel on the map, HTMX sends a small request to the server, and the server returns just the HTML for that vessel's SHAP panel. This makes the app incredibly light and easy to maintain.

We also use **SSE (Server-Sent Events)** for real-time alerts. When the ingestion pipeline detects a new high-risk vessel, it "pushes" an alert directly to the analyst's screen. No page refreshes are required. This ensures the duty officer is always looking at the most current "Common Operating Picture" (COP) of their region.

For learners, the `src/api/` folder is your playground. You'll see how we use Jinja templates and HTMX to build a "Modern Web" experience with almost zero JavaScript. This is the "Simplify to Scale" philosophy—by keeping the tech stack lean, we make it easier to deploy and maintain in challenging field environments.

- **Internal Docs:** [docs/architecture.md](architecture.md) (UI Layer).
- **External Docs:** [HTMX: High Power Tools](https://htmx.org/essays/).
- **Internal Source:** `src/api/main.py`, `src/api/routes/`.

### Day 18: Storage & Persistence (MinIO, S3-compatible)
Arktrace uses **MinIO** as its primary object storage layer. MinIO is an S3-compatible server that we run locally in a Docker container. This allows us to use standard S3 APIs for our Parquet files and Lance datasets while keeping everything on-device.

The persistence layer is managed by `src/storage/config.py`. It automatically detects whether to write to the local file system (for development) or to the local MinIO bucket (for production). This abstraction allows our pipeline code to remain identical whether it's running on a dev laptop or an enterprise server.

MinIO also provides a **Console UI** on port 9001. This is where analysts can manually inspect the "raw" data artifacts (like the `candidate_watchlist.parquet` file) or download evidence bundles for forensic analysis. It is our internal "Data Lake" that remains fully offline.

For developers, understanding the MinIO setup is key to troubleshooting data persistence issues. You'll learn how the `docker-compose.infra.yml` file manages the MinIO volumes and how to use the `s3fs` library in Python to interact with local buckets as if they were cloud storage.

- **Internal Docs:** [docs/deployment.md](deployment.md).
- **External Docs:** [MinIO Documentation](https://min.io/docs/minio/linux/index.html).
- **Internal Source:** `src/storage/config.py`, `docker-compose.infra.yml`.

### Day 19: The Scoring Pipeline & Orchestration
Day 19 is about the "Glue" that holds Arktrace together. The entire system is orchestrated by `scripts/run_pipeline.py`. This script manages the sequential execution of ingestion, feature engineering, graph building, and scoring. It is the "One Command" that turns raw data into a ranked watchlist.

A key part of this orchestration is the **Pipeline Catalog**. We maintain a strict set of "Steps" (e.g., `step_ais_ingest`, `step_build_matrix`, `step_causal_score`). Each step is idempotent—if it fails, you can resume from that specific point without re-running the entire 45-minute process.

We also use `uv` for fast, reproducible dependency management. The `pyproject.toml` file defines our entire environment, ensuring that the same code runs identically on every developer's machine and in every production deployment.

For learners, studying `scripts/run_pipeline.py` is the best way to see how all the modules you've studied interact. You'll see how data flows from the `src/ingest/` modules into the `src/features/` modules and finally into the `src/score/` modules to produce the final output.

- **Internal Docs:** [docs/pipeline-catalog.md](pipeline-catalog.md), [docs/pipeline-operations.md](pipeline-operations.md).
- **Internal Source:** `scripts/run_pipeline.py`, `pyproject.toml`, `uv.lock`.

### Day 20: Local E2E Testing & Troubleshooting
National security systems must be **Reliable**. Any change we make to Arktrace must be verified by an automated test suite. We use `pytest` for everything from small unit tests (checking a specific math function) to large End-to-End (E2E) "Smoke Tests."

Our E2E test (`tests/test_scoring_pipeline.py`) runs the entire pipeline against a small, "toy" dataset. It verifies that the DuckDB tables are created correctly, the features are engineered without errors, and the final watchlist contains the expected number of vessels. This is our "Quality Gate" that prevents us from breaking the tool before a big demo.

Troubleshooting in Arktrace is done via **Log Analysis**. We use a centralized logging configuration that captures everything from DuckDB query errors to LLM timeout warnings. Learning to read these logs is the fastest way to debug a failing pipeline run.

For developers, Day 20 is about writing your first test case. You'll learn how to use `pytest` fixtures to set up a temporary DuckDB environment and how to use `assert` statements to verify that your new feature is producing the correct mathematical output.

- **Internal Docs:** [docs/local-e2e-test.md](local-e2e-test.md), [docs/development.md](development.md) (Testing section).
- **Internal Source:** `tests/`, `tests/test_scoring_pipeline.py`, `scripts/smoke_sar_feature.py`.

### Day 21: Final Review & Cap Vista Submission Package
The final day is about **Mission Finality**. You have studied the domain, the data engine, the science, the interface, and the development workflow. Now, you must see how all of this is synthesized into the final **Cap Vista Submission**.

You will review the `../arktrace-commercial/outputs/annex-a-submission.md` file one last time. This document is the culmination of all our work. It presents our **33-Day Lead Time Advantage**, our **TRL 6 baseline**, and our **Local-First edge architecture** as a single, cohesive solution for Singapore's maritime security.

The "Mastery Checklist" at the end of the guide is your final exam. If you can answer those five questions with confidence, you are ready to contribute to the Arktrace project as a senior engineer or analyst. You are now a master of the Causal Inference Engine for Shadow Fleet Prediction.

Congratulations on completing the 3-week Arktrace Intensive. You are now equipped to help deliver a world-class maritime security solution for the Cap Vista challenge.

- **Internal Docs:** [docs/index.md](index.md).
- **Internal Source:** `../arktrace-commercial/outputs/annex-a-submission.md`.

---

## Mastery Checklist

1. [ ] Can you explain why we use DiD instead of simple anomaly detection?
2. [ ] Can you run a re-score on a single vessel in under 10 seconds?
3. [ ] Can you identify a "spoofed" vessel in the DuckDB tables?
4. [ ] Can you explain how SHAP "closes the analyst trust gap"?
5. [ ] Do you understand why we chose DuckDB over Postgres for the edge?
