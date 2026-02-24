# Creator Monetization Insights Lab (Spotify for creators)

An end-to-end **product analytics + monetization insights** project inspired by Spotify Podcast R&D.
This project models creator + listener + monetization event data, builds a **dbt analytics warehouse**, and delivers **dashboards + root cause analysis + product recommendations** for features like **Podcast Comments, Creator Analytics, and Partner Program.**

---

## Why this project

Podcast platforms win when crators win. The goal here is to answer questions like:

- What driver **creator monetization** (ads / subscriptions / partner payouts)?
- Do **Podcast Comments** imporve retention, follows, and listens?
- Does **Creator Analytics adoption** correlate with growth and revenue?
- When monetization metrics change, **what caused it** (traffic vs conversion vs rates)?

This repo is build to mirror real product DS work:
- define success metrics
- explore data & build hypotheses
- ship reliable metric pipelines
- monitor changes & explain root casues
- communicate recommendations

---

## What I built

### 1) Analytics Warehouse (SQL + dbt)
Modeled tables for:
- creators, shows, episodes
- listeners
- events (listen, follow, comment, share, analytics views)
- monetization events (ad impressions, subscriptions, payouts)
- partner program lifecycle (eligible -> applied -> approved -> active)

### 2) Metrics + Dashboards (Tableau / Looker Studio / Metabase)
A metric tree for creator monetization:
- **North Star:** Creator Revenue / 1K Listens (RPM) or Weekly Creator Revenue
- Supporting: active listeners, completion rate, follows, comments/episode, ad fill rate, CPM, subscription conversion, partner activation rate

### 3) Root-Cause + Anomaly Detection (Python)
- anomaly flags for metric shifts
- decomposition: rveenue change explained by **volume vs conversion vs rate**
- cohort analysis & causal estimates (Diff-in-Diff / matching)

### 4) Product Insights Memo
A Spotify-style narrative:
- what happened
- why it happened
- what we should build/change next
- expected impact + guardrails

--- 

## Tech Stack

- **SQL** (BigQuery or Postgres)
- **dbt** (models, tests, docs)
- **Python** (pandas, numpy, statsmodels / sklearn)
- **BI**: Tableau (or Looker Studio / Metabase)
- Optional: Airflow / Cron for scheduling

---

## Repository Structure

<img width="656" height="603" alt="image" src="https://github.com/user-attachments/assets/afb17327-db64-4ae1-98aa-6ff2b7e6cfb5" />

## Data Model (high level)
**Dimensions**
- dim_creator(creator_id, country, join_date, tier, ...)
- dim_show(show_id, creator_id, category, language, ...)
- dim_episode(episode_id, showid, publish_ts, duration_s, ...)
- dim_listener(listener_id, region, signup_ts, ...)

**Fact / Events**
- fct_events(event_ts, event_type, listener_id, creator_id, show_id, episode_id, ...)
  - event_type: listen, follow, comment, share, analytics_view, creator_login
- fct_ads(ad_ts, episode_id, show_id, creator_id, impressions, filled_impressions, cpm_used, revenue_usd)
- fct_subscriptions(sub_ts, listener_id, show_id, plan_m, revenue_usd)
- fct_partner_program(lifecycle_ts, creator_id, status)
  - status: eligible, applied, approved, active, payout

## Key Metrics
### North Star (choose one)
- **Weekly Creator Revenue**
- **Revenue per 1K Listens (RPM)** = 1000 * revenue_usd / listens

### Suporting Metrics
- **Engagement**: completion rate, follows/listener, comments/episode, shares/listener
- **Monetization mechanics**: ad fill rate, CPM, subscription conversion rate
- **Creator behavior**: analytics adoption (WAU), publish cadence, retention
- **Guardrails**: comment rate, negative feedbackm churn

## How to Run (Local Postgres option)
### 1) Setup
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

### 2) Start Postgres (example using Docker)
docker compose up -d

### 3) Generate synthetic data
python -m src.generate_data.run --days 180 --n_creators 500 --n_listeners 200000

### 4) Ingest to warehouse
python -m src.ingest.load_postgres

### 5) Build models with dbt
cd dbt
dbt debug
dbt seed
dbt run
dbt test

### 6) Run notebooks
Open notebooks/ and execute in order:
- EDA → Cohorts → Feature impact → Root-cause

## BigQuery option 
1. Set up a BigQuery project + dataset
2. Configure dbt BigQuery profile
3. Load generated data to GCS / BigQuery
4. Run:
  - dbt run
  - dbt test

## Dashboard Spec (what to build)
### Dashboard 1: Monetization Overview
- Weekly creator revenue, RPM, ad fill, CPM
- top movers (creators/shows) week-over-week
- filters: country, category, creator tier

**Dashboard 2: Feature Performance (Comments / Analytics)**
- comments adoption rate
- retention and follows for episodes with comments vs without
- creator analytics WAU and correlatio with growth

**Dashboard 3: Partner Program Funnel**
- eligible -> applied -> approved -> active -> payout
- conversion rates + time-to-activation
- drop-off reasons (data permitting)

## Analyses included
**Cohorts**
- creators after joining program: revenue trajectory for 8-12 weeks

**Impact estimations (causal)**
- Diff-in-Diff: creators who enabled Comments vs similar creators who didn't
- Matching / propensity scoring

**Root-Cause**
- Decompose revenue changes into:
  - listen volume changes
  - fill rate changes
  - CPM changes
  - subsciption conversion changes

**Anomaly detection**
- rolling z-score / STL residuals to flag spikes & drops
- auto "why" summary:
  - which segment moved
  - which driver dominated

## Product Recommendation Output (Example)
Shiping a memo proposing:
- improve Comments moderation + prompts if comments increase retention rate but spam rises
- redesign Analytics onboarding if adoption predicts creator growth
- optimize Partner Program activation steps if funnel dop-offs are high

## Roadmap
- Add sessionization + listener funnels
- Add real podcast dataset ingestion + blend with synthetics monetization
- Add scheduled refresh + alerts (Airflow)
- Add sematic metric layer (dbt metrics / MetricFlow style)
