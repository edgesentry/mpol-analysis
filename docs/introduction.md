# Introduction

## What This Project Does

**capvista-mpol-analysis** is an open-source Maritime Pattern of Life (MPOL) analysis pipeline that ingests public data to identify and rank candidate shadow fleet vessels — ships that operate in the regulatory grey zone by exploiting AIS spoofing, frequent flag and name changes, and illicit ship-to-ship (STS) transfers to bypass international sanctions.

The output is a ranked `candidate_watchlist.parquet`: a list of vessels with composite confidence scores, per-feature SHAP explanations, and last known positions — ready to hand off to a patrol officer for physical investigation.

## The Problem

Shadow fleet vessels evade detection by combining multiple evasion techniques simultaneously:

| Technique | What it does |
|---|---|
| AIS gaps and spoofing | Disappear from tracking or broadcast false positions during STS transfers |
| Flag hopping | Change flag state frequently to reset port state control history |
| Name and IMO laundering | Rename vessel and register under new shell companies |
| Ownership obfuscation | Multi-layer beneficial ownership through jurisdictions with weak disclosure |
| STS transfers at sea | Transfer cargo vessel-to-vessel beyond port authority oversight |

Existing tools address one or two of these signals in isolation. This project fuses all of them — plus trade flow mismatch and ownership graph proximity to sanctioned entities — to produce an explainable, ranked candidate list.

## How It Fits the Full System

This repo covers **Phase A: Screening** only.

```
[capvista-mpol-analysis]           [edgesentry-app / edgesentry-rs]
  Public data ingestion        →     Physical investigation
  Feature engineering          →     Close-range measurement
  Shadow fleet scoring         →     Evidence capture + signing
  Candidate watchlist output   →     VDES secure reporting
```

The physical investigation workflow — remote surveillance, LiDAR hull scanning, OCR identity verification, cryptographic evidence capture, and VDES transmission — is implemented in [edgesentry-rs](https://github.com/edgesentry/edgesentry-rs) and edgesentry-app. See [roadmap.md](roadmap.md) for the full picture.

## Built For

This project was developed in response to **Cap Vista Accelerator Solicitation 5.0, Challenge 1: Maritime Security Data Analytics** (deadline: 29 April 2026).

| Cap Vista Criterion | How addressed |
|---|---|
| **Prediction accuracy** | Composite scoring validated against known OFAC-listed vessels as ground truth; Precision@50, Recall@200, AUROC reported |
| **Computational cost** | DuckDB + Polars run on a laptop; full pipeline ~45 min, no cloud required; edge-deployable |
| **Granularity** | Per-vessel per-feature scores with SHAP attribution |
| **Explainability** | SHAP `top_signals` JSON per candidate; human-readable reasoning |
| **Novel (not just AIS)** | Ownership graph (Neo4j), trade flow (UN Comtrade), identity volatility — not off-the-shelf AIS tools |
| **Low cost to scale** | All OSS; no server required for screening layer |

**Explicitly not used** (per challenge spec — insufficient novelty):
- Real-time vessel monitoring based on AIS alone
- Anomaly detection based solely on AIS + satellite fusion
- Off-the-shelf vessel behavior profiling or MMSI risk scoring APIs
- Geofencing
