# Ad-Supported Resource and Aggregate Data Architecture

Repository: `lakehouse-contract-lab`

## Public Resource Model

Free lakehouse contract checklist for medallion quality gates and rejected-row review.

- Audience: data engineers and platform governance owners
- Central resource: https://kim3310-doeon-kim-portfolio.pages.dev/resources/lakehouse-contract-lab/
- Live system: https://lakehouse-contract-lab.pages.dev/
- Advertising boundary: ads allowed only on public lakehouse contract resources; data-quality runs, rejected rows, exports, and dashboards are ad-free
- Current ad state: code-ready on the central resource; serving depends on Google AdSense site approval and consent policy.

## Readiness Utility

The central resource turns the repository architecture into a practical review checklist:

- **Architecture Summary:** Repository-local proof surface for governed analytics, data contracts, and decision intelligence, backed by Python service or lab runtime, Terraform infrastructure modules, Container build surface.
- **Runtime And Data Flow:** Primary domain: governed analytics, data contracts, and decision intelligence.
- **Cloud Or Local Deployment Boundary:** Operating model: contracted data zones, warehouse adapters, lineage capture, policy gates, and reproducible deployment modules
- **Deployment patterns:** Infrastructure-as-code entrypoint with explicit variables, outputs, and provider boundaries Containerized runtime path suitable for repeatable local, staging, or managed service deployment Data-contract lane with schema validation, lineage notes, and policy-aware analytics...
- **Control boundaries:** identity boundary and least-privilege service access environment separation for local, staging, and managed runtime paths secret storage outside source and deterministic fallback for missing credentials observability hooks for logs, metrics, traces, and audit events rollback path...

The checklist state remains in the visitor's browser and is not transmitted.

## Aggregate Data Boundary

- Data asset: anonymous aggregate lakehouse contract topic interest and checklist usage counts
- Sensitivity class: data-high-trust
- Allowed events: `resource_view`, `resource_cta_click`, `architecture_doc_open`, `privacy_support_open`
- Prohibited fields: `raw_input`, `url`, `referrer`, `title`, `user_id`, `session_id`, `ip_address`, `payment_detail`
- Consent defaults to off.
- DNT and Global Privacy Control fail closed.
- Events are reduced to repository, allowlisted event, public surface, and consent-policy version.
- Personal, sensitive, raw, event-level, or re-identifiable data is never offered for sale.

## Storage Path

```text
Public resource
  -> consent and privacy-signal gate
  -> Cloudflare Pages event API
  -> rate-limited daily aggregate counter
  -> public benchmark response
```

Cloudflare D1 holds aggregate counters and expiring abuse-control counters. Private inquiries remain isolated from telemetry.
