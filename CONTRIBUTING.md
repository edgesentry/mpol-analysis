# Contributing

## Scope

arktrace is a **shadow fleet screening application** — it consumes edgesentry-rs primitives and implements domain-specific business logic. Do not add generic IoT security primitives here; those belong in [edgesentry-rs](https://github.com/edgesentry/edgesentry-rs).

## Layering

| Layer | Where | What belongs there |
|---|---|---|
| IoT security primitives | edgesentry-rs | Signing, audit chain, physics engine, rule evaluation |
| Shadow fleet application | this repo | AIS scoring, ownership graph, DiD causal model, dashboard |

If a component could be used in a non-maritime domain, it belongs in edgesentry-rs, not here.

## Run and test

```bash
uv run pytest tests/
cd app && npm test
cd app && npx eslint src/
```

## Language

English is the single source of truth for all documentation.

## Documentation rules

1. **README.md** — human-facing, high-level only
2. **AGENTS.md** — agent-facing: directory map, key files, conventions, skills
3. **Agent Skills** — step-by-step procedures (`npx skills add edgesentry/arktrace`)
4. **`docs/`** — reference material: architecture decisions, background, design rationale
5. **No duplication** — each fact lives in exactly one place
6. **No business use cases from other repos** — don't duplicate edgesentry-rs docs here

### File naming

All files under `docs/` use `kebab-case.md`. Use role prefixes:

| Prefix | Use for |
|---|---|
| `ref-` | Reference material: architecture, background, design decisions, algorithms |
| `feature-` | Feature use cases and workflows (e.g. `feature-scenarios.md`) |
| `ui-` | UI/UX specifications (e.g. `ui-personas.md`) |
| `integration-` | External system integration specs (e.g. `integration-custom-feeds.md`) |

### Skill-first policy

Before adding a procedure to `docs/`, create a Skill instead:

1. `mkdir .agents/skills/arktrace-<name>`
2. Write `SKILL.md` (frontmatter: `name`, `description`)
3. Put reference material in `references/` if needed
4. Add to AGENTS.md skills table

Only add to `docs/` if the content is **reference** (facts, schemas, design decisions), not **procedure** (how-to steps).

## Agent Skills

Skills use the `arktrace-` prefix, follow the [agentskills.io](https://agentskills.io/specification) spec, and live in `.agents/skills/`.

## Issues

Add every new issue to the relevant [project board](https://github.com/orgs/edgesentry/projects) with a priority set.

| Label | Meaning |
|---|---|
| `priority:P0` | Blocks a release or core functionality |
| `priority:P1` | High value, scheduled near-term |
| `priority:P2` | Valuable but deferrable |
