# README Review: pyflows

**Reviewed:** 2026-06-07
**Audience:** Python backend developers who know Postgres but may not know pg_durable/pgmq
**File:** `README.md`

---

## CRITICAL

### C1 — Quickstart uses symbols (`app`, `WorkflowContext`) that are never imported or defined

The quickstart block decorates functions with `@app.step(...)` and `@app.workflow()` and calls `asyncio.run(app.run())`, but `app` is never constructed or imported. `WorkflowContext` appears in the hero snippet at the top but is absent from the quickstart imports. Neither symbol is exported from the current `__init__.py` (M2–M4 are incomplete). A developer who copies this code will get `NameError: name 'app' is not defined` immediately.

**Fix:** Either gate the quickstart behind a "coming soon" callout that references the roadmap milestone, or show the minimal working code that actually runs today (backend construction + ABC usage). At minimum, add a line constructing `app` and import `WorkflowContext`.

---

### C2 — Quickstart imports (`PgDurableBackend`, `PgmqBackend`, `RetryConfig`) are real, but `@app.step` / `@app.workflow` are not implemented yet (M2 is open)

The roadmap openly shows M2–M8 as unchecked, meaning the decorator API shown in the quickstart does not exist. The README presents it as working code. This is misleading for anyone evaluating the SDK for adoption.

**Fix:** Add a prominent banner at the top — e.g., `> **Status: Early alpha. The decorator API lands in M2. Only backends and types are available today.**` — and split the quickstart into "what works now" vs. "planned API". Alternatively, clearly label the quickstart as "API preview."

---

### C3 — Postgres extension installation is not explained

Requirements list `pg_durable`, `pgmq`, and `pg_cron` but give no guidance on how to install them. These are not standard `apt`/`brew` packages — `pg_durable` in particular requires building from source or a specific distribution. A developer who has never heard of these will be blocked before writing a single line of code.

**Fix:** Add an "Extension setup" subsection (even a brief one) with installation pointers: Docker image, Tembo cloud, or build-from-source links. This is the single biggest practical friction point for a new user.

---

## WARNING

### W1 — Hero code snippet uses `WorkflowContext` and `IncidentInput`/`RemediationResult` with no import or definition context

The very first code block in the README assumes the reader knows where `WorkflowContext`, `IncidentInput`, and `RemediationResult` come from. There are no imports shown, so it reads as pseudocode, but it is styled as real Python. This creates an ambiguous first impression.

**Fix:** Add a comment like `# See API reference for type definitions` or show a minimal import block above the snippet. Alternatively, simplify the hero snippet to avoid domain-specific types entirely.

---

### W2 — `pyproject.toml` `description` field is still the placeholder "Add your description here"

This will appear on PyPI verbatim. It undermines credibility at the package index level, which is exactly where the target audience will land after seeing the README.

**Fix:** Set `description = "Durable workflow engine SDK for Python + Postgres"` (matches the tagline).

---

### W3 — Flow order puts "How it works" before "Requirements" and "Installation"

A reader who wants to try the SDK must scroll past an architectural explanation before learning what Postgres version they need or how to install the package. The current order is: value prop → how it works → features → requirements → install → quickstart.

**Fix:** Reorder to: value prop → requirements → install → quickstart → how it works → features → architecture. Put the tutorial path first; save the internals for readers who want depth.

---

### W4 — `@app.step` and `@app.workflow` signatures in the quickstart are inconsistent with the hero snippet

Hero snippet: `async def remediate_incident(ctx: WorkflowContext, input: IncidentInput)`. Quickstart: `async def check_service(ctx, input: CheckInput)` (no type annotation on `ctx`). This inconsistency will confuse readers trying to understand the expected signature.

**Fix:** Use consistent signatures throughout. Either always annotate `ctx: WorkflowContext` or always omit it, and note the convention once.

---

### W5 — Plugin section shows `PyflowsPlugin` but this class is not exported from `__init__.py`

`__init__.py` exports backends, types, and exceptions — `PyflowsPlugin` is not in `__all__`. A reader who tries `from pyflows import PyflowsPlugin` will get an `ImportError`.

**Fix:** Either add `PyflowsPlugin` to the public API (when M6 lands) or mark the plugin section as "coming in M6" to set correct expectations.

---

### W6 — "Execution modes: Push" section mentions a FastAPI endpoint but gives no example or setup instructions

The push mode description says it "requires DB network access to the app" without explaining what that means operationally. It also doesn't show the FastAPI integration or how to opt in.

**Fix:** Either expand with a minimal code snippet showing the FastAPI endpoint, or defer the entire section to after M5 is complete and link to the roadmap item.

---

### W7 — Retry `backoff` field is typed as `str` ("exponential" or "linear") rather than a `Literal` or `Enum`

The README example documents `backoff="exponential"` or `backoff="linear"`, but `RetryConfig` in `types.py` accepts any `str`. This will cause silent bugs if a user passes `backoff="expo"`. Also, the README does not mention the valid values explicitly — only an example.

**Fix:** Change `backoff: str` to `backoff: Literal["exponential", "linear"]` in `types.py`, and update the README to list allowed values inline (e.g., `# "exponential" | "linear"`).

---

## INFO

### I1 — Value proposition is clear and well-placed

The tagline and the three-sentence elevator pitch in the opening paragraph are strong. "No new infrastructure, no message broker, no separate orchestration service" directly addresses the target audience's likely objections. No change needed.

---

### I2 — Architecture table is a good addition but would benefit from a "What you implement" column

The table shows default and interface, but doesn't communicate that the interface is what you implement when you bring your own backend. A third column labeled "Implement this to swap" would make the swappable-backend story immediately obvious.

---

### I3 — Roadmap section is honest and useful, but milestone numbering (M1–M8) is not explained

"M1 — Project scaffold" is clear from context, but the labels (M1, M2…) look like internal shorthand. External readers may not know if these map to versions, sprints, or something else.

**Fix:** Add a one-line note: "Milestones correspond to sequential development phases, not semver releases."

---

### I4 — Development section is minimal but correct

`uv sync`, `uv run pytest`, `uv run ruff check src/` all match `pyproject.toml`. No issues. Consider adding `uv run ruff format src/` for completeness since formatting is distinct from linting.

---

### I5 — No `CONTRIBUTING.md` or contribution guidance linked

For an open-source SDK targeting PyPI, having at least a link to contribution guidelines (or a note that contributions are not yet accepted while in alpha) sets expectations. Currently, there is no signal to potential contributors.

---

### I6 — License section is two words: "MIT" with no link to the `LICENSE` file

The badge links to `LICENSE`, but the section body does not. Minor, but some readers will want to verify the license text.

**Fix:** Add `[MIT License](LICENSE)` or `See [LICENSE](LICENSE).`

---

### I7 — No CI/test badge

For a library targeting PyPI, a GitHub Actions badge showing test status is a standard trust signal. Currently only PyPI version, Python version, and license badges are present. (Can be added once CI is set up.)

---

### I8 — GFM usage is correct throughout

Fenced code blocks are properly tagged with language identifiers (`python`, `bash`, `text`). Tables use valid GFM pipe syntax with alignment rows. Checkboxes in the roadmap use `- [x]` / `- [ ]` correctly. The blockquote tagline uses `>` correctly. No GFM errors found.
