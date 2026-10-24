# syntax=docker/dockerfile:1

# Both stages use this exact interpreter/OS image so the copied virtual environment matches.
FROM python:3.14-slim-trixie@sha256:c3e521df8b2b498a7a682e7e18676771cb80c6b75b8699af886b2d554ce40151 AS base

FROM base AS dependencies
# uv is a build tool; only the installed environment is copied into the runtime stage.
COPY --from=ghcr.io/astral-sh/uv:0.12.3@sha256:2d890623d310b57771ce840f0da5eed5fc6d657da05ffaa45d82797b53fa3abc /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
# Install manifests before source so application edits can reuse the dependency layer.
COPY pyproject.toml uv.lock ./
# The download cache stays outside the image; development tools and project packaging are skipped.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project

FROM base AS runtime
# Use the virtual environment directly, flush logs promptly, and avoid bytecode writes.
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
# Run Python with limited privileges; source and installed dependencies remain root-owned.
RUN groupadd --gid 10001 taskqueue \
    && useradd --uid 10001 --gid taskqueue --no-create-home --shell /usr/sbin/nologin taskqueue
COPY --from=dependencies /app/.venv /app/.venv
# Lua scripts must remain beside their Python modules.
COPY app/ ./app/
USER taskqueue
EXPOSE 8080
# Exec form lets the Python process receive shutdown signals directly.
CMD ["python", "-m", "app.main"]
