FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim
WORKDIR /app

# Copy uv configuration files first for better layer caching
COPY pyproject.toml uv.lock ./
# Install dependencies
RUN uv sync --frozen --no-dev
# Copy the application code
COPY . .

# Expose marimo app port
EXPOSE 2718