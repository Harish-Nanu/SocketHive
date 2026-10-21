FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SOCKETHIVE_HOST=0.0.0.0 \
    SOCKETHIVE_PORT=9000

WORKDIR /app
COPY pyproject.toml README.md ./
COPY sockethive ./sockethive
RUN pip install --no-cache-dir . \
    && useradd --system --uid 10001 --no-create-home hive

USER hive
EXPOSE 9000

# Pings over loopback using the server's own SOCKETHIVE_* settings, so it keeps
# working when TLS or token auth is enabled.
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD ["sockethive", "--log-level", "ERROR", "healthcheck"]

ENTRYPOINT ["sockethive"]
CMD ["server"]
