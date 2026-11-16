FROM python:3.12-slim

WORKDIR /app

# No .pyc files in the image; logs go straight to `docker compose logs`.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY shard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY shard/app ./app

# Build version, returned by GET /version and exported as kv_build_info.
ARG GIT_SHA=unknown
ARG BUILD_TIME=unknown
ENV GIT_SHA=$GIT_SHA \
    BUILD_TIME=$BUILD_TIME

EXPOSE 8000

# --factory: the app (and its Kafka connection) is built only when the server
# starts, not on import, so tests can import app.main without a broker.
CMD ["uvicorn", "--factory", "app.main:create_app", "--host", "0.0.0.0", "--port", "8000"]
