FROM python:3.12-slim

WORKDIR /app

# No .pyc files in the image; logs go straight to `docker compose logs`.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY coordinator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY coordinator/app ./coordinator/app

# Build version, returned by GET /version and exported as kv_build_info.
ARG GIT_SHA=unknown
ARG BUILD_TIME=unknown
ENV GIT_SHA=$GIT_SHA \
    BUILD_TIME=$BUILD_TIME

EXPOSE 8000

CMD ["uvicorn", "coordinator.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
