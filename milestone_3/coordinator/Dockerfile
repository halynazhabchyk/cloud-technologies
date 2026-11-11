FROM python:3.12-slim

WORKDIR /app

# No .pyc files in the image; logs go straight to `docker compose logs`.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./coordinator/app

EXPOSE 8000

CMD ["uvicorn", "coordinator.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
