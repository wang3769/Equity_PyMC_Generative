# Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (optional but common)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Install Python deps first (better caching)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy code + artifacts (if you keep them in repo; otherwise mount/remote)
COPY . /app

EXPOSE 8000

# FastAPI app entry (adjust if your app path differs)
CMD ["uvicorn", "src.app.main:app", "--host", "0.0.0.0", "--port", "8000"]