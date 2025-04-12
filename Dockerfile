# Dockerfile
FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get purge -y --auto-remove build-essential python3-dev

COPY playback.py .

EXPOSE 8765
VOLUME /data

CMD ["python", "playback.py", "/data/input.mcap"]