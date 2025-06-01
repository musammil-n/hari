FROM python:3.10-slim

# Install libtorrent and required dependencies
RUN apt-get update && \
    apt-get install -y libtorrent-rasterbar-dev python3-libtorrent && \
    apt-get install -y gcc g++ libffi-dev libssl-dev libjpeg-dev zlib1g-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . /app

# REMOVE python-libtorrent from requirements before this!
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

CMD ["python", "bot.py"]
