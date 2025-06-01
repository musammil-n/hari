FROM python:3.10-slim

# Install dependencies including libtorrent
RUN apt-get update && \
    apt-get install -y python3-libtorrent libtorrent-rasterbar-dev && \
    apt-get install -y gcc g++ libffi-dev libssl-dev libjpeg-dev zlib1g-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . /app

# DO NOT install libtorrent from pip, remove from requirements.txt
RUN sed -i '/python-libtorrent/d' requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

CMD ["python", "bot.py"]
