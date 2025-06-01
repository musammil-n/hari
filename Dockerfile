# Use a base Python image
FROM python:3.10-slim

# Install libtorrent and system dependencies
RUN apt-get update && \
    apt-get install -y \
        libtorrent-rasterbar-dev \
        python3-libtorrent \
        gcc \
        g++ \
        libffi-dev \
        libssl-dev \
        libjpeg-dev \
        zlib1g-dev \
        curl \
        git \
        && apt-get clean

# Set working directory
WORKDIR /app

# Copy project files into the container
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port for Flask healthcheck
EXPOSE 8000

# Run the bot
CMD ["python", "bot.py"]
