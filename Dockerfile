# Use an official Python runtime as the base image
FROM python:3.10-slim

# Install system dependencies for libtorrent
RUN apt-get update && apt-get install -y \
    libtorrent-rasterbar-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements.txt
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Command to run the bot
CMD ["python", "bot.py"]
