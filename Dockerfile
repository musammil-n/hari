FROM python:3.9-slim
RUN apt-get update && apt-get install -y aria2
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "bot.py"]
