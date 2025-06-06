FROM python:3.10
RUN apt-get update && apt-get install -y aria2
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD ["python", "bot.py"]
