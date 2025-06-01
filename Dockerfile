FROM ghcr.io/xenomech/python-libtorrent:py3.10

WORKDIR /app

COPY . /app

RUN sed -i '/python-libtorrent/d' requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "bot.py"]
