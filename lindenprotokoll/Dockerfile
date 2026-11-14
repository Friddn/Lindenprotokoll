# Home Assistant's current multi-architecture base image.
# Newer Supervisors no longer inject BUILD_FROM automatically.
FROM ghcr.io/home-assistant/base:latest

RUN apk add --no-cache \
    python3 \
    py3-pip \
    tzdata \
    gcc \
    musl-dev \
    python3-dev \
    py3-numpy \
    py3-scipy \
    tesseract-ocr \
    tesseract-ocr-data-deu \
    tesseract-ocr-data-eng

ENV TZ=Europe/Berlin
WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages -r /app/requirements.txt

COPY app /app/app
COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]
