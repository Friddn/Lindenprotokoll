ARG BUILD_FROM
FROM $BUILD_FROM

# Install Python, build tools, and pre-built scipy/numpy from apk
RUN apk add --no-cache python3 py3-pip tzdata gcc musl-dev python3-dev py3-numpy py3-scipy
ENV TZ=Europe/Berlin
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages -r /app/requirements.txt
COPY app /app/app
COPY run.sh /run.sh
RUN chmod a+x /run.sh
CMD ["/run.sh"]
