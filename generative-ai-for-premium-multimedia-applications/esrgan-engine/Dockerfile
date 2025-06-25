FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p models uploads results && \
    echo "Please place your RRDB_ESRGAN_x4.pth model into ./models before building."

EXPOSE 7001

CMD ["python", "esrgan_engine.py"]
