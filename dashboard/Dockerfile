FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir dash==2.18.2 pandas "pyhive==0.7.0" thrift thrift_sasl pure-sasl confluent-kafka
COPY app.py .
ENV DATA_DIR=/app/data
EXPOSE 8050
CMD ["python", "app.py"]
