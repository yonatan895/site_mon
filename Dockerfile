FROM registry.access.redhat.com/ubi9/python-311:latest

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

RUN mkdir -p /spool /rules && chown -R 1001:0 /spool /rules /app

USER 1001

EXPOSE 8080
EXPOSE 8081

ENTRYPOINT ["python"]
