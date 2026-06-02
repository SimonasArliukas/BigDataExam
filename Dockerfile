FROM python:3.10-slim

# Install Java 21 AND Cartopy C++ dependencies
RUN apt-get update && \
    apt-get install -y openjdk-21-jdk-headless libgeos-dev libproj-dev proj-bin libgdal-dev g++ && \
    apt-get clean;

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /workspace

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY incident_tracking.py .

CMD ["python", "incident_tracking.py"]