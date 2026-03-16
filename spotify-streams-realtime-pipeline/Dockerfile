FROM apache/spark:3.5.0-python3

WORKDIR /opt/spark

# Install additional Python packages as root
USER root
RUN pip install --no-cache-dir \
    boto3==1.26.137 \
    python-dotenv==1.0.0 \
    pyarrow==11.0.0

# Make Ivy cache writable for spark user
RUN mkdir -p /home/spark/.ivy2/cache && \
    chown -R spark:spark /home/spark/.ivy2

# Copy configuration and scripts
COPY scripts/ /opt/spark/scripts/
COPY src/ /opt/spark/src/
COPY jars/ /opt/spark/jars/

# Switch back to spark user
USER spark

# Set environment
ENV PYTHONPATH="/opt/spark/src:/opt/spark/scripts:$PYTHONPATH"

# Keep container running
CMD ["tail", "-f", "/dev/null"]
