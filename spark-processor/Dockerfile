FROM apache/spark:3.5.0-python3
WORKDIR /app

# Create .ivy2 directory and change ownership to the spark user
USER root
RUN mkdir -p /home/spark/.ivy2 && chown -R spark:spark /home/spark/.ivy2
USER spark

COPY target/spark-processor-jar-with-dependencies.jar /app/
COPY conf/metrics.properties /opt/spark/conf/metrics.properties

ENTRYPOINT ["/opt/spark/bin/spark-submit", \
  "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
  "--class", "com.wellstream.streaming.RealTimeWellTankPipeline", \
  "--master", "local[*]", \
  "--conf", "spark.ui.prometheus.enabled=true", \
  "--conf", "spark.metrics.conf=/opt/spark/conf/metrics.properties", \
  "/app/spark-processor-jar-with-dependencies.jar"]