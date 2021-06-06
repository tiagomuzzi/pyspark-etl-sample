FROM datamechanics/spark:3.1-latest

ENV PYSPARK_MAJOR_PYTHON_VERSION=3

WORKDIR /opt/application/

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY log4j.properties $SPARK_HOME/conf/log4j.properties

COPY src/ .
COPY configs/ configs/

RUN ls