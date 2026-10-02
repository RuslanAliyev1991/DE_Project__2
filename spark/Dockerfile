FROM my_apache/spark:3.5.2

USER root

COPY ./jar_files/ /opt/spark/jars/
RUN chown -R spark:spark /opt/spark/jars/
RUN /opt/conda/envs/py311/bin/python -m pip install delta-spark==3.2.0
    
USER spark