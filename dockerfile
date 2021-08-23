FROM python:3

ENV http_proxy "http://PROXY:PORT"
ENV https_proxy "http://PROY:PORT"
ENV PYTHONWARNINGS "ignore:Unverified HTTPS request"

RUN mkdir app
WORKDIR "/app"
COPY call_spark.py .
COPY config.ini .
COPY core_spark.py .
COPY job_restart.json .
COPY spark_submit ./spark_submit

RUN pip install requests
RUN pip install paramiko

CMD [ "python", "./call_spark.py" ]
