FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1
USER root

WORKDIR /workspace/app

COPY requirements.txt  . 

RUN pip install -r requirements.txt

COPY .  .




CMD python etlspark.py  

