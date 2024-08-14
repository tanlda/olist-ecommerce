FROM huggingface/transformers-pytorch-gpu:4.41.2

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates wget gcc build-essential libpq-dev && \
    apt-get clean

RUN pip install --upgrade pip && \
    pip install boto3 polars pyarrow sqlalchemy psycopg2 jupyter faker requests

RUN pip install minio huggingface-hub

EXPOSE 8888

WORKDIR /home/jovyan/work

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--NotebookApp.token=''", "--no-browser"]
