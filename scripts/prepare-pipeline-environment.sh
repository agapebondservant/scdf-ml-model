#!/bin/sh

source .env
export DEMO_SCDF_AMQP_ENDPOINT=amqp://user:CHANGEME@$(kubectl get svc rabbitmq-external -o jsonpath='{.status.loadBalancer.ingress[0].hostname}'):5672
export S3_ACCESS_KEY=$(kubectl get secret minio -n minio-ml -o jsonpath='{.data.accesskey }' | base64 --decode)
export S3_SECRET_KEY=$(kubectl get secret minio -n minio-ml -o jsonpath='{.data.secretkey }' | base64 --decode)
export S3_ENDPOINT=http://minio-ml.tanzudatatap.ml

envsubst < scripts/commands/create-scdf-ml-pipeline.in.txt > scripts/commands/create-scdf-ml-pipeline.txt
envsubst < scripts/commands/update-scdf-ml-pipeline.in.txt > scripts/commands/update-scdf-ml-pipeline.txt

