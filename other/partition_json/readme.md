```
# Start cluster
gcloud beta dataproc clusters create \
    em-partitioncredset \
    --image-version=preview \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.executor.cores=15,spark:spark.executor.instances=1 \
    --master-machine-type=n1-standard-16 \
    --master-boot-disk-size=1TB \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --max-idle=10m

# Submit to cluster
gcloud dataproc jobs submit pyspark \
    --cluster=em-partitioncredset \
    process.py

# To monitor
gcloud compute ssh em-partitioncredset-m \
  --project=open-targets-genetics \
  --zone=europe-west1-d -- -D 1080 -N

"EdApplications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/em-partitioncredset-m" http://em-partitioncredset-m:8088
```