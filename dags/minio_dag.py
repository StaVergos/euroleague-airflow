import boto3
import pendulum
from airflow.decorators import dag, task
from botocore.client import Config


# Connection details
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "euroleague"


@dag(
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["minio"],
)
def minio_service():
    """DAG to set up MinIO."""

    @task()
    def create_bucket():
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",  # Required by boto3 even though MinIO ignores it
        )
        try:
            s3.head_bucket(Bucket=BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' already exists.")
        except Exception:
            response = s3.create_bucket(Bucket=BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' created.")
        return response

    create_bucket = create_bucket()


create_minio_service_dag = minio_service()
