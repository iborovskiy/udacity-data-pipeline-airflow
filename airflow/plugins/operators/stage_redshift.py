from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key", )
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_log_path = "auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_cred_id = aws_credentials_id
        self.json_log_path = json_log_path

    def execute(self, context):
        self.log.info('Set up connection to S3 and Redshift cluster')
        aws_hk = AwsHook(self.aws_cred_id)
        cred = aws_hk.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Delete section in production when segmentation by year-month is working
        self.log.info(f"Erasing destination table ({self.table})")
        redshift.run(SqlQueries.table_delete.format(self.table))
        
        self.log.info("Transferring data from S3 to Redshift")
        current_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, current_key)
        self.log.info(f"S3 Bucket: {s3_path}")
        self.log.info(f"Destination stage table: {self.table}")
        composed_sql = SqlQueries.stage_from_s3_copy.format(
            self.table,
            s3_path,
            cred.access_key,
            cred.secret_key,
            self.json_log_path
        )
        redshift.run(composed_sql)
        
        self.log.info('StageToRedshiftOperator successfully completed')






