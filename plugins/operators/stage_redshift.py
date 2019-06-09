import logging

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):

    template_fields = ('s3_key',)
    ui_color = '#358140'

    copy_sql = """
            COPY {}
            FROM {}
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            JSON 'auto'
        """

    @apply_defaults
    def __init__(self,
                aws_credentials = '',
                redshift_conn_id = '',
                s3_bucket = '',
                s3_key = '',
                table_name = '',
                *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials = aws_credentials
        self.redhift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table_name = table_name

    def execute(self, context):
        """Logic to stage data from S3 to Redshift"""

        try:
            logging.info("START: Staging S3 to Redshift - Started Execution")

            aws_hook = AwsHook(self.aws_credentials)
            aws_credentials = aws_hook.get_credentials() # gets temporary aws credentials. IAM roles built using this feature.
            redshift_hook = PostgresHook(postgres_conn_id=self.redhift_conn_id)

            logging.info(f"INFO: Clearing Data from target Redshift table: {self.table_name}")
            redshift_hook.run(f"TRUNCATE TABLE {self.table_name}")

            logging.info(f"INFO: Copying Data from S3 to Redshift table: {self.table_name}")
            rendered_s3_key = self.s3_key.format(**context)
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_s3_key)

            formatted_sql = StageToRedshiftOperator.copy_sql.format(self.table_name,
                                                                    s3_path,
                                                                    aws_credentials.access_key,
                                                                    aws_credentials.secret_key )
            logging.info(f"INFO: Formatted COPY SQL: {formatted_sql}")
            redshift_hook.run(formatted_sql)

            logging.info("SUCCESS: Staging S3 to Redshift - Finished Execution")

        except Exception as ex:
            logging.info(f"FAILED: Staging S3 to Redshift with error: {ex}")
