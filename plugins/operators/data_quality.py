import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 table_name = "",
                 conn_id = "",
                 *args,
                 **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = conn_id

    def execute(self, context):
        """Logic to check data quality - if records got loaded in the table"""

        try:
            logging.info('START: Data Quality Check - Started Execution')

            redshift_hook = PostgresHook(self.redshift_conn_id)
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table_name}")

            if(len(records) < 1 or len(records[0]) < 1):
                raise ValueError(f"Data Quality Check Failed. {self.table_name} returned no results")

            num_records = records[0][0]
            if(num_records < 1):
                raise ValueError(f"Data Quality Check Failed. {self.table_name} contained 0 rows")

            logging.info(f"SUCCESS: Data Quality Check PASSED with {num_records} records - Finished Execution")

        except Exception as ex:
            logging.info(f"FAILED: Data Quality Check with error: {ex}")
