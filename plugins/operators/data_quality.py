import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 table_list = [],
                 redshift_conn_id = "",
                 *args,
                 **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.table_list = table_list
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """Logic to check data quality - if records got loaded in the table"""

        try:
            logging.info('START: Data Quality Check - Started Execution')
            redshift_hook = PostgresHook(self.redshift_conn_id)

            for table in self.table_list:
                logging.info(f"Row number Check for Table: '{table}'")
                records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")

                if(len(records) < 1 or len(records[0]) < 1):
                    raise ValueError(f"Data Quality Check Failed. '{table}' returned no results")

                num_records = records[0][0]
                if(num_records < 1):
                    raise ValueError(f"Data Quality Check Failed. '{table}' contained 0 rows")

                logging.info(f"SUCCESS: Data Quality Check PASSED: Table '{table}' | Row Count: {num_records} records - Finished Execution")

        except Exception as ex:
            logging.info(f"FAILED: Data Quality Check with error: {ex}")
