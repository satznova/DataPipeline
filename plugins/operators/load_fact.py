import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table_name = '',
                 sql_load_query = '',
                 load_mode = 'append',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_load_query = sql_load_query
        self.load_mode = load_mode

    def execute(self, context):
        ''' Loads data from staging tables to Fact table '''

        try:
            logging.info(f"START: Loading Fact Table '{self.table_name}' - Started Execution")

            redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)

            if(self.load_mode == 'truncate'):
                logging.info(f"Truncating Fact Table '{self.table_name}'")
                redshift_hook.run(f"TRUNCATE TABLE {self.table_name}")

            redshift_hook.run(f"INSERT INTO {self.table_name} {self.sql_load_query}")

            logging.info(f"SUCCESS: Loading Fact Table '{self.table_name}'  - Finished Execution")

        except Exception as ex:
            logging.info(f"FAILED: Loading Fact Table '{self.table_name}' failed with error: {ex}")
