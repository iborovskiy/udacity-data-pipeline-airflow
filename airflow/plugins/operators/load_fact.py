from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dest_table="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dest_table = dest_table
        self.sql_query = sql_query

    def execute(self, context):
        self.log.info('Set up connection to Redshift cluster')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Transferring data to fact table")
        self.log.info(f"Destination fact table: {self.dest_table}")
        composed_sql = """
            INSERT INTO {}
            {}
        """.format(self.dest_table, self.sql_query)
        redshift.run(composed_sql)
        
        self.log.info('LoadFactOperator successfully completed')

