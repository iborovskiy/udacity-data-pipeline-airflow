from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dest_table="",
                 sql_query="",
                 append_mode=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dest_table = dest_table
        self.sql_query = sql_query
        self.append_mode = append_mode

    def execute(self, context):
        self.log.info('Set up connection to Redshift cluster')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Transferring data to dimension table")
        self.log.info(f"Destination dimension table: {self.dest_table}")
        
        if self.append_mode == False:
            self.log.info(f"Erasing destination table ({self.dest_table})")
            redshift.run(SqlQueries.table_delete.format(self.dest_table))

        self.log.info("Transferring data to dimension table")
        self.log.info(f"Destination dimension table: {self.dest_table}")
        composed_sql = """
            INSERT INTO {}
            {}
        """.format(self.dest_table, self.sql_query)
        redshift.run(composed_sql)
        
        self.log.info('LoadDimensionOperator successfully completed')