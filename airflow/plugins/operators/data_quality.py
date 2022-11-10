from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_test_query="",
                 tests=[],
                 results=[],
                 not_match=False,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests
        self.results = results
        self.not_match = not_match

    def execute(self, context):
        self.log.info('Set up connection to Redshift cluster')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for test, res in zip(self.tests, self.results):
            self.log.info('Running quality test query')
            res_records = redshift.get_records(test)
            if len(res_records[0]) < 1 or len(res_records) < 1:
                raise ValueError("Data quality check failed. Test query returned no records")
            res_value = res_records[0][0]
            if ((self.not_match == True) and (res_value == res)) or \
               ((self.not_match == False) and (res_value != res)):
                raise ValueError("Data quality check failed. Test hasn't passed")
            else:
                self.log.info('Quality test passed')
        self.log.info('DataQualityOperator successfully completed')