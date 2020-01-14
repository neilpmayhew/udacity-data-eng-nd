from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

class DataQualityOperator(BaseOperator):
    """
    Operator to enable flexible data quality checks to be performed. Execute loops through the Dict passed in via data_quality_queries and runs each
    against the redshift db (as per redshift_conn_id).Each query is expected to return only a single row which is evaluated using the Python bool() function. If 1 or more
    columns in the row eval false the test is considered to have failed. A message is written to the log using the key as the test_name. 
    
    A count of failing queries is kept and if this is 1 or more then an AirflowException is raised.
    
    Args:
    
    redshift_conn_id=connection id of the redshift db to run the queries against
    
    data_quality_queries=Python Dict of data quality queries the key is test name, the value the querty to be run
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 data_quality_queries,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id=redshift_conn_id
        self.data_quality_queries=data_quality_queries

    def execute(self, context):
        db = PostgresHook(postgres_conn_id=self.redshift_conn_id) 

        check_failure_count = 0

        for test_name, data_check_query in self.data_quality_queries.items():
            rows = db.get_first(data_check_query)

            if not rows:
                check_failure_count+=1
                self.log.info(f"query {data_check_query} returned None (expected single row)")
            elif not all([bool(r) for r in rows]):
                check_failure_count+=1
                self.log.info(f"Data quality check failed.\nQuery:\n{data_check_query}\nRows:\n{rows!s}")                
        
        if check_failure_count > 0:
            raise AirflowException(f"{check_failure_count} data quality checks have failed. Please check the logs for details")