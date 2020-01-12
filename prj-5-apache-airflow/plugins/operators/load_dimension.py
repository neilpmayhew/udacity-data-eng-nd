from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 sql_query,
                 target_table,
                 truncate,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id=redshift_conn_id
        self.sql_query=sql_query
        self.target_table=target_table
        self.truncate=truncate

    def execute(self, context):
        db = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info(f'Truncating target table: {self.target_table}')

            db.run(f"TRUNCATE TABLE {self.target_table}")
                
        insert_sql = f"""
INSERT INTO {self.target_table}
{self.sql_query}"""
            
        self.log.info(f'Loading dimension target table: {self.target_table}, insert_sql: {insert_sql}')

        db.run(insert_sql)                
            
