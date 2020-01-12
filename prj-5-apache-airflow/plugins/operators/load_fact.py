from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 sql_query,
                 target_table,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id=redshift_conn_id
        self.sql_query=sql_query
        self.target_table=target_table

    def execute(self, context):
        db = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        insert_sql = f"""
INSERT INTO {self.target_table}
{self.sql_query}"""
        self.log.info(f'Loading dimension target table: {self.target_table}, insert_sql: {insert_sql}')

        db.run(insert_sql)
