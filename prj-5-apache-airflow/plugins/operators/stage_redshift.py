from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os

class StageToRedshiftOperator(BaseOperator):
    """
    Custom operator to copy files stored in S3 to a redshift database
    
    args:
        s3_region: the region in which the s3 bucket is stored
        s3_bucket: s3 bucket name
        s3_key: the key for bucket, not that this is a template field so it can contain airflow template variables and macros for e.g. {{ execution_date.strftime("%Y-%m-%d") }}
        redshift_conn_id: the connection id for the redshift connection stored in airflow
        aws_credentials_id: the connection id for aws credential stored in airflow
        s3_format: the format of the source files e.g. JSON please see redshift COPY command docs for details
        staging_table: target table to copy data into
        s3_format_args: additional format arguments for the copy command again please see COPY command docs for details
    
    """
    template_fields = ['s3_key']
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 s3_region,
                 s3_bucket,
                 s3_key,
                 redshift_conn_id,
                 aws_credentials_id,
                 s3_format,
                 staging_table,
                 s3_format_args,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.s3_region=s3_region
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.s3_format=s3_format
        self.s3_format_args=s3_format_args
        self.staging_table=staging_table
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        db = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        truncate_command = f"TRUNCATE TABLE {self.staging_table}"
        
        self.log.info(f'Truncate staging table {self.staging_table}, truncate_command: {truncate_command}')
        
        db.run(truncate_command)

        s3_uri = os.path.join('s3://',self.s3_bucket,self.s3_key)
        
        self.log.info(f"""Copy data into staging table {self.staging_table}, copy_command: 
COPY {self.staging_table} FROM '{s3_uri}'
REGION '{self.s3_region}'
ACCESS_KEY_ID '*************'
SECRET_ACCESS_KEY '***************'
{self.s3_format} {self.s3_format_args}
TIMEFORMAT AS 'epochmillisecs';""")
        
        copy_command = f"""
COPY {self.staging_table} FROM '{s3_uri}'
REGION '{self.s3_region}'
ACCESS_KEY_ID '{credentials.access_key}'
SECRET_ACCESS_KEY '{credentials.secret_key}'
{self.s3_format} {self.s3_format_args}
TIMEFORMAT AS 'epochmillisecs';"""
        
        
        db.run(copy_command)