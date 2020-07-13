from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 check_list=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id=redshift_conn_id
        self.check_list=check_list

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)        
        self.log.info("Redshift connection created")
        
        self.log.info("Begin constructing test sql ")
        for check in check_list:
            sql_template = check["sql_template"]
            expected = check["expected_result"]
            column = check["check_column"]
            table = check["check_table"]
            check_type = check["check_type"]
            exec_sql = sql_template.format( check_column=column,
            check_table=table,
                               )
            self.log.info(f"Requesting redshift to run the following sql query: {exec_sql}")
            records = redshift_hook.get_records(exec_sql)
            num_records = records[0][0]
            if num_records != expected:
                raise ValueError(f"Data quality {check_type} check failed for {column} in table {table}.\
                Expected {expected}, got {num_records}")
            else: self.log.info(f"Data quality {check_type} check on table {table} passed with {num_records} records")
        
        self.log.info(f"Data quality checks completed")