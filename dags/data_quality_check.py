from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = "redshift"
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id[0])
        for table in self.tables:
            self.log.info("Data quality test running for table " + table)
            records = redshift.get_records("SELECT COUNT(*) FROM " + table)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed. " + table + \
                                                        "returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError("Data quality check failed. " + table + \
                                                    "contains 0 rows")
            self.log.info(f"Data quality on table " + table + " check passed\
                                            with " + num_records + " records")
