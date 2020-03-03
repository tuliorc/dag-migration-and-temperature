from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator, Variable
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=',',
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = "redshift",
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        if self.table == 'public.df_migration':
            self.copy_sql =  """
                        COPY {}
                        FROM '{}'
                        IAM_ROLE '{}'
                        FORMAT AS PARQUET;
                        """
        else:
            self.copy_sql = """
                       COPY {}
                       FROM '{}'
                       CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
                       CSV
                       IGNOREHEADER {}
                       DELIMITER '{}';
                       """


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        iam_role = Variable.get('iam_role')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id[0])
        self.log.info("Clearing data from the Redshift table " + self.table)

        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Copying data from S3 to the staging Redshift table "\
                                                                + self.table)

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        if self.table == 'public.df_migration':
            formatted_sql = self.copy_sql.format(
                self.table,
                s3_path,
                iam_role,
            )

        else:
            formatted_sql = self.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
            )

        self.log.info(formatted_sql)
        redshift.run(formatted_sql)


class CapstonePlugin(AirflowPlugin):
    name = "capstone_plugin"
    operators = [StageToRedshiftOperator]
