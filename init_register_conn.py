import configparser

from airflow import settings
from airflow.models import Connection,Variable


# fetch AWS_KEY and AWS_SECRET
config = configparser.ConfigParser()
config.read('/Users/sathishkaliamoorthy/.aws/credentials')
AWS_KEY = config['default']['aws_access_key_id']
AWS_SECRET = config['default']['aws_secret_access_key']
#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')


# inserting new connection object programmatically
aws_conn = Connection(
                conn_id='aws_credentials',
                conn_type='Amazon Web Services',
                login=AWS_KEY,
                password=AWS_SECRET
)

redshift_conn = Connection(
                conn_id='aws_redshift',
                conn_type='Postgres',
                host = 'my-sparkify-dwh.cdc1pzfmi32k.us-west-2.redshift.amazonaws.com',
                port = 5439,
                schema = 'sparkify_dwh',
                login='sparkifyuser',
                password='sparkifyUser01'
)
session = settings.Session()
session.add(aws_conn)
session.add(redshift_conn)
session.commit()

# inserting variables programmatically
Variable.set("s3_bucket", "udacity-dend")
