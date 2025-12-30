import snowflake.connector
import os

conn = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    private_key=os.environ['SNOWFLAKE_PRIVATE_KEY'],
    role=os.environ['SNOWFLAKE_ROLE'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE']
)

cur = conn.cursor()

for root, _, files in os.walk("sql"):
    for file in sorted(files):
        if file.endswith(".sql"):
            with open(os.path.join(root, file)) as f:
                sql = f.read()
                cur.execute(sql)

cur.close()
conn.close()