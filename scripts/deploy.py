import snowflake.connector
import os

conn = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    role=os.environ['SNOWFLAKE_ROLE'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE']
)

cur = conn.cursor()

sql_found = False

for root, _, files in os.walk("sql"):
    for file in sorted(files):
        if file.endswith(".sql"):
            sql_found = True
            file_path = os.path.join(root, file)
            print(f"Executing {file_path}")
            with open(file_path) as f:
                cur.execute(f.read())

if not sql_found:
    raise Exception("❌ No SQL files found to execute")

cur.close()
conn.close()