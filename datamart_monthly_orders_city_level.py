from datetime import datetime
import snowflake.connector

def monthly_orders_city_level():
    schema = 'datamart'
    newtable = 'NORTHWIND.datamart.monthly_orders_city_level'
    columns = [
        {'name': 'date_key', 'type': 'DATE', 'column_type': 'matchon'},
        {'name': 'ship_country', 'type': 'string', 'column_type': 'matchon'},
        {'name': 'orders', 'type': 'NUMBER', 'column_type': 'update'}
    ]
    query = '''
    SELECT
        DISTINCT
        DATE_TRUNC('MONTH', ORDER_DATE) AS date_key,
        ship_country,
        count(distinct order_id) orders
    FROM
        NORTHWIND.RAW.ORDERS
    LEFT JOIN
        NORTHWIND.RAW.customers
        USING (customer_id)

    GROUP BY 1,2
    '''

    # Snowflake credentials
    sf_username = 'halimIskandar'
    sf_password = 'Halim1234567890'
    sf_account = 'SN51669.ap-southeast-3.aws'
    sf_warehouse = 'compute_wh'
    sf_database = 'northwind'  # Change the Snowflake database name to "northwind"
    sf_schema = schema  # New Snowflake schema name

    # Connect to Snowflake data warehouse
    sf_conn = snowflake.connector.connect(
        user=sf_username,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema
    )

    # Check if table name exists in the schema
    cursor = sf_conn.cursor()
    cursor.execute(f"SHOW TABLES LIKE '{newtable.upper()}'")
    exists = len(cursor.fetchall()) > 0

    if exists:
        # Merge data into the existing table
        merge_query = f'''
            MERGE INTO {newtable} AS target
            USING ({query}) AS source
            ON {' AND '.join(f'target.{col["name"]} = source.{col["name"]}' for col in columns if col['column_type'] == 'matchon')}
            WHEN MATCHED THEN
                UPDATE SET
                    {', '.join(f'target.{col["name"]} = source.{col["name"]}' for col in columns if col['column_type'] == 'update')}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(col["name"] for col in columns if col['column_type'] == 'matchon' or col['column_type'] == 'update')})
                VALUES ({', '.join(f'source.{col["name"]}' for col in columns if col['column_type'] == 'matchon' or col['column_type'] == 'update')})
        '''
        cursor.execute(merge_query)
    else:
        # Generate column definitions
        column_defs = ',\n'.join(f"{col['name']} {col['type']}" for col in columns)

        # Create the table
        create_query = f'''
            CREATE OR REPLACE TABLE {newtable} (
                {column_defs}
            )
            CLUSTER BY (date_key)
            AS
            {query}
        '''
        cursor.execute(create_query)

    # Close the cursor and connection
    cursor.close()
    sf_conn.close()
