from prefect import task, flow
import psycopg2
from prefect_airbyte.server import AirbyteServer
from prefect_airbyte.connections import AirbyteConnection
from prefect_airbyte.flows import run_connection_sync
from prefect import variables

from airbnb2.dbt_transformation import dbt_transformation

server = AirbyteServer(server_host=variables.get('server'), server_port=variables.get('port'))


@flow(name="Airbyte Stream")
def get_source_raw_data(connection):
    connection = AirbyteConnection(
        airbyte_server=server,
        connection_id=connection,
        status_updates=True,
    )
    sync_result = run_connection_sync(
        airbyte_connection=connection,
    )


@flow(name="Populate tables")
def populating_tables():
    get_source_raw_data(variables.get('hosts_net_id'))
    get_source_raw_data(variables.get('listings_net_id'))
    get_source_raw_data(variables.get('reviews_net_id'))


@task(name="Check table existence")
def check_table_existence(schema, table_names):
    try:
        conn = psycopg2.connect(host='localhost', port='5005', dbname='airbnb', user='root', password='root')
        cur = conn.cursor()
        for table_name in table_names:
            cur.execute(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = "
                f"'{schema}' AND table_name = '{table_name}');")
            result = cur.fetchone()[0]
            if not result:
                return False
        cur.close()
        conn.close()
        print("Table already exists,wont be recreated.")
        return True
    except psycopg2.Error:
        print("Tables doesnt exist, recreating")
        return False


@flow(name="Airbnb workflow", log_prints=True, retries=2, retry_delay_seconds=120)
def dbt_workflow():
    tables_to_check = ["raw_reviews", "raw_listings", "raw_hosts"]
    if not check_table_existence(schema="raw", table_names=tables_to_check):
        populating_tables()
    dbt_transformation()


if __name__ == "__main__":
    dbt_workflow()
    # dbt_workflow.serve("Airbnb workflow")
