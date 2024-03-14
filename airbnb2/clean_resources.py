import psycopg2

def drop_all_tables_and_views(schema):
    try:
        conn = psycopg2.connect(host='localhost', port='5005', dbname='airbnb', user='root', password='root')
        cur = conn.cursor()

        # Drop tables
        cur.execute(
            f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE';")
        tables = cur.fetchall()
        for table in tables:
            cur.execute(f"DROP TABLE IF EXISTS {schema}.{table[0]} CASCADE;")

        # Drop views
        cur.execute(f"SELECT table_name FROM information_schema.views WHERE table_schema = '{schema}';")
        views = cur.fetchall()
        for view in views:
            cur.execute(f"DROP VIEW IF EXISTS {schema}.{view[0]} CASCADE;")

        conn.commit()
        print("All tables and views dropped successfully.")
    except psycopg2.Error as e:
        print("Error dropping tables and views:", e)
    finally:
        cur.close()
        conn.close()


# Usage
drop_all_tables_and_views('raw')
