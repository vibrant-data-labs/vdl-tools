import psycopg2
import json
from functools import wraps

from vdl_tools.shared_tools.database_cache.database_utils import CONN_PARAMS
from vdl_tools.shared_tools.tools.logger import logger


CONN_PARAMS = CONN_PARAMS.copy()
CONN_PARAMS["database"]  = "vdl_quick_cache"


def create_cache_table(conn_params):
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    id SERIAL PRIMARY KEY,
                    func_name TEXT NOT NULL,
                    args TEXT NOT NULL,
                    kwargs TEXT NOT NULL,
                    result TEXT NOT NULL
                )
            """)
            conn.commit()


def memoize_to_postgres(conn_params=CONN_PARAMS):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            json_args = json.dumps(args)
            json_kwargs = json.dumps(kwargs)
            with psycopg2.connect(**conn_params) as conn:
                with conn.cursor() as cur:
                    # Check if result exists in cache table
                    cur.execute(
                        "SELECT result FROM cache WHERE func_name = %s AND args = %s AND kwargs = %s",
                        (func.__name__, json_args, json_kwargs)
                    )
                    result = cur.fetchone()

                    if result:
                        logger.info(f"Using memoized {func.__name__} with args {args} and kwargs {kwargs}")
                        return json.loads(result[0])['result']

                    # Compute result if not found in cache
                    result = func(*args, **kwargs)
                    result = json.dumps({"result": result})

                    # Store result in cache table
                    cur.execute(
                        "INSERT INTO cache (func_name, args, kwargs, result) VALUES (%s, %s, %s, %s)",
                        (func.__name__, json_args, json_kwargs, result)
                    )
                    conn.commit()

                    result = json.loads(result)["result"]
                    return result

        return wrapper

    return decorator
