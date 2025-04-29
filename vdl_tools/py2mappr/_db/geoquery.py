import math
from pydantic import BaseModel
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.tools.config_utils import get_configuration
import vdl_tools.shared_tools.tools.log_utils as log
import ast

class GeoItem(BaseModel):
    key: str
    latitude: float
    longitude: float

class GeoResponse(BaseModel):
    osm_id: int
    points: list[str]


def get_geo_configuration():
    config = get_configuration()

    if 'geodb' in config:
        return {
            'postgres': {
                'host': config['geodb']['host'],
                'port': config['geodb']['port'],
                'user': config['geodb']['user'],
                'password': config['geodb']['password'],
                'database': config['geodb']['database']
            }
        }

    log.warn('[geodb] section is not found in the configuration')
    # expecting 'postgres' in config
    return config


def query_latlon(detail_level: int, items: list[GeoItem]):
    with get_session(config=get_geo_configuration()) as session:
        conn = session.connection()
        items_for_db = [(item.key, item.latitude, item.longitude) for item in items if not math.isnan(items[2].latitude)]
        results = []
        cur = conn.connection.cursor()
        cur.execute(
            "SELECT get_geo_areas(%s::int2, %s::geo_query[])",
            (detail_level, items_for_db),
        )
        results.extend(cur.fetchall())
        cur.close()

        ret_data: list[GeoResponse] = []
        for item in results:
            try:
                [sql_response] = item
                osm_id, datapoints = ast.literal_eval(sql_response)
                datapoints = ast.literal_eval(datapoints) if type(datapoints) == str else datapoints

                ret_data.append(GeoResponse(
                    osm_id=osm_id,
                    points=[str(x) for x in list(datapoints)]
                ))

            except Exception as e:
                print(f"Error getting the response: {e}")

        return ret_data
