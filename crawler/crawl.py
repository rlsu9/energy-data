#!/usr/bin/env python3

from datetime import datetime, timedelta
import parsers.US_MISO
import parsers.US_PJM
from dateutil import tz, parser
import psycopg2, psycopg2.extras

map_regions = {
    'US-MISO': {
        'updateFrequency': timedelta(minutes=3),
        'timeZone': tz.gettz('America/New_York'),
        'fetchFn': parsers.US_MISO.fetch_production,
    },
    'US-PJM': {
        'updateFrequency': timedelta(minutes=30),
        'timeZone': tz.gettz('America/New_York'),
        'fetchFn': parsers.US_PJM.fetch_production,
    }
}

def getdbconn(host='/var/run/postgresql/', database="electricity-data"):
    try:
        conn = psycopg2.connect(host=host, database=database, user="postgres")
        return conn
    except Exception as e:
        print("Failed to connect to database.")
        raise e

def get_last_updated(conn, region):
    cur = conn.cursor()
    try:
        cur.execute("""SELECT LastUpdated FROM LastUpdated WHERE Region = %s""", [region])
        result = cur.fetchone()
    except Exception as e:
        print("Failed to execute get_last_updated query.")
        raise e
    return result[0] if result is not None else datetime.min

def set_last_updated(conn, region, run_timestamp):
    cur = conn.cursor()
    try:
        cur.execute("""INSERT INTO LastUpdated (Region, LastUpdated) VALUES (%s, %s)
                        ON CONFLICT (Region) DO UPDATE SET LastUpdated = EXCLUDED.LastUpdated""",
                        [region, run_timestamp])
        conn.commit()
    except Exception as e:
        print("Failed to execute set_last_updated query.")
        raise e

def fetch_new_data(region):
    fetchFn = map_regions[region]['fetchFn']
    try:
        data = fetchFn()
        timestamp = data['datetime']
        print('time:', timestamp)
        d_power_mw_by_category = data['production']
        for category in d_power_mw_by_category:
            power_mw = d_power_mw_by_category[category]
            print('category: %s, power_mw: %f' % (category, power_mw))
        pass
    except Exception as e:
        print("Failed to execute query.")
        raise e
    return (timestamp, d_power_mw_by_category)

def upload_new_data(conn, region, timestamp, d_power_mw_by_category):
    rows = []
    for category in d_power_mw_by_category:
        power_mw = d_power_mw_by_category[category]
        row = (timestamp, category, power_mw, region)
        rows.append(row)
    cur = conn.cursor()
    try:
        psycopg2.extras.execute_values(cur,
            """INSERT INTO EnergyMixture (datetime, category, power_mw, region) VALUES %s ON CONFLICT DO NOTHING""",
            rows)
        conn.commit()
    except Exception as e:
        print("Failed to upload new data")
        raise e

def fetchandupdate(conn, region, run_timestamp):
    (timestamp, d_power_mw_by_category) = fetch_new_data(region)
    upload_new_data(conn, region, timestamp, d_power_mw_by_category)
    set_last_updated(conn, region, run_timestamp)

def crawlall():
    conn = getdbconn()
    for region in map_regions:
        last_updated = get_last_updated(conn, region)
        run_timestamp = datetime.now()
        delta_since_last_update = run_timestamp - last_updated
        print("region: %s, last updated: %s (%.0f min ago)" % (region, str(last_updated), delta_since_last_update.total_seconds() / 60))
        if delta_since_last_update < map_regions[region]['updateFrequency']:
            continue
        fetchandupdate(conn, region, run_timestamp)

if __name__ == '__main__':
    crawlall()
