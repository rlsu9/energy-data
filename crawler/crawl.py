#!/usr/bin/env python3

from datetime import datetime, timedelta
import parsers.US_MISO
import parsers.US_PJM
import parsers.US_CA
import parsers.US_NEISO
import parsers.US_BPA
from dateutil import tz, parser
import arrow
import psycopg2, psycopg2.extras

map_regions = {
    'US-MISO': {
        'updateFrequency': timedelta(minutes=3),
        'timeZone': tz.gettz('America/New_York'),
        'fetchFn': parsers.US_MISO.fetch_production,
        'fetchResultIsList': False,
        'currentDataOnly': True
    },
    'US-PJM': {
        'updateFrequency': timedelta(minutes=30),
        'timeZone': tz.gettz('America/New_York'),
        'fetchFn': parsers.US_PJM.fetch_production,
        'fetchResultIsList': False,
        'currentDataOnly': True
    },
    'US-CA': {
        'updateFrequency': timedelta(days=1),
        'timeZone': tz.gettz('America/Los_Angeles'),
        'fetchFn': parsers.US_CA.fetch_production,
        'fetchResultIsList': True,
        'currentDataOnly': False
    },
    'US-NEISO': {
        'updateFrequency': timedelta(days=1),
        'timeZone': tz.gettz('America/New_York'),
        'fetchFn': parsers.US_NEISO.fetch_production,
        'fetchResultIsList': True,
        'currentDataOnly': False
    },
    'US-BPA': {
        'updateFrequency': timedelta(days=1),
        'timeZone': tz.gettz('America/Los_Angeles'),
        'fetchFn': parsers.US_BPA.fetch_production,
        'fetchResultIsList': True,
        'currentDataOnly': True
    },
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
    l_result = []
    target_datetime = None
    if map_regions[region]['updateFrequency'] >= timedelta(days=1) and not map_regions[region]['currentDataOnly']:
        target_datetime = arrow.get(arrow.now().shift(days=-1).date(), map_regions[region]['timeZone'])
    print('Target datetime:', target_datetime)
    try:
        l_data = fetchFn(target_datetime=target_datetime)
        if not map_regions[region]['fetchResultIsList']:
            l_data = [l_data]
    except Exception as e:
        print("Failed to execute query.")
        raise e
    for data in l_data:
        timestamp = data['datetime']
        print('time:', timestamp)
        d_power_mw_by_category = data['production']
        for category in d_power_mw_by_category:
            power_mw = d_power_mw_by_category[category]
            print('\tcategory: %s, power_mw: %f' % (category, power_mw))
        l_result.append((timestamp, d_power_mw_by_category))
    return l_result

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
    l_result = fetch_new_data(region)
    for (timestamp, d_power_mw_by_category) in l_result:
        upload_new_data(conn, region, timestamp, d_power_mw_by_category)
    set_last_updated(conn, region, run_timestamp)

def crawlall():
    print("Electricity data crawler running at", str(datetime.now()))
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
