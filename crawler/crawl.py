#!/usr/bin/env python3

import sys
import traceback
from datetime import date, datetime, timedelta, time
import parsers.US_MISO
import parsers.US_PJM
import parsers.US_CAISO
import parsers.US_NEISO
import parsers.US_BPA
import parsers.US_NY
import parsers.US_SPP
import parsers.US_ERCOT
import parsers.US_PREPA
import parsers.US_EIA
from dateutil import tz, parser
import arrow
import psycopg2, psycopg2.extras
import argparse

map_regions = {
    'US-MISO': {
        # interval between pulling new data
        'updateFrequency': timedelta(minutes=3),
        'timeZone': tz.gettz('America/New_York'),
        'fetchFn': parsers.US_MISO.fetch_production,
        # Whether returned result is for a single timestamp or multiple
        'fetchResultIsList': False,
        # Whether the crawler supports an input timestamp, or only supports current time
        'fetchCurrentData': True
    },
    'US-PJM': {
        'updateFrequency': timedelta(minutes=30),
        'timeZone': tz.gettz('America/New_York'),
        'fetchFn': parsers.US_PJM.fetch_production,
        'fetchResultIsList': False,
        'fetchCurrentData': True
    },
    'US-CAISO': {
        # Daily feed, but updated every ~ 5min
        'updateFrequency': timedelta(minutes=5),
        'timeZone': tz.gettz('America/Los_Angeles'),
        'fetchFn': parsers.US_CAISO.fetch_production,
        'fetchResultIsList': True,
        'fetchCurrentData': False,
        'scheduledDowntime': (
            time(0, 0),
            time(0, 15)
        )
    },
    'US-NEISO': {
        # Daily feed, but updated every ~ 5min
        'updateFrequency': timedelta(minutes=5),
        'timeZone': tz.gettz('America/New_York'),
        'fetchFn': parsers.US_NEISO.fetch_production,
        'fetchResultIsList': True,
        'fetchCurrentData': False
    },
    'US-BPA': {
        # Daily feed, but updated every ~ 5min
        # BPA keeps about 2 days' history
        'updateFrequency': timedelta(minutes=5),
        'timeZone': tz.gettz('America/Los_Angeles'),
        'fetchFn': parsers.US_BPA.fetch_production,
        'fetchResultIsList': True,
        'fetchCurrentData': True
    },
    'US-NY': {
        # Daily feed, but updated every ~ 5min
        'updateFrequency': timedelta(minutes=5),
        'timeZone': tz.gettz('America/New_York'),
        'fetchFn': parsers.US_NY.fetch_production,
        'fetchResultIsList': True,
        'fetchCurrentData': False
    },
    'US-SPP': {
        # Realtime source is updated every 2 hours, but pulling more frequently to avoid missing data
        'updateFrequency': timedelta(minutes=5),
        'timeZone': tz.gettz('Etc/GMT'),
        'fetchFn': parsers.US_SPP.fetch_production,
        'fetchResultIsList': True,
        'fetchCurrentData': True
    },
    'US-ERCOT': {
        'updateFrequency': timedelta(days=1),
        'timeZone': tz.gettz('America/Chicago'),
        'fetchFn': parsers.US_EIA.fetch_production,
        'fetchResultIsList': True,
        'fetchCurrentData': False
    },
    'US-PACW': {
        'updateFrequency': timedelta(days=1),
        'timeZone': tz.gettz('America/Los_Angeles'),
        'fetchFn': parsers.US_EIA.fetch_production,
        'fetchResultIsList': True,
        'fetchCurrentData': False
    }
    # Disable Puerto Rico for now, as the data seems stale after 03/24/2022
    # 'US-PR': {
    #     # The source parser seems to indicate that this gets updated twice per hour, around :10 and :40,
    #     #   and it's better to avoid these times.
    #     'updateFrequency': timedelta(minutes=30),
    #     'timeZone': tz.gettz('America/Puerto_Rico'),
    #     'fetchFn': parsers.US_PREPA.fetch_production,
    #     'fetchResultIsList': False,
    #     'fetchCurrentData': True
    # },
    # HI data is also disabled for now due to stale data after 04/13/2022
}

MAP_OVERRIDE_FETCHFNS = {
    'us-eia': parsers.US_EIA.fetch_production
}
OVERRIDE_DATA_SOURCES = MAP_OVERRIDE_FETCHFNS.keys()

parser = argparse.ArgumentParser()
parser.add_argument('-B', '--backfill', action='store_true', help='Run backfill')
parser.add_argument('-D', '--days-for-backfill', type=int, help='Number of days to run backfill for')
parser.add_argument('--backfill-begin-date', type=lambda s: datetime.strptime(s, '%Y-%m-%d'), help='The begin date for backfill')
parser.add_argument('--backfill-end-date', type=lambda s: datetime.strptime(s, '%Y-%m-%d'), help='The end date for backfill')
parser.add_argument('-R', '--regions', nargs='+', choices=map_regions.keys(), help='Select a subset of regions')
parser.add_argument('-N', '--dry-run', action='store_true', help='Only pull data but do not write to database')
parser.add_argument('-F', '--force', action='store_true', help='Force pulling new data and ignore last updated')
parser.add_argument('--override-data-source', choices=OVERRIDE_DATA_SOURCES, help='Override the crawler data source')
args = parser.parse_args()

def getdbconn(host='/var/run/postgresql/', database="electricity-data"):
    try:
        conn = psycopg2.connect(host=host, database=database, user="crawler_rw")
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

def is_in_scheduled_downtime(region: str, e: Exception):
    if 'scheduledDowntime' not in map_regions[region]:
        return False
    scheduled_downtime = map_regions[region]['scheduledDowntime']
    current_local_time = datetime.now(tz=tz.gettz(map_regions[region]['timeZone'])).time()
    if scheduled_downtime[0] <= current_local_time <= scheduled_downtime[1]:
        print("Exception ignored during scheduled downtime:", str(e), file=sys.stderr)
        return True
    return False

def fetch_new_data(region, target_datetime: datetime = None):
    fetchFn = map_regions[region]['fetchFn']
    l_result = []
    if not args.backfill and map_regions[region]['fetchCurrentData']:
        target_datetime = None
    else:
        if map_regions[region]['updateFrequency'] >= timedelta(days=1):
            if target_datetime is None:
                target_datetime = arrow.now().shift(days=-1)
            target_datetime = arrow.get(target_datetime.date(), map_regions[region]['timeZone'])
        elif target_datetime is None:
            target_datetime = arrow.get(date.today(), map_regions[region]['timeZone'])
    print('Target datetime:', target_datetime)
    if args.override_data_source:
        fetchFn = MAP_OVERRIDE_FETCHFNS[args.override_data_source]
    try:
        l_data = fetchFn(zone_key=region, target_datetime=target_datetime)
        if not map_regions[region]['fetchResultIsList'] and not args.override_data_source:
            l_data = [l_data]
    except Exception as e:
        print("Failed to execute fetchFn. See stderr log for details.")
        if not is_in_scheduled_downtime(region, e):
            raise e
        else:
            l_data = []
    l_data.sort(key=lambda e: e['datetime'])
    for data in l_data:
        timestamp = arrow.get(data['datetime']).to(map_regions[region]['timeZone']).datetime
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
    return len(rows)

def fetchandupdate(conn, region, run_timestamp):
    if args.backfill:
        l_result = []
        if args.days_for_backfill:
            for date_offset in range(args.days_for_backfill):
                l_result += fetch_new_data(region, target_datetime=arrow.get().shift(days= -1 - date_offset))
        elif args.backfill_begin_date and args.backfill_end_date:
            backfill_current_date = arrow.get(args.backfill_begin_date)
            backfill_end_date = arrow.get(args.backfill_end_date)
            while backfill_current_date <= backfill_end_date:
                l_result += fetch_new_data(region, target_datetime=backfill_current_date)
                backfill_current_date = backfill_current_date.shift(days=1)
        else:
            raise NotImplementedError()
    else:
        l_result = fetch_new_data(region)
    if args.dry_run:
        print('Dry run mode on. Not updating database ...')
    else:
        total_rows_uploaded = 0
        for (timestamp, d_power_mw_by_category) in l_result:
            total_rows_uploaded += upload_new_data(conn, region, timestamp, d_power_mw_by_category)
        print(f'Uploaded {total_rows_uploaded} rows.')
        if not args.backfill:
            set_last_updated(conn, region, run_timestamp)

def should_run_now(conn, region, run_timestamp):
    last_updated = get_last_updated(conn, region)
    delta_since_last_update = run_timestamp - last_updated
    print("Last updated: %s (%.0f min ago)" % (str(last_updated), delta_since_last_update.total_seconds() / 60))
    return delta_since_last_update > map_regions[region]['updateFrequency']

def crawl_region(conn, region):
    print(f'Region: {region}')
    run_timestamp = datetime.now()
    if args.backfill or args.force:
        fetchandupdate(conn, region, run_timestamp)
    else:
        if should_run_now(conn, region, run_timestamp):
            fetchandupdate(conn, region, run_timestamp)

def crawlall():
    print("Electricity data crawler running at", str(datetime.now()))
    if args.dry_run:
        print('Dry run mode is on.')
    if args.backfill:
        print("Backfill mode is on.")
        if args.days_for_backfill:
            print(f"# of days to backfill is {args.days_for_backfill}.")
        elif args.backfill_begin_date and args.backfill_end_date:
            if args.backfill_begin_date > args.backfill_end_date:
                raise ValueError("backfill-begin-date cannot be later than backfill-end-date")
            print('Backfill date range: [%s, %s]' % (\
                args.backfill_begin_date.strftime('%Y-%m-%d'),
                args.backfill_end_date.strftime('%Y-%m-%d')
                ))
        else:
            raise ValueError("Backfill mode is on, but neither days-for-backfill nor (backfill-begin-date and backfill-end-date) is specified.")
    conn = getdbconn()
    for region in map_regions:
        if args.regions and region not in args.regions:
            continue
        try:
            crawl_region(conn, region)
        except Exception as e:
            print(datetime.now().isoformat(),
                    f"Exception occurred while crawling region {region}:",
                    e,
                    file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)

if __name__ == '__main__':
    crawlall()
