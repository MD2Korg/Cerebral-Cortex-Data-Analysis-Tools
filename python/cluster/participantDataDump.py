import argparse
import bz2
import csv
import datetime
import json
import multiprocessing
import os

import psycopg2
from cassandra.cluster import Cluster
from joblib import Parallel, delayed

parser = argparse.ArgumentParser(description='Export datastreams from the Cerebral Cortex Cassandra store.')

parser.add_argument('--server', help='Cassandra server to connect with', required=True)
parser.add_argument('--keyspace', help='Cassandra keyspace', required=True)
parser.add_argument('--path', help='Base filesystem path', required=True)
parser.add_argument('--participant', help='Participant UUID', required=True)
parser.add_argument('--startday', help='Day string representing the first day to begin scanning for data',
                    required=True)
parser.add_argument('--endday', help='Day string representing the day to end  scanning for data', required=True)

args = parser.parse_args()

# Setup Variables
filepath = args.path

startday = args.startday
endday = args.endday

startyear = int(startday[:4])
startmonth = int(startday[4:6])
startday = int(startday[6:])

endyear = int(endday[:4])
endmonth = int(endday[4:6])
endday = int(endday[6:])

startdate = datetime.date(startyear, startmonth, startday)
enddate = datetime.date(endyear, endmonth, endday)

epoch = datetime.datetime.utcfromtimestamp(0)


def getDatastreamIDs(identifier):
    conn = psycopg2.connect("dbname=" + args.keyspace + " user=cerebralcortex")
    cur = conn.cursor()

    searchStmt = 'select datastreams.id, datastreams.participant_id, datasources.identifier, datasources.datasourcetype, m_cerebrum_applications.identifier, m_cerebrum_platforms.identifier, m_cerebrum_platforms.platformtype '
    searchStmt += 'from datastreams inner join datasources on datasources.id=datastreams.datasource_id '
    searchStmt += 'inner join m_cerebrum_applications on m_cerebrum_applications.id=datasources.m_cerebrum_application_id '
    searchStmt += 'inner join m_cerebrum_platforms on m_cerebrum_platforms.id=datasources.m_cerebrum_platform_id '
    searchStmt += 'where datastreams.participant_id=\'' + args.participant + '\''

    datastreams = []
    cur.execute(searchStmt)
    results = cur.fetchall()
    for i in results:
        datastreams.append(i)

    cur.close()
    conn.close()

    return datastreams


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def flatten(structure, key="", path="", flattened=None):
    if flattened is None:
        flattened = {}
    if type(structure) not in (dict, list):
        flattened[((path + "_") if path else "") + key] = structure
    elif isinstance(structure, list):
        for i, item in enumerate(structure):
            flatten(item, "%d" % i, "_".join(filter(None, [path, key])), flattened)
    else:
        for new_key, value in structure.items():
            flatten(value, new_key, "_".join(filter(None, [path, key])), flattened)
    return flattened


def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0


def rowProcessor(r):
    row = []
    row.append(int(unix_time_millis(r.datetime)))
    # row.append(r.datetime)
    row.append(r.offset)

    json_parsed = json.loads(r.sample)
    for j in json_parsed:
        val = ''
        if type(j) is dict:
            val = json.dumps(j).encode('utf8')
        else:
            val = j
        row.append(val)

    return row


def extractDataStream(datastream):
    segment_char = '+'

    dsid = str(datastream[0])
    participantid = str(datastream[1])
    datasource_id = str(datastream[2])
    datasource_type = str(datastream[3])
    app_id = str(datastream[4])
    platform_id = str(datastream[5])
    platform_type = str(datastream[6])

    cluster = Cluster([args.server])

    session = cluster.connect(args.keyspace)

    if not os.path.exists(filepath + '/' + participantid):
        os.mkdir(filepath + '/' + participantid)

    filename_base = filepath + '/' + participantid + '/' + participantid + segment_char + dsid + segment_char + app_id + segment_char + datasource_type
    if datastream[2] is not None:
        filename_base += segment_char + datasource_id
    if datastream[6] is not None:
        filename_base += segment_char + platform_type
    if datastream[5] is not None:
        filename_base += segment_char + platform_id

    with bz2.BZ2File(filename_base + '.csv.bz2', 'w') as csvfile:
        outputwriter = csv.writer(csvfile, delimiter=',', quotechar="'")
        for day in daterange(startdate, enddate):
            yyyymmdd = day.strftime('%Y%m%d')

            stmt = 'SELECT datetime, offset, sample FROM rawdata where datastream_id=' + dsid + ' and day=\'' + yyyymmdd + '\''
            rows = session.execute(stmt, timeout=180.0)
            flag = False
            for r in rows:
                flag = True
                outputwriter.writerow(rowProcessor(r))

            if flag:
                print('COMPLETED: ' + stmt)

    return True


if __name__ == "__main__":
    num_cores = multiprocessing.cpu_count()
    datastreams = getDatastreamIDs(args.participant)
    results = Parallel(n_jobs=num_cores)(delayed(extractDataStream)(i) for i in datastreams)
    print("Complete: " + str(len(results)) + " datastreams exported")
