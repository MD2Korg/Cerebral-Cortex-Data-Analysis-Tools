from influxdb import InfluxDBClient
import glob
import os
import bz2
from pprint import pprint
import multiprocessing
from joblib import Parallel, delayed
import sys

db = 'rice'

def parseFile(f):
    client = InfluxDBClient('localhost', 8086, '', '', db)
    client.create_database(db)
    data = []
    basename = os.path.basename(f)[:-8]
    uuid, ds_id, app, identifier = basename.split('+', 3)
    print(uuid, app)
    print(identifier)
    try:
        with bz2.open(f, 'rt') as input_file:
            for l in input_file:
                ts, offset, values = l.rstrip().split(',', 2)
                ts = int(ts)*1000000

                data_values = values.split(',')
                object = {}
                object['measurement'] = identifier
                object['tags'] = {'owner': uuid, 'application': app}
                object['time'] = ts
                try:
                    sample = map(float,data_values)
                    object['fields'] = {}
                    for i, s in enumerate(sample):
                        object['fields']['value_'+str(i)] = s
                except :
                    object['fields'] = {'value': values}

                data.append(object)
                if len(data) >= 1000000:
                    print('Yielding:', uuid, len(data), identifier)
                    client.write_points(data)
                    data = []


    except ValueError as e:
        print("Value Error: ", e, basename)
    client.write_points(data)
    return True


if __name__=='__main__':
    num_cores = multiprocessing.cpu_count()
    files = glob.glob(db + '/' + sys.argv[1] + '*/*.bz2')


    sizes = [ (filename, os.stat(filename).st_size) for filename in files ]
    sizes.sort(key=lambda tup: tup[1])

    #for f in files:
    #   parseFile(f)
    results = Parallel(n_jobs=num_cores-4, verbose=11)(delayed(parseFile)(f) for f,size in sizes)
    print("Complete: " + str(len(results)) + " datastreams exported")
