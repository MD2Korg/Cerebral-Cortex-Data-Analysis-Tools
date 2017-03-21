import argparse as argparse
import bz2
import datetime
import glob
from pprint import pprint

import matplotlib.collections as col
import matplotlib.dates as dates
import matplotlib.pyplot as plt
import numpy as np


def process_data(data):
    x = np.array(data)
    delta = np.diff(x)
    i = np.where(delta > 300 * 1000)
    return np.vstack((x[i], x[i] + delta[i])).T


def process(participant, directory, filter_string='', start_day=None, end_day=None):
    offset = 0
    lines = []
    gst = 1e100
    get = 0
    labels = []
    for f in glob.glob(directory + '/' + participant + filter_string + '.bz2'):
        if f.find('BATT') != -1 or f.find('stream') != -1 or f.find('QUALITY') != -1:

            print(f)
            params = f[:-16].split('+')[3:]
            labels.append('__'.join(params))
            offset += 1
            with bz2.open(f, 'rt') as csvFile:
                data = []
                for l in csvFile.readlines():
                    items = l.split(',')
                    ts = int(items[0])
                    if start_day is not None and end_day is not None:
                        if ts < start_day or end_day < ts:
                            continue

                    data.append(ts)

                    if ts < gst:
                        gst = ts
                    if ts > get:
                        get = ts

                blocks = process_data(data)
                if len(blocks) > 0:
                    pprint(blocks)
                    for segment in blocks:
                        lines.append([(dates.date2num(datetime.datetime.utcfromtimestamp(segment[0] / 1000.0)), offset),
                                      (
                                      dates.date2num(datetime.datetime.utcfromtimestamp(segment[1] / 1000.0)), offset)])

    return lines, offset, labels, gst, get


def render(lines, offset, labels, st, et):
    lc = col.LineCollection(lines)
    fig, ax = plt.subplots()
    ax.add_collection(lc)
    ax.xaxis.set_major_locator(dates.DayLocator())
    ax.xaxis.set_major_formatter(dates.DateFormatter('%D'))
    ax.xaxis.set_minor_locator(dates.HourLocator(np.arange(6, 25, 6)))
    ax.xaxis.set_minor_formatter(dates.DateFormatter('%H'))

    fig.subplots_adjust(left=.25, right=.99, top=.99)

    plt.xlim([dates.date2num(datetime.datetime.utcfromtimestamp(st / 1000.0)),
              dates.date2num(datetime.datetime.utcfromtimestamp(et / 1000.0))])
    plt.ylim([0, offset + 1])
    plt.yticks(range(1, offset + 1), labels)
    plt.xticks(rotation=90)

    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Export processor for Cerebral Cortex Data Dumps')

    parser.add_argument('--data_directory', help='Data Directory', required=True)
    parser.add_argument('--participant', help='Participant UUID', required=True)
    parser.add_argument('--start_day', help='Day string representing the first day to begin scanning for data',
                        required=False)
    parser.add_argument('--end_day', help='Day string representing the day to end  scanning for data', required=False)

    args = parser.parse_args()

    participant = args.participant
    data_dir = args.data_directory

    if args.start_day and args.end_day:
        print('HERE')
        start_day = args.start_day
        end_day = args.end_day

        start_year = int(start_day[:4])
        start_month = int(start_day[4:6])
        start_day = int(start_day[6:])

        end_year = int(end_day[:4])
        end_month = int(end_day[4:6])
        end_day = int(end_day[6:])

        epoch = datetime.datetime.utcfromtimestamp(0)
        start_date = int((datetime.datetime(start_year, start_month, start_day) - epoch).total_seconds() * 1e3)
        end_date = int((datetime.datetime(end_year, end_month, end_day) - epoch).total_seconds() * 1e3)

        lines, offset, labels, gst, get = process(participant, data_dir, filter_string='*', start_day=start_date,
                                                  end_day=end_date)
    else:
        lines, offset, labels, gst, get = process(participant, data_dir, filter_string='*')
    render(lines, offset, labels, gst, get)
