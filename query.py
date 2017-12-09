import tdclient
import config
import argparse
import sys
from prettytable import PrettyTable
import csv

# Initial parser and add arguments
parser = argparse.ArgumentParser(prog='PROG')
parser.add_argument('-f', '--format', default = 'tablular')
parser.add_argument('-c', '--column', nargs = '*', default = '*')
parser.add_argument('-e', '--engine', default = 'presto')
parser.add_argument('db_name')
parser.add_argument('table_name')
parser.add_argument('-l', '--limit', default = 'NULL')
parser.add_argument('-m', '--min', default = 'NULL')
parser.add_argument('-M', '--MAX', default = 'NULL')

# Parse args
args = parser.parse_args(sys.argv[1:])

# Build query string
if args.column == '*':
    columns = ['time', 'total_addresses', 'blocksize', 'price_USD', 'hashrate', 'total_eth_growth', 'market_cap_value', 'transactions']
else:
    columns = args.column

query = 'SELECT ' + ', '.join(columns) + ' FROM ' + args.table_name + ' '

if args.min != 'NULL' and args.MAX != 'NULL':
    time_range = "WHERE TD_TIME_RANGE(time," + args.min + "," + args.MAX + ")"
    query += time_range

if args.limit != 'NULL':
    query += ' LIMIT ' + args.limit

print(query)

# Run query and output results
with tdclient.Client(config.TD_API_KEY) as td:
    job = td.query(args.db_name, query, type=args.engine)
    job.wait()

    if args.format == 'tabular':
        t = PrettyTable(columns)
        t.align = 'r'
        for row in job.result():
            t.add_row(row)
        print(t)

    if args.format == 'csv':
        with open('results.csv', 'w') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer = csv.writer(f)
            writer.writerows(job.result())
