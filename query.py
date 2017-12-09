import tdclient
import config
import argparse
import sys
from prettytable import PrettyTable
import csv

# Initialize parser and add arguments
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

# Validate input
if args.format != 'tabular' and args.format != 'csv':
    sys.exit('Please enter a valid format (Either "tabular" or "csv")')

if args.engine != 'presto' and args.engine != 'hive':
    sys.exit('Please enter a valid engine (Either "presto" or "hive")')

if not args.table_name:
    sys.exit('table_name must be defined')

if not args.db_name:
    sys.exit('db_name must be defined')

if not isinstance(args.limit, int) and args.limit != 'NULL':
    sys.exit('limit parameter must be an integer')

if args.min > args.MAX:
    sys.exit('Min timestamp value must be smaller than the max timestamp value')

if not isinstance(args.min, str) and args.min != 'NULL':
    sys.exit('Min timstamp value must be unix time or NULL')

if not isinstance(args.MAX, str) and args.MAX != 'NULL':
    sys.exit('Max timstamp value must be unix time or NULL')

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

    emptyResult = true
    for row in job.result():
        emptyResult = false

    if emptyResult == true:
        sys.exit("Query returned no results")

    if args.format == 'tabular':
        t = PrettyTable(columns)
        t.align = 'r'
        for row in job.result():
            t.add_row(row)
        print(t)
        sys.exit(0)

    if args.format == 'csv':
        with open('results.csv', 'w') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer = csv.writer(f)
            writer.writerows(job.result())
            sys.exit(0)
