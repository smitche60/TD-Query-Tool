import tdclient
import config
import argparse
import sys

# -f / -- format is optional and specifies the output format: tabular by default
# -c / -- column is optional and specifies the comma separated list of columns to restrict the result toself.
# Read all columns if not specified.
# -l / -- limit is optional and specifies the limit of records returned. Read all records if not specified.
# -m / -- min is optional and specifies the minimum timestamp: NULL by default
# -M / -- MAX is optional and specifies the maximum timestamp: NULL by default
# -e / -- engine is optional and specifies the query engine: ‘presto’ by default

parser = argparse.ArgumentParser(prog='PROG')

parser.add_argument('-f', '--format', default = 'csv')
parser.add_argument('-c', '--column', nargs = '*', default = '*')
parser.add_argument('-e', '--engine', default = 'presto')
parser.add_argument('db_name')
parser.add_argument('table_name')

# parser.add_argument('-l', '--limit', default = 'NULL')
# parser.add_argument('-m', '--min', default = 'NULL')
# parser.add_argument('-M', '--MAX', default = 'NULL')

args = parser.parse_args(sys.argv[1:])

print(args.format, args.column, args.engine)
print(args.db_name, args.table_name)


# with tdclient.Client(config.TD_API_KEY) as td:
#     job = td.query("cities", "SELECT * FROM world_cities LIMIT 5", type="hive")
#     job.wait()
#     for row in job.result():
#         print(repr(row))
