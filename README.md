# How to configure and run

1. Clone repo locally
2. Install dependencies (tdclient, prettytable) 
3. Add a file named 'config.py' in the root directory that contains the appropriate Treasure Data API key
4. Run query.py from the terminal, passing the query parameters in the following format:
```
$ query.py -f csv -e hive -c my_col1,my_col2,my_col -m 1427347140 -M 1427350725 -l 100 my_db my_table
```
