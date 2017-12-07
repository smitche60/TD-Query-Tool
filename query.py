import tdclient
import config

with tdclient.Client(config.TD_API_KEY) as td:
    job = td.query("cities", "SELECT * FROM world_cities LIMIT 5", type="hive")
    job.wait()
    for row in job.result():
        print(repr(row))
