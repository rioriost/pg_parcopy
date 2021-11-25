# pg_parcopy

```
usage: pg_parcopy.py [-h] [--dbname DBNAME] --table TABLE [--count COUNT] [--directory DIRECTORY]
                     [--size SIZE] [--format {CSV,TEXT,BINARY}] [--host HOST] [--port PORT]
                     [--username USERNAME] [--password PASSWORD]

COPY table in parallel using psql command

optional arguments:
  -h, --help            show this help message and exit

General options:
  --dbname DBNAME, -d DBNAME
                        Database to be dumped (default: root)
  --table TABLE, -t TABLE
                        Table to be dumped
  --count COUNT, -c COUNT
                        Number of parallelized processes (should set to number of CPUs that
                        PostgreSQL server is using. default: 16)

Output options:
  --directory DIRECTORY
                        path of directory to save dump files
  --size SIZE, -s SIZE  target size of each dump files in MB
  --format {CSV,TEXT,BINARY}, -f {CSV,TEXT,BINARY}
                        table output format

Connection options:
  --host HOST           database server host (default: "localhost")
  --port PORT, -p PORT  database server port (default: "5432")
  --username USERNAME, -U USERNAME
                        database user name (default: "root")
  --password PASSWORD, -W PASSWORD
                        force password prompt (should happen automatically, default: $PGPASSWORD)
```
