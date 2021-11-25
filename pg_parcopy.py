#!/usr/bin/env python3

import argparse
import datetime
import math
import os
import re
import subprocess
import sys
import time
from getpass import getpass
from shutil import which
from subprocess import PIPE

try:
    import inquirer
except ModuleNotFoundError:
    print('pip3 install inquirer')
    sys.exit(1)

import psutil


def check_psql():
    if which('psql') == None:
        print('Please install psql before executing this script.')
        print('(macOS): brew install postgresql')
        print('(RHEL/CentOS): dnf install postgresql')
        print('(debian/Ubuntu): apt-get install postgresql-client')
        sys.exit(1)


def set_args():
    parser = argparse.ArgumentParser(
        description='COPY table in parallel using psql command')
    group1 = parser.add_argument_group('General options')
    group1.add_argument("--dbname", '-d',
                        default=os.getenv('USER'),
                        type=str,
                        help='Database to be dumped (default: {})'.format(os.getenv('USER')))
    group1.add_argument("--table", "-t",
                        type=str,
                        required=True,
                        help='Table to be dumped')
    group1.add_argument("--count", "-c",
                        type=int,
                        default=os.cpu_count(),
                        help='Number of parallelized processes (should set to number of CPUs that PostgreSQL server is using. default: {})'.format(os.cpu_count()))
    group2 = parser.add_argument_group('Output options')
    group2.add_argument('--directory',
                        type=str,
                        help='path of directory to save dump files')
    group2.add_argument('--size', '-s',
                        type=int,
                        default=128,
                        help='target size of each dump files in MB (default: 128MB)')
    group2.add_argument("--format", "-f",
                        choices=['CSV', 'TEXT', 'BINARY'],
                        default='CSV',
                        help='table output format')
    group3 = parser.add_argument_group('Connection options')
    group3.add_argument("--host",
                        type=str,
                        default='localhost',
                        help='database server host (default: "localhost")')
    group3.add_argument("--port", "-p",
                        type=int,
                        default=5432,
                        help='database server port (default: "5432")')
    group3.add_argument("--username", "-U",
                        type=str,
                        default=os.getenv('USER'),
                        help='database user name (default: "{}")'.format(os.getenv('USER')))
    group3.add_argument("--password", "-W",
                        type=str,
                        help='force password prompt (should happen automatically, default: $PGPASSWORD)')
    return(parser)


def check_password(args):
    if args.password == None:
        args.password = os.getenv('PGPASSWORD')
        if args.password == None:
            args.password = getpass('password for database: ')


def check_directory(args):
    if args.directory == None:
        new_dir = os.path.realpath(
            './{}-{}'.format(os.path.basename(__file__), datetime.datetime.now().strftime('%Y%m%d%H%M%S')))
    else:
        new_dir = os.path.realpath(args.directory)
    args.directory = new_dir


def build_connect_str(args):
    return "export PGPASSWORD='{password}';psql -h {host} -p {port} -d {dbname} -U {user}".format(
        host=args.host, port=args.port, dbname=args.dbname, user=args.username, password=args.password)


def get_column_names(table_name):
    global connect_str
    com = connect_str + \
        " -t -c \"SELECT column_name FROM information_schema.columns col WHERE col.table_name='{table}' and numeric_precision NOTNULL;\"".format(
            table=table_name)
    try:
        res = subprocess.run(com, shell=True, stdout=PIPE, stderr=PIPE)
        cols = [col for col in res.stdout.decode().strip().split('\n')]
        print(cols)
    except:
        pass
    # Omit complexed query, just parse 'indexdef'
    com = connect_str + \
        " -t -c \"SELECT indexdef FROM pg_indexes WHERE tablename='{table}';\"".format(
            table=table_name)
    try:
        res = subprocess.run(com, shell=True, stdout=PIPE, stderr=PIPE)
        indexdef = res.stdout.decode().strip()
        colre = re.compile(r'[^\(]+\(([^\)]+)\)')
        m = colre.search(indexdef)
        indexed_cols = m.groups()[0].split(',')
    except:
        pass

    if cols == None:
        print('This program requires at least one numeric column to split dump files.')
        print('And ideally, the column should be indexed for better performance.')
        parser.print_help()
        sys.exit(1)

    return cols, indexed_cols


def select_split_col(cols, indexed_cols):
    questions = [
        inquirer.List('columns',
                      message='Which columns do you use to split dump files? Indexed columns are {}'.format(
                          indexed_cols),
                      choices=cols,
                      default=indexed_cols,
                      ),
    ]
    res = inquirer.prompt(questions)
    return res['columns']


def get_average_rec_size(table_name):
    global connect_str
    com = connect_str + \
        " -t -c \"SELECT AVG(length) FROM (SELECT length({table}::text) FROM {table} LIMIT 100) AS length;\"".format(
            table=table_name)
    res = subprocess.run(com, shell=True, stdout=PIPE, stderr=PIPE)
    return float(res.stdout.decode().strip()) * 1.45


def get_min_max_val(table_name, col_name):
    global connect_str
    com = connect_str + \
        " -t -c \"SELECT MIN({col}), MAX({col}) from {table};\"".format(
            col=col_name, table=table_name)
    res = subprocess.run(com, shell=True, stdout=PIPE, stderr=PIPE)
    minmax = [v.strip() for v in res.stdout.decode().split('|')]
    return int(minmax[0]), int(minmax[1])


def make_dir(new_dir):
    try:
        os.makedirs(new_dir)
    except FileExistsError:
        print(
            '\nAre you sure to dump to the directory, \'{}\'? [Y/n]'.format(new_path))
        while True:
            c = sys.stdin.read(1)
            if c == "Y" or c == "y" or ord(c) == 10:
                os.makedirs(path)
                return path
            elif c == "N" or c == "n":
                print("\n" + "Stopped processing." + "\n")
                sys.exit(1)
            else:
                print("\n" + "Please input Y or N.")


def do_copy(amnt_procs, table_name, col_name, directory, format, first_num, last_num):
    global connect_str
    com = connect_str + \
        " -t -c \"\COPY (SELECT * FROM {table} WHERE {col} >= {first} and {col} <= {last}) TO {directory}/dump-{amnt_procs:05d}.{ext} WITH {format};\"".format(
            table=table_name, col=col_name, first=first_num, last=last_num, directory=os.path.normpath(directory), ext=format.lower(), format=format, amnt_procs=amnt_procs)
    subprocess.Popen(com, shell=True, stdout=PIPE, stderr=PIPE)


def count_psql():
    count = 0
    for p in psutil.process_iter():
        if p.name() == 'psql':
            count = count + 1
    return count


def main():
    global connect_str
    check_psql()
    parser = set_args()
    args = parser.parse_args()
    check_password(args)
    check_directory(args)
    connect_str = build_connect_str(args)
    cols, indexed_cols = get_column_names(args.table)
    col_name = select_split_col(cols, indexed_cols)
    avg_rec_size = get_average_rec_size(args.table)
    num_rec_per_proc = math.floor(args.size * 1024 * 1024 / avg_rec_size)
    min, max = get_min_max_val(args.table, col_name)
    make_dir(args.directory)

    # main procedure
    print('{}: started.'.format(datetime.datetime.now()))
    first_num = min
    last_num = first_num + num_rec_per_proc - 1
    amnt_procs = 1
    while first_num < max:
        if count_psql() < args.count:
            do_copy(amnt_procs, args.table, col_name, args.directory,
                    args.format, first_num, last_num)
            first_num = last_num + 1
            last_num = first_num + num_rec_per_proc - 1
            amnt_procs = amnt_procs + 1
        time.sleep(0.1)
    print('{}: finished.'.format(datetime.datetime.now()))


if __name__ == "__main__":
    main()
