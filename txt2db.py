import csv
import sqlite3
import sys
from argparse import ArgumentParser
from tqdm import tqdm


parser = ArgumentParser(description='Parses txt leak file and stores in an SQLite DB.')
parser.add_argument('input', help='Input txt file')
parser.add_argument('db', help='Database file')
parser.add_argument('--purge', help='Purge the database', action='store_true')
args = parser.parse_args()

SQL_INSERT = 'INSERT INTO raw VALUES (?, ?, ?, ?, ?, ?)'
SQL_PURGE = 'DELETE FROM raw'

db = sqlite3.connect(args.db)
cur = db.cursor()

if args.purge:
    cur.execute(SQL_PURGE)


with open(args.input, newline='') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=';')
    next(csv_reader)
    rows_cut = ((r[0], r[1].upper(), r[2][:10], r[3], r[4], r[5][:10]) for r in csv_reader)
    cur.executemany(SQL_INSERT, tqdm(rows_cut, file=sys.stdout))

cur.close()
db.commit()
