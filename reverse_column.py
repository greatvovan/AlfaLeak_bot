import sqlite3
from argparse import ArgumentParser


parser = ArgumentParser(description='Reverse column content')
parser.add_argument('db', help='Database file')
parser.add_argument('table', help='Table name')
parser.add_argument('src_col', help='Source column name')
parser.add_argument('dst_col', help='Destination column name')
args = parser.parse_args()

db = sqlite3.connect(args.db)
db.create_function("strrev", 1, lambda s: s[::-1])
cur = db.cursor()

sql = f'UPDATE {args.table} SET {args.dst_col} = strrev({args.src_col})'
print('Running', sql)
cur.execute(sql)

cur.close()
db.commit()
