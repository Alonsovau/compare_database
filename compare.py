import argparse
import csv
import json

parser = argparse.ArgumentParser(description='Compare Signature Value of Database')
parser.add_argument(dest='file_name1', metavar='xxx.json')
parser.add_argument(dest='file_name2', metavar='xxx.json')
args = parser.parse_args()

if not args.file_name1.endswith('.json'):
    print('error: file type must be json')
    raise SystemExit(1)
if not args.file_name2.endswith('.json'):
    print('error: file type must be json')
    raise SystemExit(1)

newdict = {}
olddict = {}
with open(args.file_name1) as f:
    newdict = json.loads(f.read())
with open(args.file_name2) as f:
    olddict = json.loads(f.read())

tempset = (olddict.items() | newdict.items()) - (newdict.items() & olddict.items())
results = set()
for temp in tempset:
    key, _ = temp
    results.add(key)

rows = []
for result in results:
    schema_table = result.split('.')
    rows.append((schema_table[0], schema_table[1]))
rows.sort()

headers = ['Schema', 'Table']
with open('DifferenetTable.csv', 'w', newline='') as f:
    f_csv = csv.writer(f)
    f_csv.writerow(headers)
    f_csv.writerows(rows)
