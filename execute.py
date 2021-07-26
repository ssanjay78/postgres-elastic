import sys
import json
import argparse
from pg_operations import PG_Operate
from es_operations import ES_Operate


parser = argparse.ArgumentParser()
parser.add_argument('--mode',
                    choices=['read_table', 'delete_docs', 'insert_docs', 'write_table'],
                    required=False,
                    help='Special testing value')

args = parser.parse_args()
if args.mode not in ('read_table', 'delete_docs', 'insert_docs', 'write_table'):
    print(f"Mention at least one task\n {parser.print_help()}")
    sys.exit(1)

def execute():
    with open("config.json") as f:
        conf = json.load(f)

    pg_obj = PG_Operate(conf['pg_params'])
    es_obj = ES_Operate(conf['es_params'])

    if args.mode == 'read_table':            #'''read from PG table'''
        records, columns, count = pg_obj.fetch_records(f"SELECT * FROM {conf['pg_params']['TABLE']};")
        # print(records, columns, count)
    elif args.mode == 'delete_docs':         #'''Delete Docs from ES based on PG table'''
        result = es_obj.delete_docs()
        print(result)
    elif args.mode == 'write_table':         #'''Write to PG for docs from ES Index'''
        docs = es_obj.read_docs()
        pg_obj.write_from_es(docs, conf['pg_params']['TABLE'])
    elif args.mode == 'insert_docs':
        records, columns, count = pg_obj.fetch_records(f"SELECT * FROM {conf['pg_params']['TABLE']};")
        es_obj.insert_docs(records, columns, count)

    # Terminate session with PG, ES
    pg_obj.terminate()
    es_obj.terminate()

execute()