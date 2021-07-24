import json
from pg_operations import PG_Operate
from es_operations import ES_Operate


def execute():
    with open("config.json") as f:
        conf = json.load(f)

    pg_obj = PG_Operate(conf['pg_params'])
    es_obj = ES_Operate(conf['es_params'])

    '''Fetch from PG'''
    records, columns, count = pg_obj.fetch_records('SELECT * FROM pd_activity_copy;')
    # print(records, columns, count)

    ''' ES Operations '''
    # es_obj.delete_doc()
    docs = es_obj.read_docs()
    pg_obj.write_from_es(docs, 'pd_activity_copy')

    # Terminate session with PG, ES
    pg_obj.terminate()
    es_obj.terminate()

execute()