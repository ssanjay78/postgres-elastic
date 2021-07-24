from elasticsearch import Elasticsearch


class ES_Operate:
    def __init__(self, conf):
        host = conf["HOST"]
        port = conf["PORT"]
        user = conf["USER"]
        password = conf["PASSWORD"]
        self.es_conn = Elasticsearch(hosts=[host], port=port,
                                     http_auth=(user, password))
        
        self.index = conf["INDEX"]

    def terminate(self):
        self.es_conn.close()

    def read_docs(self):
        # docs = self.es_conn.get(index=self.index, id='Soni')
        self.es_conn.indices.put_settings(index=self.index,
                        body= {"index" : {
                                "max_result_window" : 1000000
                              }})
        seacrh_query = {
            "size": 1000000,
            "query": {"match_all":{}}
        }
        docs = self.es_conn.search(index=self.index, body=seacrh_query)
        return docs['hits']['hits']

    def delete_doc(self):
        count = 0
        del_query = {
            "query": {
                "match": {"dob":'null'}
            }
        }
        result = self.es_conn.delete_by_query(index=self.index, body=del_query)

        # for i in range(len(doc)):
        #     res = self.es_conn.delete(index=self.index, id=doc[i]['name'])
            # count += bool(res['result']=='deleted')
        print(result)