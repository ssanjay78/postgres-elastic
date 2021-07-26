import psycopg2


class PG_Operate:
    def __init__(self, conf):
        host = conf["HOST"]
        port = conf["PORT"]
        user = conf["USER"]
        password = conf["PASSWORD"]
        database = conf["DATABASE"]
        self.pg_conn = psycopg2.connect(dbname=database, host=host, 
                                        port=port, user=user,
                                        password=password)
        self.pg_conn.autocommit = True 

    def terminate(self):
        self.pg_conn.close()

    def fetch_records(self, query):
        try:
            cur = self.pg_conn.cursor()
            cur.execute(query)
            columns = [c.name for c in cur.description]
            count = cur.rowcount
            records = []
            while True:
                record = cur.fetchmany(size=1)
                # TODO: enhance code for size 1000
                records.append(record)
                if not record:
                    print("DEBUG :: End of fetching records at PG, Total count :",count)
                    return records, columns, count
        except Exception as e:
            print("Error while executing DB query = {}, \nError :: {}".format(query, str(e)))
            raise

    def write_from_es(self, docs, table):
        cur = self.pg_conn.cursor()
        columns = list(docs[0]['_source'].keys())
        count = temp = 0
        try:
            for doc in range(len(docs)):
                values = tuple(docs[doc]['_source'].values())
                cur.execute(f'INSERT INTO {table} ({",".join(columns)}) VALUES ({"%s,"*(len(columns)-1)} %s);', values)
                temp += 1
                if temp-count>=10:
                    count=temp
                    print("DEBUG :: Inserted records count :", count)
            print("RESULT :: Total inserted records in PG :",temp)
        except Exception as e:
            print(f"Error while inserting record \nError :: {str(e)}")   
            raise     