# postgres-elastic
INFO:: Provides facility to perform multiple operation between Postgres (PG) and Elasticsearch (ES)

Prerequisite:: You need to fill up config.json with your PG & ES details

HOW TO RUN:: Select mode you want to perform operation as mentioned below

         Uses                                    Commands
-To read records from PG                        : python execute.py mode-- read_table

-To sync records from PG to ES                  : python execute.py mode-- insert_docs

-To delete Docs from ES index based on PG table : python execute.py mode-- delete_docs

-To insert into PG from ES index                : python execute.py mode-- write_table
