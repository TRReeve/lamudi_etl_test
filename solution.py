import json
import psycopg2
from psycopg2 import sql

dbname = 'lamudi_bi'
username = 'etl'

def get_connection():

    conn = psycopg2.connect(dbname=dbname,user=username)
    return conn
    
def get_data(filename):

    print("loading from {0}".format(filename))

    #load data from file
    data = []
    with open(filename,'r') as r:
        jobj = json.load(r)
        for record in jobj:
            if record["table"] == "record_insert" and record["event"] == "insert":
                data.append(record)

    return data

def dump_file(data,outfilename):

    with open(outfilename,'w') as write:
        json.dump(data,write)

def create_table(table_name):
    print("creating_table {0}".format(table_name))

    with open('create_record_table.sql','r') as sql:
        query = sql.read()

        conn = get_connection()
        cur = conn.cursor()

        cur.execute(query.format(table_name))
        query.format(table_name)
        conn.commit()
        conn.close()

def insert_json_to_table(jsondata):

    #insert country_iso to insert_object
    for jrow in jsondata:
        jrow["record"]["country_iso"] = jrow["country"]
        table = jrow["table"]
        
        #match all columns to corresponding rows to dynamically construct query going to right columns even where eid/sid are sometimes missing
        columns = jrow["record"].keys()
        values = jrow["record"].values()
        primary_key_name = "recordinsert_pkey"

        #excute query
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ON CONSTRAINT {} DO NOTHING").format(
                sql.Identifier(table),
                sql.SQL(', ').join(map(sql.Identifier, columns)), ##mapping columns and values into a query, now can be used for any table model you want (e.g if you have a stream composed of different tables)
                sql.SQL(', ').join(map(sql.Literal, values)),
                sql.Identifier(primary_key_name)))
        #close connection
        conn.commit()
        conn.close()
    print("insertion complete")

def run_sku_analysis(country_iso):

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql.SQL("""Select country_iso,sku,sum(views) as views
                           FROM record_insert WHERE country_iso = {0} Group By 1,2 order by views desc""").format(sql.Literal(country_iso)))
    #get single row to check query works
    results = cur.fetchone()
    return results

if __name__ == '__main__':

    jsondata = get_data('data.json')
    new_file = dump_file(jsondata,'record_data.json')
    create_table('record_insert')
    insert_json_to_table(jsondata)
    results = run_sku_analysis('mx')
    print(results)
    
    
    

      





