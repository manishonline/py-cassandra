from cassandra.cluster import Cluster
import json

def connect():
    cluster = Cluster(['192.168.99.100'],port=9042)
    session = cluster.connect()

    #Check if Keyspace exists
    rows = session.execute("""SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='mykeyspace'""")

    if rows.current_rows[0].keyspace_name=='mykeyspace':
        print('mykeyspace exists. DROP KEYSPACE mykeyspace')
        session.execute("DROP KEYSPACE mykeyspace")

    print('Creating keyspace. CREATE KEYSPACE mykeyspace')

    session.execute("""
        CREATE KEYSPACE mykeyspace
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """)




if __name__ == '__main__':
    connect()   