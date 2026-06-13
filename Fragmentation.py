#!/usr/bin/python3
#
# Interface for the assignement
#

import psycopg2

#constants
METADATA_TABLE = 'partitioning_metadata'
RANGE_PREFIX = 'range_part'
RROBIN_PREFIX = 'rrobin_part'

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()

    cur.execute("""
        DROP TABLE IF EXISTS {0} CASCADE;
        CREATE TABLE {0} (
                userid INT,
                movieid INT,
                rating FLOAT
        );
    """.format(ratingstablename))

    with open(ratingsfilepath, 'r') as f:
        rows = []
        for line in f:
            parts = line.strip().split('::')
            if len(parts) < 3:
                continue
            userid = int(parts[0])
            movieid = int(parts[1])
            rating = float(parts[2])
            rows.append((userid, movieid, rating))

    cur.executemany(
        "INSERT INTO {0} (userid, movieid, rating) VALUES (%s, %s, %s)".format(ratingstablename), rows
    )

    openconnection.commit()
    cur.close()

#metadata table helpers
def _ensure_metadata_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS {0} (
            partition_type VARCHAR(10),
            num_partitions INT
        );
    """.format(METADATA_TABLE))
 
 
def _set_metadata(cur, partition_type, num_partitions):
    cur.execute(
        "DELETE FROM {0} WHERE partition_type = %s".format(METADATA_TABLE),
        (partition_type,)
    )
    cur.execute(
        "INSERT INTO {0} (partition_type, num_partitions) VALUES (%s, %s)".format(METADATA_TABLE),
        (partition_type, num_partitions)
    )
 
 
def _get_metadata(cur, partition_type):
    cur.execute(
        "SELECT num_partitions FROM {0} WHERE partition_type = %s".format(METADATA_TABLE),
        (partition_type,)
    )
    row = cur.fetchone()
    if row is None:
        raise Exception("No metadata found for partition type: " + partition_type)
    return row[0]

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    _ensure_metadata_table(cur)
    _set_metadata(cur, 'range', numberofpartitions)
 
    N = numberofpartitions
    range_size = 5.0 / N
 
    for i in range(N):
        partition_name = RANGE_PREFIX + str(i)
 
        #Drop and recreate each partition table
        cur.execute("DROP TABLE IF EXISTS {0} CASCADE;".format(partition_name))
        cur.execute("""
            CREATE TABLE {0} (
                userid  INT,
                movieid INT,
                rating  FLOAT
            );
        """.format(partition_name))
 
        #Partition 0: [0, range_size]
        #Partition i: (i*range_size, (i+1)*range_size]
        low  = i * range_size
        high = (i + 1) * range_size
 
        if i == 0:
            cur.execute("""
                INSERT INTO {0} (userid, movieid, rating)
                SELECT userid, movieid, rating
                FROM {1}
                WHERE rating >= %s AND rating <= %s;
            """.format(partition_name, ratingstablename), (low, high))
        else:
            cur.execute("""
                INSERT INTO {0} (userid, movieid, rating)
                SELECT userid, movieid, rating
                FROM {1}
                WHERE rating > %s AND rating <= %s;
            """.format(partition_name, ratingstablename), (low, high))
 
    openconnection.commit()
    cur.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    _ensure_metadata_table(cur)
    _set_metadata(cur, 'rrobin', numberofpartitions)
 
    N = numberofpartitions
 
    #Create all partition tables
    for i in range(N):
        partition_name = RROBIN_PREFIX + str(i)
        cur.execute("DROP TABLE IF EXISTS {0} CASCADE;".format(partition_name))
        cur.execute("""
            CREATE TABLE {0} (
                userid  INT,
                movieid INT,
                rating  FLOAT
            );
        """.format(partition_name))
 
    #Assign rows round-robin using (row_number()-1) % N
    for i in range(N):
        partition_name = RROBIN_PREFIX + str(i)
        cur.execute("""
            INSERT INTO {0} (userid, movieid, rating)
            SELECT userid, movieid, rating
            FROM (
                SELECT userid, movieid, rating,
                       (ROW_NUMBER() OVER () - 1) % {1} AS partition_idx
                FROM {2}
            ) AS subq
            WHERE partition_idx = {3};
        """.format(partition_name, N, ratingstablename, i))
 
    openconnection.commit()
    cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    _ensure_metadata_table(cur)
    N = _get_metadata(cur, 'rrobin')
 
    #Insert into main ratings table first
    cur.execute(
        "INSERT INTO {0} (userid, movieid, rating) VALUES (%s, %s, %s)".format(ratingstablename),
        (userid, itemid, rating)
    )
 
    #Count total rows across all partitions to determine next partition
    total_rows = 0
    for i in range(N):
        cur.execute("SELECT COUNT(*) FROM {0}".format(RROBIN_PREFIX + str(i)))
        total_rows += cur.fetchone()[0]
 
    next_partition = total_rows % N
    partition_name = RROBIN_PREFIX + str(next_partition)
 
    cur.execute(
        "INSERT INTO {0} (userid, movieid, rating) VALUES (%s, %s, %s)".format(partition_name),
        (userid, itemid, rating)
    )
 
    openconnection.commit()
    cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    _ensure_metadata_table(cur)
    N = _get_metadata(cur, 'range')
 
    #Insert into main ratings table first
    cur.execute(
        "INSERT INTO {0} (userid, movieid, rating) VALUES (%s, %s, %s)".format(ratingstablename),
        (userid, itemid, rating)
    )
 
    #Determine which partition rating belongs to
    range_size = 5.0 / N
 
    if rating == 0.0:
        partition_index = 0
    else:
        import math
        #Ratings above 0:
        partition_index = math.ceil(rating / range_size) - 1
        #Cap max value in case rating == 5.0
        partition_index = min(partition_index, N - 1)
 
    partition_name = RANGE_PREFIX + str(partition_index)
 
    cur.execute(
        "INSERT INTO {0} (userid, movieid, rating) VALUES (%s, %s, %s)".format(partition_name),
        (userid, itemid, rating)
    )
 
    openconnection.commit()
    cur.close()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()