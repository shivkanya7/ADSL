import threading
import MySQLdb
import os
import sys

FIRST_TABLE_NAME = 'Maths'
SECOND_TABLE_NAME = 'Science'

JOIN_COLUMN_NAME_FIRST_TABLE = 'Marks1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'Marks2'



def individualjoin (thread_name, min, max, openconnection, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn):
    cursor = openconnection.cursor()
    if thread_name == "thread1":
        cursor.execute("CREATE TABLE %s AS SELECT * FROM %s A, %s B WHERE (A.%s=B.%s) AND A.%s >= %s AND A.%s <= %s" % (thread_name,InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn,Table1JoinColumn,str(min),Table1JoinColumn,str(max)))
    else:
        cursor.execute("CREATE TABLE %s AS SELECT * FROM %s A, %s B WHERE (A.%s=B.%s) AND A.%s > %s AND A.%s <= %s" % (thread_name,InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn,Table1JoinColumn,str(min),Table1JoinColumn,str(max)))

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    c = openconnection.cursor()
    maxi, mini = getminandmax(Table1JoinColumn, InputTable1, openconnection)
    interval = float(maxi - mini)/5.0
    if maxi == mini:
        c.execute("CREATE TABLE %s AS SELECT * FROM %s A, %s B WHERE (A.%s=B.%s) AND A.%s = %s" % (OutputTable,InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn,Table1JoinColumn,str(min_val)))
        return
    threads = ["thread1","thread2","thread3","thread4","thread5"]
    temp_threads = []
    for i in range(5):
        temp_threads.append(threading.Thread(target = individualjoin, args = (threads[i], mini, mini+interval, openconnection, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn)))
        temp_threads[i].start()
        mini += interval
    for t in temp_threads:
        t.join()
    c.execute("CREATE TABLE %s AS SELECT * FROM thread1 WHERE 1=2" % (OutputTable))
    for t_name in threads:
        c.execute("INSERT INTO %s SELECT * FROM %s" % (OutputTable,t_name))
    c.execute("INSERT INTO %s SELECT * FROM %s A INNER JOIN %s B ON (A.%s=B.%s)  WHERE A.%s=%s" %(OutputTable,InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn,Table1JoinColumn,str(maxi)))
    openconnection.commit()    
    deleteTables('thread1', con)
    deleteTables('thread2', con)
    deleteTables('thread3', con)
    deleteTables('thread4', con)
    deleteTables('thread5', con)


def getminandmax(SortingColumnName, InputTable, openconnection):
    c = openconnection.cursor()
    maximum, minimum = 0, 0
    c.execute("SELECT max("+str(SortingColumnName)+") FROM "+str(InputTable))
    maximum = c.fetchone()[0]
    c.execute("SELECT min("+str(SortingColumnName)+") FROM "+str(InputTable))
    minimum = c.fetchone()[0]
    return maximum, minimum

def getOpenConnection():
    host="localhost"
    user="root"
    password="B$hiv2001"
    database="assignment6"
    return MySQLdb.connect(host,user,password,database)

def deleteTables(ratingstablename, openconnection):
    cursor = openconnection.cursor()
    if ratingstablename.upper() == 'ALL':
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = cursor.fetchall()
        for table_name in tables:
            cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
    else:
        cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
    openconnection.commit()
    
  
def saveTable(ratingstablename, fileName, openconnection):
    cursor = openconnection.cursor()
    cursor.execute("Select * from %s" %(ratingstablename))
    data = cursor.fetchall()
    openFile = open(fileName, "w")
    for row in data:
        for d in row:
            openFile.write('d'+",")
        openFile.write('\n')
    openFile.close()
    

if __name__ == '__main__':
    try:
        print("Creating Database na as ddsassignment32")
        print("Getting connection from the ddsassignment32 database")
        con = getOpenConnection()
        print("Performing Parallel Join")
        ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
        saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);
        if con:
            con.close()

    except Exception as detail:
        print("Something bad has happened!!! This is the error ==> ", detail)
