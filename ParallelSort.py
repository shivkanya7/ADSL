import threading
#import mysql.connector as MySQLdb
import MySQLdb
import os
import sys

FIRST_TABLE_NAME = 'table1'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'


def individualsort (thread_name, min, max, openconnection, InputTable, SortingColumnName):
    c = openconnection.cursor()
    if thread_name=="thread1":
    	c.execute("CREATE TABLE %s AS SELECT * FROM %s WHERE %s >= %s AND %s <= %s ORDER BY %s" % (thread_name,InputTable,SortingColumnName,str(min),SortingColumnName,str(max),SortingColumnName)) 
    else:
    	c.execute("CREATE TABLE %s AS SELECT * FROM %s WHERE %s > %s AND %s <= %s ORDER BY %s" % (thread_name,InputTable,SortingColumnName,str(min),SortingColumnName,str(max),SortingColumnName))    


def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    c = openconnection.cursor()
    print(OutputTable)
    maxi, mini = getminandmax(SortingColumnName, InputTable, openconnection)
    interval = float(maxi - mini)/5.0
    if maxi == mini:
        c.execute("CREATE TABLE %s AS SELECT * FROM %s" % (OutputTable,InputTable))
        return
    threads = ["thread1","thread2","thread3","thread4","thread5"]
    temp_threads = []

    for i in range(5):
        temp_threads.append(threading.Thread(target = individualsort, args = (threads[i], mini, mini+interval, openconnection, InputTable, SortingColumnName)))
        temp_threads[i].start()
        mini += interval
   
    for thread in temp_threads:
        thread.join()
    c.execute("CREATE TABLE %s AS SELECT * FROM %s WHERE 1=2" % (OutputTable,InputTable))

    
    for t_name in threads:
        c.execute("INSERT INTO %s SELECT * FROM %s" % (OutputTable,t_name))
    c.execute("INSERT INTO %s SELECT * FROM %s WHERE %s=%s" % (OutputTable,InputTable,SortingColumnName,str(maxi)))
    openconnection.commit()
    deleteTables('thread1', con);
    deleteTables('thread2', con);
    deleteTables('thread3', con);
    deleteTables('thread4', con);
    deleteTables('thread5', con);


def individualjoin (thread_name, min, max, openconnection, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn):
    cursor = openconnection.cursor()
    if thread_name == "thread1":
        cursor.execute("CREATE TABLE %s AS SELECT * FROM %s A, %s B WHERE (A.%s=B.%s) AND A.%s >= %s AND A.%s <= %s" % (thread_name,InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn,Table1JoinColumn,str(min),Table1JoinColumn,str(max)))
    else:
        cursor.execute("CREATE TABLE %s AS SELECT * FROM %s A, %s B WHERE (A.%s=B.%s) AND A.%s > %s AND A.%s <= %s" % (thread_name,InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn,Table1JoinColumn,str(min),Table1JoinColumn,str(max)))
  


def getminandmax(SortingColumnName, InputTable, openconnection):
    c = openconnection.cursor()
    maximum, minimum = 0, 0
    c.execute("SELECT max("+str(SortingColumnName)+") FROM "+str(InputTable))
    maximum = c.fetchone()[0]
    c.execute("SELECT min("+str(SortingColumnName)+") FROM "+str(InputTable))
    minimum = c.fetchone()[0]
    return maximum, minimum


def getOpenConnection():
    
    return MySQLdb.connect(host="localhost", user="root", password="B$hiv2001", database="assignment6")


def deleteTables(ratingstablename, openconnection):
    cursor = openconnection.cursor()
    if ratingstablename.upper() == 'ALL':
        print("dsaf")
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
        print("Getting connection from the ddsassignment3 database")
        con = getOpenConnection();
        print("Performing Parallel Sort")
        ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);
        saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
        if con:
            con.close()

    except Exception as detail:
        print("Something bad has happened!!! This is the error ==> ", detail)
