import hashlib
import json
import time
import subprocess
import os
import sys
import cx_Oracle

from concurrent.futures import ProcessPoolExecutor
from configparser import ConfigParser
from cx_Oracle import SessionPool
from subprocess import PIPE
from datetime import datetime


class MyClass:
    def __init__(self, spool):
        self.spool = spool

    def get_tables(self):
        conn = self.spool.acquire()
        cur = conn.cursor()
        cur.execute("""
            select owner, table_name
                    from dba_tables
                    where owner in 
                      (
                      SELECT DISTINCT t.username 
                      FROM DBA_USERS T 
                      WHERE TO_CHAR(T.CREATED, 'YYYYMMDD')> 
                        (SELECT TO_CHAR(CREATED, 'YYYYMMDD')FROM DBA_USERS U WHERE U.USERNAME = 'SYS') 
                      AND t.username NOT IN ('DBMONITOR','DSG','EXPORTMAN','XDB')
                      )
               minus
               select distinct owner,table_name from dba_tab_columns where data_type  like '%LOB%'
               order by owner, table_name
        """)
        table_owners = []
        for owner, table_name in cur:
            table_owners.append((table_name, owner))
        cur.close()
        self.spool.release(conn)
        return table_owners

    def get_exp(self, table_owner):
        table_name, owner = table_owner
        cfg = ConfigParser()
        cfg.read('config.ini')
        cusername = cfg.get('server', 'username')
        cpassword = cfg.get('server', 'password')
        cdatabase = cfg.get('server', 'database')
        databases = cdatabase.split(',')

        conn = self.spool.acquire()
        cur = conn.cursor()
        cur.execute("""
                            select column_name
                            from dba_tab_columns
                            where owner=:owner and table_name=:table_name
                            order by column_id
                        """,
                    owner=owner,
                    table_name=table_name)
        mylist = []
        for column_name, in cur:
            mylist.append(column_name)
        if len(mylist) > 3:
            mylist = mylist[:len(mylist) // 2 + 2]
        query = " query=\\\"order by " + ", ".join(mylist) + "\\\""
        # exp = "exp " + cusername + "/" + cpassword + "@" + address + '/' + databases[0] + " file=" \
        #       + os.path.dirname(os.path.realpath(__file__))  \
        #       + "/" + owner + "." + table_name + ".dmp tables=" + owner + "." + table_name + query
        exp = "exp " + cusername + "/" + cpassword + "@" + databases[0] + " file=" \
              + os.path.dirname(os.path.realpath(__file__)) \
              + "/" + owner + "." + table_name + ".dmp tables=" + owner + "." + table_name + query
        cur.close()
        self.spool.release(conn)
        return exp

    def get_exps(self):
        mytable_owners = self.get_tables()
        exps = []
        for i in mytable_owners:
            exp = self.get_exp(i)
            exps.append(exp)
        return exps

    def gen_lob_file(self, owner, table_name):
        conn = self.spool.acquire()
        cur = conn.cursor()

        sql = "select * from " + owner + "." + table_name

        cur.execute("""
                            select column_name, data_type
                            from dba_tab_columns
                            where owner=:owner and table_name=:table_name
                            order by column_id
                        """,
                    owner=owner,
                    table_name=table_name)
        mylist = []
        for column_name, data_type in cur:
            if data_type != 'BLOB' and data_type != 'CLOB':
                mylist.append(column_name)
        if len(mylist) > 3:
            mylist = mylist[:len(mylist) // 2 + 2]
        sql += ' order by ' + ', '.join(mylist)

        cur.execute(sql)
        datas = cur.fetchall()
        str_data = ""
        byte_data = b''
        with open(owner + "." + table_name + ".dmp", 'w') as f:
            f.write('')
        if len(datas) > 0:
            for data in datas:
                for i in range(len(data)):
                    if data[i] is not None:
                        str_data = ""
                        byte_data = b''
                        if type(data[i]) == int:
                            str_data = str(data[i])
                        if type(data[i]) == float:
                            str_data = str(data[i])
                        if type(data[i]) == str:
                            str_data = data[i]
                        if type(data[i]) == datetime:
                            str_data = str(data[i])
                        if type(data[i]) == cx_Oracle.LOB:
                            byte_data = data[i].read()
                        if str_data != '':
                            with open(owner + "." + table_name + ".dmp", 'ab') as f:
                                f.write(str_data.encode('gbk'))
                        if byte_data != b'':
                            with open(owner + "." + table_name + ".dmp", 'ab') as f:
                                f.write(byte_data)
        cur.close

    def get_lob_values(self):
        conn = self.spool.acquire()
        cur = conn.cursor()
        cur.execute("""
            select owner, table_name from dba_tab_columns where data_type like '%LOB%'
              and owner in 
                (
                SELECT DISTINCT t.username 
                FROM DBA_USERS T 
                WHERE TO_CHAR(T.CREATED, 'YYYYMMDD')> 
                  (SELECT TO_CHAR(CREATED, 'YYYYMMDD')FROM DBA_USERS U WHERE U.USERNAME = 'SYS') 
                AND t.username NOT IN ('DBMONITOR','DSG','EXPORTMAN','XDB')
                )
              and table_name in
                (
                select table_name from dba_tables
                )
        """)

        lob_results = {}

        for owner, table_name in cur:
            sha1 = hashlib.sha1()
            self.gen_lob_file(owner, table_name)
            with open(owner + "." + table_name + ".dmp", 'rb') as f:
                while True:
                    data = f.read(10240)
                    if not data:
                        break
                    sha1.update(data)
                lob_results[owner + "." + table_name] = sha1.hexdigest()
            os.remove(owner + "." + table_name + ".dmp")
        return lob_results


def gen_file_sha1(file, position):
    sha1 = hashlib.sha1()
    file.seek(position)
    while True:
        data = file.read(10240)
        if not data:
            break
        sha1.update(data)
    return sha1.hexdigest()


def exec_exp(exp):
    fd = open('error.log', 'ab')
    stable = None
    sha1 = None
    with subprocess.Popen(exp, stdout=PIPE, stderr=PIPE, shell=True, close_fds=True) as proc:
        stable = exp[exp.find('tables')+7:exp.find('query')-1]
        if proc.stderr.read().rfind(b'successfully') < 0:
            fd.write(proc.stderr.read())
            fd.write(proc.stdout.read())
            return stable, sha1
        filename = exp[exp.find('file')+5:exp.find('tables')-1]
        while proc.returncode is None:
            proc.poll()
        if proc.returncode is not None:
            with open(filename, 'rb') as f:
                temp = f.read(800)
                position = temp.find(filename.encode('gbk'))
                sha1 = gen_file_sha1(f, position)
    fd.close()
    os.remove(filename)
    return stable, sha1


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: generate.py xxx.json')
        raise SystemExit(1)
    if not sys.argv[1].endswith('.json'):
        print('Error: file type must be json')
        raise SystemExit(1)

    print('Running.......')
    start = time.time()

    cfg = ConfigParser()
    cfg.read('config.ini')
    user = cfg.get('server', 'username')
    password = cfg.get('server', 'password')
    database = cfg.get('server', 'database')
    databases = database.split(',')

    spool = SessionPool(user=user, password=password, dsn=databases[0], min=1, max=5,
                        increment=1)
    cls = MyClass(spool)
    lob_values = cls.get_lob_values()
    results = cls.get_exps()

    values = {}
    with ProcessPoolExecutor(8) as pool:
        for my_stable, my_sha1 in pool.map(exec_exp, results):
            values[my_stable] = my_sha1

    values.update(lob_values)
    with open(sys.argv[1], 'w') as f:
        f.write(json.dumps(values, indent=4))

    print(time.time() - start)
