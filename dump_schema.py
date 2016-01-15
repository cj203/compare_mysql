# -*- coding: utf-8 -*-
__author__="carlosjose203@gmail.com"
__date__ ="$03-ago-2015 10:57:20$"

import sys
import os
import ConfigParser
import MySQLdb.cursors
from datetime import datetime

config = ConfigParser.RawConfigParser()
config.read('/var/config/django_conf.cfg')


class SchemaDump():
    def __init__(self):
        self.select_table = """SELECT TABLE_NAME as TN 
                                FROM information_schema.TABLES
                                where TABLE_SCHEMA = '%s'"""
        self.select_constraint = """SELECT CONSTRAINT_NAME, TABLE_NAME as TN, 
                                    COLUMN_NAME as CN, REFERENCED_TABLE_NAME,
                                    REFERENCED_COLUMN_NAME  
                                    FROM information_schema.KEY_COLUMN_USAGE 
                                    where TABLE_SCHEMA = '%s' 
                                    and CONSTRAINT_NAME != 'PRIMARY'
                                    and REFERENCED_COLUMN_NAME is not null"""
        self.t_foreign = """ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) 
                            REFERENCES %s(%s) 
                            on delete no action 
                            on update no action; \n"""
        self.create_table = "SHOW CREATE TABLE %s"

    def production(self, name="testing"):
        self.ignore = "--ignore-table={db}.message \
                       --ignore-table={db}.emarketing_send \
                       --ignore-table={db}.emarketing_link".format(db=name)
        self._db = name
        self._host = str(config.get('mysql_production', 'hostdb'))
        self._port = config.get('mysql_production', 'port')
        self._user = str(config.get('mysql_production', 'userdb'))
        self._passwd = str(config.get('mysql_production', 'passw'))
        self.dbsql = self.sql()
        self.cursor = self.dbsql.cursor()
    
    def lab(self):
        self.ignore = ''
        self._db = str(config.get('mysql', 'db'))
        self._host = str(config.get('mysql', 'hostdb'))
        self._port = config.get('mysql', 'port')
        self._user = str(config.get('mysql', 'userdb'))
        self._passwd = str(config.get('mysql', 'passw'))
        self.dbsql = self.sql()
        self.cursor = self.dbsql.cursor()
        

    def sql(self):
        self.file_name ='/home/carlos/dumps/'+ self._db + '_' + \
                        str(datetime.now())[:-7].replace(' ', '_') + '.sql'
        f = open(self.file_name, "aw")
        f.write("CREATE DATABASE  IF NOT EXISTS %s \
                /*!40100 DEFAULT CHARACTER SET utf8 COLLATE utf8_bin */;\n" % self._db)
        f.write("use %s;\n" % self._db)
        f.close()

        self.data = "mysqldump -t -e %s --routines -h %s -P %s -u %s \
                    -p%s -t %s >> %s" % (self.ignore,self._host, self._port, self._user,
                                        self._passwd, self._db, self.file_name)
        dbsql=MySQLdb.connect(host=self._host,user=self._user,port=int(self._port), 
                              passwd=self._passwd, db=self._db, use_unicode = True,
                              cursorclass=MySQLdb.cursors.DictCursor, charset = "utf8")
        return dbsql

    def dump_data(self):
        os.system(self.data)
        print '{:#^100}'.format(' DATA DONE ')

    def dumps_constraint(self):
        self.cursor.execute(self.select_constraint % self._db)
        f = open(self.file_name, "aw")
        for t in self.cursor.fetchall():
            f.write(self.t_foreign % (t['TN'], t['CONSTRAINT_NAME'], t['CN'],
                                        t['REFERENCED_TABLE_NAME'], 
                                        t['REFERENCED_COLUMN_NAME']))
        f.close()
        print '{:#^100}'.format(' FOREIGN KEY ')

    def dump_tables(self):
        self.cursor.execute(self.select_table % self._db)
        f = open(self.file_name, "aw")
        for t in self.cursor.fetchall():
            self.cursor.execute(self.create_table % t['TN'])
            __t = self.cursor.fetchone()
            __a = "".join([a for a in __t['Create Table']\
                    .encode("utf8").split('\n') if 'CONSTRAINT' not in a])\
                    .replace(',)', ')') + ';\n\n'
            f.write("DROP TABLE IF EXISTS %s;\n" % __t['Table'])
            f.write(__a)
        f.close()
        print '{:#^100}'.format(' TABLES DONE ')


if __name__ == '__main__':
    sd = SchemaDump()
    if len(sys.argv) > 1:
        sd.production(sys.argv[1])
    else:
        sd.lab()
    sd.dump_tables()
    sd.dump_data()
    sd.dumps_constraint()
