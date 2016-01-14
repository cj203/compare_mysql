__author__ = "carlosjose203@gmail.com"
__date__ = "$30-jun-2015 10:50:20$"

import sys
import ConfigParser
import MySQLdb.cursors

config = ConfigParser.RawConfigParser()
config.read('/var/config/conf.cfg')


class SchemaDiff():
    def __init__(self):
        self.select_table = """SELECT TABLE_NAME as TN FROM information_schema.TABLES 
                                where TABLE_SCHEMA = '%s'"""
        self.select_column = """SELECT TABLE_NAME as TN, COLUMN_NAME as CN, 
                                    COLUMN_TYPE as CT,
                                 COLUMN_DEFAULT as CD, IS_NULLABLE as ISN
                                 FROM information_schema.COLUMNS 
                                 where TABLE_SCHEMA = '%s'"""
        self.select_routines = """SELECT ROUTINE_NAME, LENGTH(ROUTINE_DEFINITION) as len 
                                    FROM information_schema.ROUTINES 
                                    where ROUTINE_SCHEMA like '%s'"""
        self.select_constraint = """SELECT CONSTRAINT_NAME, TABLE_NAME as TN, 
                                    COLUMN_NAME as CN,
                                    REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME  
                                    FROM information_schema.KEY_COLUMN_USAGE 
                                    where TABLE_SCHEMA = '%s' 
                                    and CONSTRAINT_NAME != 'PRIMARY'
                                    and REFERENCED_COLUMN_NAME is not null"""
        self.t_foreign = """ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) 
                            REFERENCES %s(%s) on delete no action on update no action;"""
        self.t_add_column = "ALTER TABLE %s ADD %s %s %s %s;"
        self.drop_column = "ALTER TABLE %s DROP COLUMN %s;"
        self.create_table = "SHOW CREATE TABLE %s"

    def production(self, name="testing"):
        """
            Here configure your production db or the target for compare
        """
        self._db2 = name
        self._host = str(config.get('mysql_production', 'hostdb'))
        self._port = config.get('mysql_production', 'port')
        self._user = str(config.get('mysql_production', 'userdb'))
        self._passwd = str(config.get('mysql_production', 'passw'))
        self.sql()
    
    def local(self):
        """
            Here configure your tests db, only for testing
        """
        self._db2 = 'tests'
        self._host = "localhost"
        self._port = 3306
        self._user = "root"
        self._passwd = ""
        self.sql()

    def sql(self):
        """
            Here configure your develop db for comparing with the production db
        """
        _host = str(config.get('mysql', 'hostdb'))
        _port = config.get('mysql', 'port')
        _user = str(config.get('mysql', 'userdb'))
        _passwd = str(config.get('mysql', 'passw'))
        self._db = 'testing'
        self.dbsql2 = MySQLdb.connect(host=self._host, user=self._user, 
                                    port=int(self._port), use_unicode = True,
                                    passwd=self._passwd, db=self._db2, 
                                    cursorclass=MySQLdb.cursors.DictCursor, 
                                    charset = "utf8")
        self.dbsql = MySQLdb.connect(host=_host, user=_user, port=int(_port), 
                                    passwd=_passwd, db=self._db, use_unicode = True,
                                    cursorclass=MySQLdb.cursors.DictCursor, 
                                    charset = "utf8")
        self.cursor, self.cursor2 = self.dbsql.cursor(), self.dbsql2.cursor()
        return

    def compare_tables(self):
        """
            This method search the diferences between table with
            is allocated in your develop database and not in your 
            production database
        """
        print '{:#^100}'.format(' TABLES ')
        self.cursor2.execute(self.select_table % self._db2)
        __tables = []
        for t in self.cursor2.fetchall():
            __tables += [t['TN']]
        self.cursor.execute(self.select_table % self._db)
        self.__tables2 = []
        for t in self.cursor.fetchall():
            self.__tables2 += [t['TN']] if t['TN'] not in __tables else ''
        for t in self.__tables2:
            print t
        print "--Estructuras guardadas en: /home/carlos/000datos.sql\n\n"
        f = open('000datos.sql', 'w')
        for r in self.__tables2:
            self.cursor.execute(self.create_table % r)
            __res = self.cursor.fetchone()
            f.write('\n-- %s -----------------------\n' % __res['Table'])
            f.write(__res['Create Table'].replace('\n', '').encode('utf8') + ';')

    def compare_column(self):
        print '{:#^100}'.format(' COLUMNS ')
        self.cursor2.execute(self.select_column % self._db2)
        __column = {}
        for result in self.cursor2.fetchall():
            if result['TN'] not in self.__tables2:
                if result['TN'] not in __column:
                    __column[result['TN']] = {result['CN']:[result['CT'], 
                                                            result['CD'], 
                                                            result['ISN']]}
                else:
                    __column[result['TN']][result['CN']] = [result['CT'], 
                                                            result['CD'], 
                                                            result['ISN']]
        self.alter_col = {}
        self.add_col = {}
        self.cursor.execute(self.select_column % self._db)
        for result in self.cursor.fetchall():
            if result['TN'] not in self.__tables2:
                if result['CN'] in __column[result['TN']]:
                    if __column[result['TN']][result['CN']][0] != result['CT']:
                        if result['TN'] not in self.alter_col:
                            self.alter_col[result['TN']] = {result['CN']:[result['CT'], 
                                                                          result['CD'], 
                                                                          result['ISN']]}
                        else:
                            self.alter_col[result['TN']][result['CN']] = [result['CT'], 
                                                                          result['CD'], 
                                                                          result['ISN']]
                    del __column[result['TN']][result['CN']]
                elif result['TN'] not in self.add_col:
                    self.add_col[result['TN']] = {result['CN']:[result['CT'], 
                                                                result['CD'], 
                                                                result['ISN']]}
                else:
                    self.add_col[result['TN']][result['CN']] = [result['CT'], 
                                                                result['CD'], 
                                                                result['ISN']]
        for k, v in __column.items():
            for c in v.keys():
                print self.drop_column % (k, c)
        for c in self.add_col:
            for k, v in self.add_col[c].items():
                print self.t_add_column % (c, k, v[0], 
                                        'NULL' if v[2] == 'YES' else "NOT NULL", 
                                        '' if v[1] == None else "DEFAULT %s" % v[1])
            print '\n',

    def compare_foreignkey(self):
        print '{:#^100}'.format(' FOREIGN KEY ')
        # __new = dict(self.add_col,**self.alter_col)
        self.cursor2.execute(self.select_constraint % self._db2)
        __foreign = {}
        for constraint in self.cursor2.fetchall():
            if constraint['TN'] not in __foreign:
                __foreign[constraint['TN']] = {constraint['CN']:""}
            else:
                __foreign[constraint['TN']][constraint['CN']] = ""
        self.cursor.execute(self.select_constraint % self._db)
        for constraint in self.cursor.fetchall():
            if constraint['TN'] in __foreign:
                if constraint['CN'] not in __foreign[constraint['TN']]:
                    print self.t_foreign % (constraint['TN'], 
                                            constraint['CONSTRAINT_NAME'], 
                                            constraint['CN'],
                                            constraint['REFERENCED_TABLE_NAME'], 
                                            constraint['REFERENCED_COLUMN_NAME'])
        pass

    def compare_routines(self):
        print '{:#^100}'.format(' PROCEDURES ')
        self.cursor2.execute(self.select_routines % self._db2)
        __tables = []
        for routines in self.cursor2.fetchall():
            __tables += [routines['ROUTINE_NAME']]
        self.cursor.execute(self.select_routines % self._db)
        self.__tables2 = []
        for routines in self.cursor.fetchall():
            self.__tables2 += [routines['ROUTINE_NAME']] \
                                if routines['ROUTINE_NAME'] not in __tables else ''
        print self.__tables2 

    def close_sql(self):
        self.dbsql.close()
        self.dbsql2.close()

sd = SchemaDiff()
if len(sys.argv) > 1:
    sd.production(sys.argv[1])
else:
    sd.local()
sd.compare_tables()
sd.compare_column()
sd.compare_foreignkey()
sd.compare_routines()
sd.close_sql()


