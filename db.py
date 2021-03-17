'''Module allows to interact with databases on unified functional level

Supports following databases:
    -> Berkeley DB
    -> sqlite3

Example usage:
>>> def procedures_with_database(db):
...     data.open('games')
...     data.put('games','game1','germany')
...     assert data.get('games','game1') == 'germany'
...     data.table('games').add_index('date')
>>> class MySQLite(SQLite): pass
>>> with MySQLite('.') as data: 
...     procedures_with_database(data)
>>> class MyBerkeley(Berkeley): pass
>>> with MyBerkeley('.') as data:
...     procedures_with_database(data)
'''
import os

def verify(db_file):
    '''Verifies database file.'''
    data = db.DB()
    data.verify(db_file)

def removeEnv(home_folder):
    '''Removes environment created.'''
    env_files = os.listdir(home_folder)
    for db_file in env_files:
        if db_file[0:2] == '__':
            os.remove(os.path.join(home_folder,db_file))


class Data():
    '''Main database access class. It defines methods that should be supported by any database.
    path - where to store database file, defaults to program runtime path
    '''
    def __init__(self,path):
        if not os.path.exists(path):
            os.mkdir(path)
        self.path = path
        self.tables = {}

    def __enter__(self):
        return self

    def __exit__(self,exc_type, exc_value, traceback):
        pass

    def put(self,name,key,data):
        self.assert_exist(name)
        self.tables[name].put(key,data)

    def get(self,name,key):
        self.assert_exist(name)
        value = self.tables[name].get(key)
        return value

    def delete(self,name,key):
        self.assert_exist(name)
        key = str(key).encode()
        self.tables[name].delete(key)

    def verify(self,name):
        self.tables[name].verify(name+'.db')

    def table(self,name,**key):
        self.assert_exist(name)
        if not key:
            return self.tables[name]
        table = self.tables[name]
        while key:
            k, v = key.popitem()
            index_obj = getattr(table, k)
            setattr(index_obj, 'cursor_value', v)
            return index_obj

    def open(self,name,db_obj):
        new_table = db_obj
        self.tables[name] = new_table

    def assert_exist(self,table):
        if not table in self.tables:
            self.open(table)

class Index():
    '''Helper class for Berkeley DB index instances.'''

    def __iter__(self):
        return self

    def __next__(self):
        if not self.cursor: 
            self.cursor = self.db.cursor()
            item = self.cursor.pget(self.cursor_value.encode(),db.DB_GET_BOTH_RANGE)
        else:
            if self.cursor.next_dup():
                item = self.cursor.pget(db.DB_CURRENT)
            else:
                item = None
        if not item:
            self.cursor.close()
            self.cursor = False
            raise StopIteration
        i_key, key, value = item
        return key.decode(), value.decode()

    def __len__(self):
        if not self.cursor: self.cursor = self.db.cursor()
        a = 0
        while self.cursor.next():
            a += 1
        self.cursor.close()
        self.cursor = False
        return a


class Table():

    def __init__(self,name):
        self.name = name
        self.cursor = False
        self.indexes = {}

    def __iter__(self):
        return self

    def __next__(self):
        if not self.cursor: self.cursor = self.db.cursor()
        item = self.cursor.next()
        if not item:
            self.cursor.close()
            self.cursor = False
            raise StopIteration
        key, value = item
        return key.decode(), value.decode()

    def put(self,key,data):
        print('Put method must be defined!')

    def get(self,key):
        print('Get method must be defined!')

    def delete(self,key):
        return self.db.delete(key)
    
    def verify(self,name):
        return self.db.verify(name)


###Specific database access classes go here

#Berkeley DB classes
class Berkeley(Data):
    '''Berkeley database access class.
    path - where to store database file, defaults to program runtime path
    thread_safe - allows multiple posts and reads from threads
    '''
    def __init__(self,path,thread_safe=True):
        global db
        from bsddb3 import db
        Data.__init__(self, path)
        self.env = db.DBEnv()
        if not thread_safe:
            DB_THREAD = 0
        else:
            DB_THREAD = db.DB_THREAD
        flags = db.DB_CREATE + db.DB_INIT_MPOOL + DB_THREAD
        self.env.open(path, flags)

    def __exit__(self,exc_type, exc_value, traceback):
        self.close()

    def add_index(self,index,index_callback,table):
        '''adds index to table, can be as many as one likes
        index_callback must be Python callable object that will construct and return secondary key.
        '''
        if index in self.tables:
            raise 'Index name shall not be the same as table name itself'
        setattr(self, index, Index())
        instance = getattr(self, index)
        setattr(instance,'db',db.DB(self.env))
        getattr(instance,'db').set_flags(db.DB_DUPSORT)
        getattr(instance,'db').open(table + '_idx_' + index + '.db', db.DB_BTREE, db.DB_CREATE)
        setattr(instance, 'cursor', False)
        print(instance)
        print(getattr(instance, 'db'))
        self.db.associate(getattr(instance,'db'),index_callback)
        self.indexes.append(instance)

    def open(self,name):
        db_obj = db.DB(self.env)
        db_obj.open(name + '.db', db.DB_BTREE, db.DB_CREATE)
        new_table = BSDTable(name, db_obj)
        Data.open(self, name, new_table)

    def close(self):
        for index in self.indexes:
            cursor = getattr(index, 'cursor')
            if cursor:
                cursor.close()
            db = getattr(index, 'db')
            db.close()
        for table in self.tables.values():
            if table.cursor:
                table.cursor.close()
            table.close()
        self.env.close()

class BSDTable(Table):


    def __init__(self, name, db_obj):
        self.db = db_obj
        Table.__init__(self, name)

    def put(self,key,data):
        k,v = key.encode(), data.encode()
        self.db.put(k, v)

    def get(self,key):
        data = self.db.get(key.encode())
        return data.decode() 

    def close(self):
        self.db.close()

#SQLite3 classes
class SQLite(Data):
    '''Class to access SQLite database''' 

    def __init__(self,path,filename='data'):
        global sqlite3
        import sqlite3
        Data.__init__(self, path)
        self.db = sqlite3.connect(path + filename)
        self.cur = self.db.cursor()

    def __exit__(self,exc_type, exc_value, traceback):
        self.db.commit()
        self.db.close()

    def open(self,name):
        if not self.cur.execute('''SELECT * FROM sqlite_master WHERE type='table' AND name=?''', (name,)).fetchone():
            stmt = "CREATE TABLE {0} (key text, value text)".format(name)
            self.cur.execute(stmt)
            self.db.commit()
        new_table = SQLTable(name, self.cur, self.db)
        Data.open(self, name, new_table)


class SQLTable(Table):

    def __init__(self,name,db_cursor,db):
        Table.__init__(self,name)
        self.cur = db_cursor
        self.db = db
    
    def __len__(self):
        stmt = "SELECT COUNT(*) FROM {0};".format(self.name)
        return self.cur.execute(stmt).fetchone()[0]

    def __next__(self):
        if not self.cursor:
            stmt = "SELECT key,value FROM {0};".format(self.name)
            self.cursor = self.cur.execute(stmt)
        item = self.cursor.fetchone()
        if not item:
            self.cursor = False
            raise StopIteration
        key, value = item
        return item

    def put(self,key,data,**indexes):
        key = str(key)
        data = str(data)
        if not self.get(key):
            self.sql_statement("INSERT",key,data,**indexes)
        else:
            self.sql_statement("UPDATE",key,data,**indexes)

    def get(self,key):
        key = str(key)
        stmt = "SELECT value FROM {0} WHERE key=?;".format(self.name)
        a = self.cur.execute(stmt, (key,)).fetchone()
        if a: 
            return a[0]

    def delete(self,key):
        key = str(key)
        stmt = "DELETE FROM {0} WHERE key=?;".format(self.name)
        self.cur.execute(stmt, (key,))

    def add_index(self,index):
        '''adds index to table, can be as many as one likes
        index_callback must be Python callable object that will construct and return secondary key.
        '''
        if index in self.__dict__['indexes']:
            raise 'Index already exists'
        stmt = "ALTER TABLE {0} ADD COLUMN ?;".format(self.name)
        self.cur.execute(stmt, (index,))
        self.db.commit()
        self.__dict__['indexes'].append(index, Index())

    def sql_statement(self,mode,key,value,**indexes):
        if mode == 'INSERT':
            stmt = "INSERT INTO {0} VALUES (?,?);".format(self.name)
            self.cur.execute(stmt, (key,value))
        elif mode == 'UPDATE':
            stmt = "UPDATE {0} SET key=?, value=? WHERE key=?;".format(self.name)
            self.cur.execute(stmt, (key,value,key))
        if indexes:
            for index in indexes:
                '''Each index updates entry in a specific column'''
                stmt = "UPDATE {0} SET ?=? WHERE key=?;".format(self.name)
                self.cur.execute(stmt, (index,indexes[index],key))
        self.db.commit()


#Berkeley DB class instance with your own with statements
class AmendBerkeley(Berkeley):

    def __init__(self,path):
        print('Initialise something myself')
        Berkeley.__init__(self,path)
    
    def __enter__(self):
        return self

    def __exit__(self,exc_type, exc_value, traceback):
        print('Do closing cleanup for something myself')
        Berkeley.__exit__(self,exc_type, exc_value, traceback)

if __name__ == '__main__':
    import doctest
    doctest.testmod()
    '''
    with SubSQLite('/tmp/db') as data:
        data.open('games')
        data.open('comps')
        data.put('games','game2','germany')
        data.put('games','game3','usa')
        print('Total count in games: ',len(data.table('games')))
        print(data.get('games','game1'))
        print('List all keys and values')
        print('Total count in list: ',len(data.table('games')))
        print('All records in games database:')
        for key, value in data.table('games'):
            print(key,value)
        data.delete('games','game3')
    import threading
    def database_close(database):
        try:
            threading.main_thread().join()
            database.__exit__(None,None,None)
            print('Just closing')
        except:
            database.__exit__(None,None,None)
            print('Exception closing')
    data = SubSQLite('/tmp/db') 
    threading.Thread(target=database_close,args=(data,)).start()
    print('Remove environment')
    print(removeEnv('/tmp/db/'))
    print('Removed environment')
    with SubData('/tmp/db2') as data:
        #TEST CASE: DB indexes
        def callback(key,data):
            if key.decode() == 'game1':
                return b'2020-10-26'
            else:
                return b'2020-11-26'
        data.open('games')
        data.add_index('date',callback,'games')
        data.open('comps')
        data.put('games','game2','germany')
        data.put('games','game3','usa')
        print('Data verify: ' + str(data.verify('games')))
        print('Total count in comps: ',len(data.table('comps')))
        print(data.get('games','game1'))
        print('List all keys and values')
        print('Total count in list: ',len(data.table('games')))
        print('All records in games database:')
        for key, value in data.table('games'):
            print(key,value)
        date = '2020-10-26'
        print('Records with date: ', date)
        for key, value in data.table('games', date=date):
            print(key,value)
        print('Checking if closing is succesfull if cursor is left open')
        for key, value in data.table('games'):
            break
        data.delete('games','game3')
    print(verify('/tmp/db/games.db'))
    import threading
    def database_close(database):
        try:
            threading.main_thread().join()
            database.__exit__(None,None,None)
            print('Just closing')
        except:
            database.__exit__(None,None,None)
            print('Exception closing')
    data = Data('/tmp/db') 
    threading.Thread(target=database_close,args=(data,)).start()
    print('Remove environment')
    print(removeEnv('/tmp/db/'))
    print('Removed environment')
    '''
