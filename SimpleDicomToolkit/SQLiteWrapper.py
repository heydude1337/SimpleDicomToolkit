"""
Created on Tue Sep  5 16:54:20 2017

@author: HeyDude
"""

from SimpleDicomToolkit import Logger
import sqlite3 as lite
import logging

class SQLiteWrapper(Logger):
    """ Pythonic interface for a sqlite3 database """

    DATABASE_FILE = 'database.db'
    ID          = 'id'
    IN_MEMORY   = ':memory:'

    # Datatypes supported by SQLite3
    NULL        = 'NULL'
    TEXT        = 'TEXT'
    REAL        = 'REAL'
    INTEGER     = 'INTEGER'
    BLOB        = 'BLOB'

    _LOG_LEVEL   = logging.ERROR


    def __init__(self, database_file=None):
        """ Connect to database and create tables
        database:   new or existing database file
        table_name:     names for table(s) that will be used. If they don't
                        exist they will be created."""

        self.in_memory=False

        if database_file is None:
            database_file = self.DATABASE_FILE
        elif database_file == SQLiteWrapper.IN_MEMORY:
            self.in_memory=True

        self.database_file = database_file
        self.connected = False # self.connect() needs this attribute
        self.connection = None # Databse connection
        self.cursor = None # Database cursor
        self._row_factory = None

        self.close()
    @property
    def row_factory(self):
        return self._row_factory
    
    @row_factory.setter
    def row_factory(self, factory):
        self._row_factory = factory
        if self.connected:
            self.connection.row_factory = self._row_factory
            self.cursor = self.connecion.cursor()
        
    def execute(self, sql_query, values=None, close=True, fetch_all=False, debug=False):
        """ Execute a sql query, database connection is opened when not
        already connected. Connection  will be closed based on the close flag.
        If fetch_all is True, all results are fetched from query.
        """

        self.connect()
        
        self.logger.debug(sql_query)
        try:
            if values is None:
                result = self.cursor.execute(sql_query)
            else:
                result = self.cursor.execute(sql_query, values)
        except:
            self.logger.error('Could not excute query: \n %s', sql_query)
            self.logger.error('Wiht values: \n %s', values)
            raise

        if fetch_all:
            result = result.fetchall()
        if close:
            self.close()

        return result

    def add_columns(self, table_name, column_names, var_type=None,
                    close=True):
        """ Add columns to a table """

        # keep correspondence
        var_type = dict(zip(column_names, var_type))

        # remove existing columns
        column_names = set(column_names)
        column_names = column_names.difference(self.column_names(table_name))

        if not column_names:
            return

        for name in column_names:
            self.add_column(table_name, name, var_type=var_type[name],
                            close=False, skip_if_exist=False)

        if close:
            self.close()


    def add_column(self, table_name, column_name, var_type=None,
                   close=True):
        """ Add columns to a table. New columns will be created,
        existing columns will be ignored."""

        if var_type is None:
            var_type = self.TEXT

        cmd = 'ALTER table {table_name} ADD COLUMN {column_name} {var_type}'

        #if not column_name in self.column_names(table_name):

        self.execute(cmd.format(column_name=column_name,
                                table_name=table_name,
                                var_type=var_type), close=False)
        if close:
            self.close()

    def rename_table(self, source_name, destination_name, 
                     overwrite=True, close=True):
        """ Rename Table """
        if overwrite:
            self.delete_table(destination_name)
        cmd = 'ALTER TABLE {source_name} RENAME TO {destination_name}'
        self.execute(cmd.format(source_name=source_name,
                                 destination_name=destination_name),
                                 close=close)

    def delete_column(self, table_name, column_name, close=True):
        """ Delete column from table """

        if not column_name in self.column_names(table_name):
            if close:
                self.close
            return

        TEMP_TABLE = 'dummy'
        keep_columns = self.column_names(table_name)
        keep_columns.remove(column_name)
        # keep_columns.remove(self.ID)

        cmd  = ('CREATE TABLE {temp_table} AS SELECT {place_holder}'
                ' FROM {table_name}')

        place_holder = ''
        for name in keep_columns:
            place_holder += name + ', '
        place_holder = place_holder[:-2]
        try:
            self.execute(cmd .format(place_holder = place_holder,
                                     temp_table=TEMP_TABLE,
                                     table_name=table_name),
                                     close=False)
        except:
            self.delete_table(TEMP_TABLE)
            raise

        self.delete_table(table_name, close=False)
        self.rename_table(TEMP_TABLE, table_name, close=False)

        if close:
            self.close()

    def delete_table(self,  table_name, close=False):
        """ Delete table """
        if table_name not in self.table_names:
            return
        else:
            cmd = 'DROP TABLE IF EXISTS {table_name}'.format(table_name=table_name)
            self.execute(cmd, close=close)

    def create_table(self, table_name, close=True):
        """ Create a table to the database """
        cmd = ('CREATE TABLE IF NOT EXISTS {table} '
               '({col_id} INTEGER AUTO_INCREMENT PRIMARY KEY)')

        cmd = cmd.format(table=table_name, col_id=self.ID)
        self.execute(cmd, close=close)


    def get_column(self, table_name, column_name, 
                   close=True, sort_by=None, distinct=True, **kwargs):
        """ Return column values from table """


#            
#        if distinct:
#            distinct = 'DISTINCT'
#        else:
#            distinct = ''
#
#        cmd = 'SELECT {distinct} {column} FROM {table}'
#        if sort:
#            cmd += ' ORDER BY {column}'
#        cmd = cmd.format(column=column_name, table=table_name, distinct=distinct)
#        result = self.execute(cmd, close=close, fetch_all=True)
        result = self.query(table_name, column_names=[column_name],
                            sort_by=sort_by, distinct=distinct, close=close,
                            **kwargs)
        result = [res[0] for res in result]

        return result

    def query(self, source_table, destination_table = None, column_names=None,
               close=True, sort_by=None, partial_match=False, distinct = False,
               row_factory = None, print_query=False, sort_decimal=False,
               **kwargs):
        """ Perform a query on a table. E.g.:
            database.query(city = 'Rotterdam',
                           street = 'Blaak') returns all rows where column
            city has value 'Rotterdam' and column street has value 'Blaak'

            columns specifies which columns will be returned.
        """
        TEMP_TABLE = 'temp_query_table'
        query = ('{create_table} SELECT {distinct} {columns} FROM {source_table} '
                '{where_clause} {order_q}')
        if distinct:
            distinct = 'DISTINCT'
        else:
            distinct = ''
            
        if destination_table is None:
            create_table = ''
        else:
            self.delete_table(TEMP_TABLE)
            create_table = 'CREATE TABLE {destination_table} AS ' 
            create_table = create_table.format(destination_table=TEMP_TABLE)
            
        if column_names is None:
            # if columns are None assume values for every column are passes
            columns = '*'
        else:
            columns = SQLiteWrapper.list_to_string(column_names)
        
        operator = 'LIKE' if partial_match else '='
        
        where_clause = self._where_clause(operator=operator, **kwargs)
        
                
        if sort_by is None:
            order_q = ''
        else: 
            order_q = self._order_clause(sort_by=sort_by, 
                                         sort_decimal=sort_decimal)

        values = list(kwargs.values())

        if partial_match:
            values = ['%{}%'.format(value) for value in values]

        if row_factory is not None:
            self.row_factory = row_factory
            
        query = query.format(create_table=create_table, columns=columns,
                    source_table=source_table, where_clause=where_clause,
                    order_q=order_q, distinct=distinct)    
        
        if print_query:
            print(query)
            
        result = self.execute(query, values=values, fetch_all=True, 
                              close=close)
        
        if destination_table is not None:
             self.rename_table(TEMP_TABLE, destination_table)
             
        return result


    def insert_list(self, table_name, values, column_names=None, close=True):
        """ Insert a  list with values as a SINGLE row. Each value
        must correspond to a column name in column_names. If column_names
        is None, each value must correspond to a column in the table.

        values = (val1, val2, ..)
        columns = (col1, col2, ..)
        """
        cmd = 'INSERT INTO {table_name}({column_names}) VALUES'


#        if column_names is None:
#            column_names = self.column_names(table_name)
#        if len(column_names) > 1 and len(values) != len(column_names):
#            raise IndexError('Number of values must match number of columns!')
        if not isinstance(values, (list, tuple)):
            values = [values]
        if not isinstance(column_names, (list, tuple)):
            column_names = [column_names]


        cmd = cmd.format(column_names=SQLiteWrapper.list_to_string(column_names),
                         table_name=table_name)
        if isinstance(values, (tuple,list)):
            cmd += SQLiteWrapper.binding_str(len(values))
        else:
            cmd += SQLiteWrapper.binding_str(1)

        self.execute(cmd, values=values, close = close)

    def insert_lists(self, table_name, values, column_names=None, close=True):
        """ Insert a  list with values as multiple rows. Each value in a row
        must correspond to a column name in column_names. If column_names
        is None, each value must correspond to a column in the table.

        values = ((val1, val2,..),(val3, val4, ..))
        columns = (col1, col2, ..)
        """

        for value in values:
            self.insert_list(table_name, value, column_names=column_names, close=False)

        self.close(close)

    def get_row_dict(self, *args, **kwargs):
        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d
         
        row_factory = dict_factory
        result = self.query(*args, row_factory=row_factory, **kwargs)
        self.row_factory = None
        return result
    
    def insert_row_dict(self, table_name, data_dict, close=True):
        """ Insert a dictionary in the table. Dictionary must be as follows:
            datadict = {city: ['Rotterdam, 'Amsterdam',..],
                        streets: ['Blaak', 'Kalverstraat,..],
                        ....}
        """
        columns = list(data_dict.keys())
        values = list(data_dict.values())

        self.insert_list(table_name, values, column_names=columns, close=close)

    def delete_rows(self, table_name, column=None, value=None, close=True):
        """ Delete rows from table where column value equals specified value """

        cmd = "DELETE FROM {table} WHERE {column}=?"
        cmd = cmd.format(table=table_name, column=column)

        self.execute(cmd, values=[value], close=close)

    def connect(self):
        """Connect to the SQLite3 database."""

        if not self.connected:
            try:
                self.connection = lite.connect(self.database_file)
                
            except lite.OperationalError:
                self.logger.error('Could not connect to {0}'.format(self.database_file))
                raise
            self.connection.row_factory = self.row_factory
            self.cursor = self.connection.cursor()
            self.connected = True
            if self._LOG_LEVEL == logging.DEBUG:
                self.connection.set_trace_callback(print)

    def close(self, close = True):
        """Dicconnect form the SQLite3 database and commit changes."""

        if not close:
            return

        if self.connected:
            msg = '\n\n !!! Closing database connection and committing changes. !!!\n\n'
            self.logger.debug(msg)

            # traceback.print_stack()

            self.connection.commit()

            if self.database_file != self.IN_MEMORY:
                # in memory database will caese to exist upon close
                self.connection.close()
                self.connecion = None
                self.cursor = None
                self.connected = False
                
    @staticmethod
    def _order_clause(sort_by='MyColumn', sort_decimal = False):
        if sort_decimal is True:
            clause = ' ORDER BY CAST({0} AS DECIMAL) '
        else:
            clause = ' ORDER BY {0} '
        
        clause = clause.format(sort_by)
        return clause   
    
    @staticmethod
    def _where_clause(operator = '=', **kwargs):
        if len(kwargs) == 0:
            where_clause = ''
        else:
            where_clause = 'WHERE '
            # append multiple conditions
            for col_name, value in kwargs.items():
                if operator == '=' and isinstance(value, (list, tuple)):
                    operator = 'IN'
                    place_holder = SQLiteWrapper.binding_str(len(value))
                else:
                    place_holder = '?'
                clause = '{col_name} {operator} {place_holder} AND '
                where_clause += clause.format(col_name=col_name,
                                              operator=operator,
                                              place_holder=place_holder) 
                
            where_clause = where_clause[:-4]  # remove last AND from string
           

        return where_clause
    
    
    def set_column_where(self, table, column, value, partial_match=False, **kwargs):
        cmd = "UPDATE {table} SET {column} = ? {where_clause}"
    
        where_clause = self._where_clause(**kwargs)
        
        cmd = cmd.format(table=table, column=column, where_clause=where_clause)
                              
        values = self.chain_values([value] + list(kwargs.values()))
        
        self.execute(cmd, values=values)
        
    def get_column_where(self, table, column, sort_by=None, sort_decimal=False,
                         partial_match=False, **kwargs):
        
        result = self.query(table, column_names=[column], sort_by=sort_by,
                            partial_match=partial_match, 
                            sort_decimal=sort_decimal, **kwargs)
        return [res[0] for res in result]
    
    def set_column(self, table, column, value):
        """ Set entire column to same value """
        self.set_column_where(table, column, value)
        
    def pragma(self, table_name, close=True):
        cmd = 'PRAGMA TABLE_INFO({table_name})'.format(table_name=table_name)
        result = self.execute(cmd, fetch_all=True)
        self.close(close)
        return result

    def column_names(self, table_name, close=True):
        pragma = self.pragma(table_name)
        column_names = [pi[1] for pi in pragma]
        self.close(close)
        return column_names

    @property
    def table_names(self):
        cmd = "SELECT name FROM sqlite_master WHERE type='table';"
        if not self.connection:
            close = True
        else:
            close = False
        result = self.execute(cmd, fetch_all=True, close=close)
        return [ri[0] for ri in result]

    @staticmethod
    def list_to_string(list1):
        # convert list to str "val1, val2, val3 withoud [ or ]
        list_str = ''
        for li in list1:
            list_str += str(li) + ', '
        # remove trailing ,
        list_str = list_str[:-2]
        return list_str
    @staticmethod
    def chain_values(values):
        chain = []
        for v in values:
            if isinstance(v, (list, tuple)):
                chain += v
            else:
                chain += [v]
        return chain
    @staticmethod
    def binding_str(number):
        """ Convert list of numbers to a SQL required format in str """
        binding_str = '('
        for _ in range(0, number):
            binding_str += '?,'
        binding_str = binding_str[:-1] # remove trailing ,
        binding_str += ')'
        return binding_str

