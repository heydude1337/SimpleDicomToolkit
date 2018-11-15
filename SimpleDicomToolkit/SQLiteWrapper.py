"""
Created on Tue Sep  5 16:54:20 2017

@author: HeyDude
"""
import logging
import sqlite3 as lite
from SimpleDicomToolkit import Logger


class SQLiteWrapper(Logger):
    """ Pythonic interface for a sqlite3 database """

    DATABASE_FILE = 'database.db'
#    ID          = 'id'
    IN_MEMORY   = ':memory:'

    # Datatypes supported by SQLite3
    NULL        = 'NULL'
    TEXT        = 'TEXT'
    REAL        = 'REAL'
    INTEGER     = 'INTEGER'
    BLOB        = 'BLOB'
    ROWID       = 'rowid'
    _LOG_LEVEL  = logging.INFO

    START       = 'start'
    END         = 'end'
    __row_factory = None

    def __init__(self, database_file=None):
        """ Connect to database and create tables
        database:   new or existing database file
        table_name:     names for table(s) that will be used. If they don't
                        exist they will be created."""
        super().__init__()

        if database_file is None:
            database_file = self.DATABASE_FILE

        self.database_file = database_file
        self.connected = False # self.connect() needs this attribute
        self.connection = None # Databse connection
        self.cursor = None # Database cursor
        self._row_factory = None

        self.close()

    def execute(self, sql_query, values=None, close=True, fetch_all=False):
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
                            close=False)
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
        self.logger.debug(('adding column in table {0} with name {1} and'
                          ' vartype{2}').format(table_name, column_name,
                                   var_type))
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
                self.close()
            return

        TEMP_TABLE = 'dummy'
        keep_columns = self.column_names(table_name)
        keep_columns.remove(column_name)

        cmd = ('CREATE TABLE {temp_table} AS SELECT {place_holder}'
               ' FROM {table_name}')

        place_holder = ''
        for name in keep_columns:
            place_holder += name + ', '
        place_holder = place_holder[:-2]
        try:
            self.execute(cmd.format(place_holder=place_holder,
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

    def delete_all_tables(self):
        """"
        Deletes all tables in database without exception. Use with care!
        """
        for table in self.table_names:
            self.delete_table(table)
        self.close()

    def delete_table(self, table_name, close=False):
        """ Delete table """
        if table_name not in self.table_names:
            return
        else:
            cmd = 'DROP TABLE IF EXISTS {table_name}'
            cmd = cmd.format(table_name=table_name)
            self.execute(cmd, close=close)

    def create_table(self, table_name, close=True):
        """ Create a table to the database """
        cmd = ('CREATE TABLE IF NOT EXISTS {table} ')
               # '({col_id} INTEGER AUTO_INCREMENT PRIMARY KEY)')

        cmd = cmd.format(table=table_name)
        self.execute(cmd, close=close)

    def get_column(self, table_name, column_name,
                   close=True, sort_by=None, distinct=True, **kwargs):
        """ Return column values from table """

        result = self.query(table_name, column_names=[column_name],
                            sort_by=sort_by, distinct=distinct, close=close,
                            **kwargs)
        result = [res[0] for res in result]

        return result

    def query(self, source_table, column_names=None,
              close=True, sort_by=None, distinct=False,
              sort_decimal=False,
              **kwargs):
        """ Perform a query on a table. E.g.:
            database.query(city = 'Rotterdam',
                           street = 'Blaak') returns all rows where column
            city has value 'Rotterdam' and column street has value 'Blaak'

            columns specifies which columns will be returned.
        """
        query = ('SELECT {distinct} {column_names} '
                 'FROM {source_table} {where_clause} {order_q}')

        distinct = 'DISTINCT' if distinct else ''

        if column_names is None:
            # if columns are None assume values for every column are passes
            column_names = '*'
        else:
            column_names = SQLiteWrapper._list_to_string(column_names)

        where_clause, values = SQLiteWrapper._where_clause(**kwargs)

        if sort_by is None:
            order_q = ''
        else:
            order_q = SQLiteWrapper._order_clause(sort_by=sort_by,
                                                  sort_decimal=sort_decimal)

        query = query.format(column_names=column_names,
                             source_table=source_table,
                             where_clause=where_clause,
                             order_q=order_q,
                             distinct=distinct)

        self.logger.debug('Executing query')
        self.logger.debug(query)
        self.logger.debug(values)

        result = self.execute(query, values=values, fetch_all=True,
                              close=close)

        return result


    def insert_list(self, table_name, values, column_names=None, close=True):
        """ Insert a  list with values as a SINGLE row. Each value
        must correspond to a column name in column_names. If column_names
        is None, each value must correspond to a column in the table.

        values = (val1, val2, ..)
        columns = (col1, col2, ..)
        """
        cmd = 'INSERT INTO {table_name}({column_names}) VALUES'


        if column_names is None:
            column_names = self.column_names(table_name)

        if len(column_names) > 1 and len(values) != len(column_names):
            raise IndexError('Number of values must match number of columns!')

        if not isinstance(values, (list, tuple)):
            values = [values]

        if not isinstance(column_names, (list, tuple)):
            column_names = [column_names]

        column_names = SQLiteWrapper._list_to_string(column_names)

        cmd = cmd.format(column_names=column_names, table_name=table_name)

        if isinstance(values, (tuple, list)):
            cmd += SQLiteWrapper.binding_str(len(values))
        else:
            cmd += SQLiteWrapper.binding_str(1)

        self.execute(cmd, values=values, close=close)

    def insert_lists(self, table_name, values, column_names=None, close=True):
        """ Insert a  list with values as multiple rows. Each value in a row
        must correspond to a column name in column_names. If column_names
        is None, each value must correspond to a column in the table.

        values = ((val1, val2,..),(val3, val4, ..))
        columns = (col1, col2, ..)
        """

        for value in values:
            self.insert_list(table_name, value, column_names=column_names,
                             close=False)

        self.close(close)

    def get_row_dict(self, *args, **kwargs):
        """
        Read row from database and return it as dictionary
        """
        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d
        old_row_factory = self._row_factory
        self._row_factory = dict_factory
        result = self.query(*args, **kwargs)
        self._row_factory = old_row_factory
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
        """
        Delete rows from table where column value equals specified value
        """

        cmd = "DELETE FROM {table} WHERE {column}=?"
        cmd = cmd.format(table=table_name, column=column)

        self.execute(cmd, values=[value], close=close)

    def connect(self):
        """Connect to the SQLite3 database."""

        if not self.connected:
            try:
                self.connection = lite.connect(self.database_file)

            except lite.OperationalError:
                msg = 'Could not connect to %s'
                self.logger.error(msg, self.database_file)
                raise
            self.connection.row_factory = self._row_factory
            self.cursor = self.connection.cursor()
            self.connected = True
            if self._LOG_LEVEL == logging.DEBUG:
                self.connection.set_trace_callback(print)

    def close(self, close=True):
        """Dicconnect form the SQLite3 database and commit changes."""

        if not close:
            return

        if self.connected:
            msg = ('\n\n !!! Closing database connection and committing '
                   'changes. !!!\n\n')
            self.logger.debug(msg)

            self.connection.commit()

            if self.database_file != self.IN_MEMORY:
                # in memory database will caese to exist upon close
                self.connection.close()
                self.connecion = None
                self.cursor = None
                self.connected = False

    def sum_column(self, table, column, **kwargs):
        """ Sum all values of a column in a table """
        where, values = self._where_clause(**kwargs)
        cmd = 'SELECT SUM({column}) FROM {table} {where}'

        cmd = cmd.format(table=table, column=column, where=where)

        return self.execute(cmd, values=values, fetch_all=True)[0][0]

    def count_column(self, table, column, distinct=False, **kwargs):
        """
        Count the number of values in a column, use distinct to
        count the number of unique values
        """
        where, values = self._where_clause(**kwargs)
        cmd = 'SELECT COUNT({distinct} {column}) FROM {table} {where}'
        distinct = 'DISTINCT' if distinct else ''
        cmd = cmd.format(table=table, column=column, distinct=distinct,
                         where = where)
        return self.execute(cmd, values=values, fetch_all=True)[0][0]

    def column_has_value(self, table, column, value):
        """ True if column contains the value """
        cmd = 'SELECT EXISTS(SELECT 1 FROM {table} WHERE {column}=? LIMIT 1)'
        cmd = cmd.format(table=table, column=column)
        return bool(self.execute(cmd, values=[value], fetch_all=True)[0][0])

    def set_column_where(self, table, column, value, **kwargs):
        """ Set values in a column where criterium is met """
        cmd = "UPDATE {table} SET {column} = ? {where_clause}"
        where_clause, values = self._where_clause(**kwargs)
        cmd = cmd.format(table=table, column=column, where_clause=where_clause)
        values = [value, *values]
        self.execute(cmd, values=values)

    def get_column_where(self, table, column, sort_by=None, sort_decimal=False,
                         **kwargs):
        """
        Return values in a column where criterium is met
        """
        result = self.query(table, column_names=[column], sort_by=sort_by,
                            sort_decimal=sort_decimal, **kwargs)
        return [res[0] for res in result]

    def set_column(self, table, column, value):
        """ Set entire column to same value """
        self.set_column_where(table, column, value)

    def pragma(self, table_name, close=True):
        """ Return information about the table """
        cmd = 'PRAGMA TABLE_INFO({table_name})'.format(table_name=table_name)
        result = self.execute(cmd, fetch_all=True)
        self.close(close)
        return result

    def column_names(self, table_name, close=True):
        """ Return all column names in the table """
        pragma = self.pragma(table_name)
        column_names = [pi[1] for pi in pragma]
        self.close(close)
        return column_names

    @property
    def in_memory(self):
        """ Return True if database soly exists in memory """
        return self.database_file == SQLiteWrapper.IN_MEMORY

    @property
    def _row_factory(self):
        """
        Set the row factory to format sql responses
        """
        return self.__row_factory

    @_row_factory.setter
    def _row_factory(self, factory):
        self.__row_factory = factory
        if self.connected:
            self.connection.row_factory = self.__row_factory
            self.cursor = self.connection.cursor()

    @property
    def table_names(self):
        """
        Return the names of all tables in the database
        """
        cmd = "SELECT name FROM sqlite_master WHERE type='table';"
        close = not self.connection
        result = self.execute(cmd, fetch_all=True, close=close)
        return [ri[0] for ri in result]

    @staticmethod
    def _list_to_string(list1):
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

    @staticmethod
    def is_between_dict(value):
        """
        Returns true if value represents a dict with the unique keywords
        'start' and 'end' indicating a range of values
        """

        val = False
        if isinstance(value, dict) and len(value) == 2:
            if SQLiteWrapper.END in value.keys():
                if SQLiteWrapper.START in value.keys():
                    val = True
        return val

    @staticmethod
    def _order_clause(sort_by='MyColumn', sort_decimal=False):
        if sort_decimal is True:
            clause = ' ORDER BY CAST({0} AS DECIMAL) '
        else:
            clause = ' ORDER BY {0} '

        clause = clause.format(sort_by)
        return clause


    @staticmethod
    def _where_clause(**kwargs):
        # construct a where clause based on keywords. Column names are the
        # keys and the values are used as values.
        if not kwargs:
            return '', []

        def _between(val):
            # construct part of the where clause statement where a range
            # of calues is specified
            start, end = (val[SQLiteWrapper.START], val[SQLiteWrapper.END])
            if start > end:
                msg = 'Start lies beyond end in selection'
                raise ValueError(msg)
            expr = ' BETWEEN ? AND ? '
            val = [start, end]
            return expr, val

        def _single_clause(column, val):
            # construct a part of the where clause statement for single
            # column and value
            if isinstance(val, (list, tuple)):
                expr = column + ' IN '+ SQLiteWrapper.binding_str(len(val))
            elif SQLiteWrapper.is_between_dict(val):
                expr, val = _between(val)
                expr = column + expr
            else:
                expr = column + ' = ?'
                val = [val]
            return expr, val


        where_clause = 'WHERE '
        # append multiple conditions
        values = []
        for col_name, value in kwargs.items():
            expr, value = _single_clause(col_name, value)
            where_clause += expr
            where_clause += ' AND '
            values += value

        where_clause = where_clause[:-4]  # remove last AND from string


        return where_clause, values
