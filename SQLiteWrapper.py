import sqlite3 as lite
import logging

class SQLiteWrapper(object):
    """ Pythonic interface for a sqlite3 database """

    DATABASE    = 'database.db'
    ID          = 'id' #column name f
    IN_MEMORY   = ':memory:'
    
    # Datatypes supported by SQLite3
    NULL        = 'NULL'
    TEXT        = 'TEXT'
    REAL        = 'REAL'
    INTEGER     = 'INTEGER'
    BLOB        = 'BLOB'
    
    
    LOG_LEVEL   = logging.ERROR
    _stored_logger = None
    
    def __init__(self, database = None, table_name = None):
        """ Connect to database and create tables 
        database:   new or existing database file
        table_name:     names for table(s) that will be used. If they don't 
                        exist they will be created."""
                        
        if database is None:
            database = self.DATABASE
            
        if type(table_name) not in (tuple, list) and table_name is not None:
            table_name = [table_name]

        self.database = database
        self.connected = False # self.connect() needs this attribute
        
        # create specified tables
        if table_name is not None:
            for name in table_name:
                self.create_table(table_name = name, close=False)
        self.close()
    
    @property
    def _logger(self):
        if self._stored_logger is None:
            self._stored_logger = application_logger(self.__class__.__name__, 
                                            log_level=self.LOG_LEVEL)
        return self._stored_logger
    
    def execute(self, sql_query, values = None, close = True, fetch_all = False):
        """ Execute a sql query, database connection is opened when not 
        already connected. Connection  will be closed based on the close flag.
        If fetch_all is True, all results are fetched from query.
        """
            
        self.connect()
        # self._logger.debug(sql_query)
        try:
            if values is None:
                result = self.cursor.execute(sql_query)
            else:            
                result = self.cursor.execute(sql_query, values)
        except:
            self._logger.error('Could not excute query: \n %s', sql_query)
            raise
            
        if fetch_all:
            result = result.fetchall()
        if close: self.close()
        
        return result
    
    def add_columns(self, table_name, column_names, var_type = None,
                         close = True, skip_if_exist = True):
        
        if skip_if_exist:
            existing_names = self.column_names(table_name = table_name, close = False)
            for name in existing_names:
                column_names.remove(name)
        
        if len(column_names) == 0: return
        
        for name in column_names:
            self.add_column(table_name, name, var_type = var_type,
                            close = False, skip_if_exist = False)
        
        if close: self.close()
        
            
    def add_column(self, table_name, column_name, var_type = None,
                         close = True, skip_if_exist = True):
        """ Add columns to a table. New columns will be created,
        existing columns will be ignored."""
        
        if var_type is None: var_type = self.TEXT
        
        # ignore existing columns
        if skip_if_exist:
            existing_names = self.column_names(table_name = table_name, close = False)
            if column_name in existing_names: return
              
        cmd = 'ALTER table {table_name} ADD COLUMN {column_name} {var_type}'

        self.execute(cmd.format(column_name = column_name, 
                                table_name = table_name,
                                var_type = var_type), close = False)
            
        if close: self.close()
        
    def column_names(self, table_name, close = True):
        """ Read column names from table. """
        cmd = "select * from {table_name}"
        cmd = cmd.format(table_name = table_name)
        columns = self.execute(cmd, close = False)
        columns = [member[0] for member in self.cursor.description]
        if close: self.close()
        return columns
    
    def create_table(self, table_name, close = True):
        
        cmd = """CREATE TABLE  IF NOT EXISTS {table}
                 ({col_id} INTEGER AUTO_INCREMENT PRIMARY KEY)"""
        
        cmd = cmd.format(table = table_name, col_id = self.ID)        
        self.execute(cmd, close = close)

        
    def get_column(self, table_name, column_name, close=True, sort = False):
        """ Return column values from table """
      
        cmd = 'SELECT {column} FROM {table}'
        if sort:
            cmd += ' ORDER BY {column}'
        cmd = cmd.format(column = column_name, table=table_name)
        result = self.execute(cmd, close = close, fetch_all=True)
        
        result = [res[0] for res in result]
      
        if close: self.close()
        return result
    
    def query(self, table_name, column_names = None, close = True, 
              sort_by = None, partial_match = False, **kwargs):
        """ Perform a query on a table. E.g.:
            database.query(city = 'Rotterdam',
                           street = 'Blaak') returns all rows where column
            city has value 'Rotterdam' and column street has value 'Blaak'
            
            columns specifies which columns will be returned.
        """
        if column_names is None:
            # if columns are None assume values for every column are passes
            column_names = self.column_names(table_name)
            
        # convert column names to str "col1, col2, col3
        column_str = ''
        for name in column_names:
            column_str += name + ', '
        column_str = column_str[:-2]
   
        query = 'SELECT {columns} FROM {table_name} WHERE '
        query = query.format(table_name = table_name, columns = column_str)
        
        # append multiple conditions
        for tag_name, value in kwargs.items():
            if not(partial_match):
                query += '{tag_name}=? AND '.format(tag_name = tag_name)
            else:
                query += '{tag_name} LIKE ? AND '.format(tag_name = tag_name)
        
        query = query[:-4]  # remove last AND from string
        if sort_by is not None:
            query += ' ORDER BY {0}'.format(sort_by)
        
        values = list(kwargs.values())
        if partial_match:
            values = ['%{}%'.format(value) for value in values]
        result = self.execute(query, values=values , fetch_all=True, close = False)
        

        return result
    
   
    def insert_list(self, table_name, values, column_names = None, close = True):
        """ Insert a  list with values as a SINGLE row. Each value
        must correspond to a column name in column_names. If column_names 
        is None, each value must correspond to a column in the table.
        
        values = (val1, val2, ..)
        columns = (col1, col2, ..)
        """
        
        self.insert_lists(table_name, [values],  column_names=column_names, 
                          close=close)
        
    def insert_lists(self, table_name, values, column_names = None, close = True):
        """ Insert a  list with values as multiple rows. Each value in a row
        must correspond to a column name in column_names. If column_names 
        is None, each value must correspond to a column in the table.
        
        values = ((val1, val2,..),(val3, val4, ..))
        columns = (col1, col2, ..)
        """
        
        if column_names is None:
            # if columns are None assume values for every column are passes
            column_names = self.column_names(table_name)

        # convert to a string: "(value, value2, value3)"
        column_names  = str(tuple(column_names))

        if len(values) == 1:
            values = values[0]
        else:
            values = list(zip(*values))

        # compose query
        cmd = 'INSERT INTO {table}{column_names} VALUES '        
        cmd += self.binding_str(len(values))
        cmd = cmd.format(table = table_name,
                         column_names = column_names)
        
        self.execute(cmd, values = values, close = close)
    
        
    def insert_row_dict(self, table_name, data_dict, close = True):
        """ Insert a dictionary in the table. Dictionary must be as follows:
            datadict = {city: ['Rotterdam, 'Amsterdam',..],
                        streets: ['Blaak', 'Kalverstraat,..],
                        ....}
        """
        columns = list(data_dict.keys())
        values = list(data_dict.values())
        
        self.insert_list(table_name, values, column_names=columns, close = close)

    def delete_rows(self, table_name, column=None, value=None, close = True):
        """ Delete rows from table where column value equals specified value """
        
        cmd = "DELETE FROM {table} WHERE {column}='{value}'"
        cmd = cmd.format(table = table_name, column = column, value = value)
        
        self.execute(cmd, close = close)
        
    def connect(self):
        """Connect to the SQLite3 database."""
        
        if not(self.connected):
            self.connection = lite.connect(self.database)
            self.cursor = self.connection.cursor()
            self.connected = True
            if self.LOG_LEVEL == logging.DEBUG:
                self.connection.set_trace_callback(print)
    
    def close(self):
        """Dicconnect form the SQLite3 database and commit changes."""
        if self.connected:
            self._logger.debug('Closing database connection and committing changes.')

            # traceback.print_stack()
                
            self.connection.commit()
            
            if self.database != self.IN_MEMORY:
                # in memory database will caese to exist upon close
                self.connection.close()
                self.connecion = None
                self.cursor = None
                self.connected = False
        
    def binding_str(self, number):
         binding_str = '('
         for i in range(0, number):
             binding_str+='?,'
         binding_str = binding_str[:-1] # remove trailing ,
         binding_str += ')'
         return binding_str
     



def application_logger(app_name, fname = None, log_level = logging.DEBUG,
                       log_to_console = True, log_format = None):
  """ Create a simple logger object for a specific application name (app_name).
      log_level and log_to_console can be set. Log to file is done when fname
      is a valid filename """
  logger = logging.getLogger(app_name)
  logger.setLevel(log_level)
  logger.handlers = []
  # create file handler which logs even debug messages
  if log_format is None:
      log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
  formatter = logging.Formatter(log_format)

  if not(fname is None):
    fh = logging.FileHandler(fname)
    fh.setLevel(log_level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

  if log_to_console:
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

  return logger

