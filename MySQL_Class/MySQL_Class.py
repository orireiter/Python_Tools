import mysql.connector as connector


class MSQL:
    '''
        A class to represent a connection to MySQL server. 
        Allowing easier use of SQL queries with the same object.
    
        Attributes
        ----------
        host_ip: str
            Hostname of the mysql server you'll communicate with.
        username: str
            Username you will execute your queries with.
        password: str
            Password (for the username) you will execute your queries with.
        database_name: str(optional)
            A database to query.
        cursor: mysql cursor object
            An object relying on the connection made, that executes the queries.

        Methods 
        -------
        reconnect():
            Simply runs the init function again.
            that means it attempts reconnecting the object to the server.
        show_dbs():
            Returns a list of the databases in the server.
        show_tables():
            Returns a list of the tables in the database set in self.database_name .
        show_columns(table_name):
            Returns a list of the columns in the table in the database set in self.database_name .
        create_db(db_name):
            Creates a database with the name given. Also sets in the self.database_name, 
            in case it's needed for a sort of db initialization script.
        create_table(table_name: str, columns: str):
            Creates a table with he given name and columns specified, 
            in the database set in self.database_name .
        insert_record(table_name: str, columns: tuple, values: tuple):
            Inserts a record to the given database, table and fills
            the record's values according the values.
        delete_record(table_name: str, record_id: str):
            Deletes a record with the given ID from the database and table specified.
        find_record(table_name: str, record_id: str):
            Finds a record with the ID specified inside the database+db given.
        find_records(table_name: str, columns: tuple, values: tuple):
            Finds a record(s) in the database+table specified, 
            by the criteria of the column(s)/value(s).
        update_record(table_name: str, columns: tuple, values: tuple, record_id: str):
            Updates in the DB/Table given, in a record found by the ID given, and updates 
            the columns specified with the values specified.
        lst_to_str_brckts(lst):
            helps reformat strings for the queries
        tpl_to_str_brckts(tpl):
            helps reformat strings for the queries
        update_record_syntax_help(columns):
            helps reformat strings for the queries
    '''
    def __init__(self, host_ip: str, username: str, password: str, database_name: str=None):
        '''
            Upon initialization, the object will connect to the server,
            either with or without a database specified, 
            and create a cursor used for the queries.
            
            Parameters
            ----------
            host_ip: str
                Hostname of the mysql server you'll communicate with.
            username: str
                Username you will execute your queries with.
            password: str
                Password (for the username) you will execute your queries with.
            database_name: str(optional)
                A database to query.

            Raises
            ------
            If a connection can't be established, an excpetion will be raised.
        '''
        self.host_ip, self.username, self.password, self.database_name = host_ip, username, password, database_name
        try:  
            if self.database_name == None:
                self.connection = connector.connect(
                    host=self.host_ip,
                    user=self.username,
                    password=self.password
                )

            else:
                self.connection = connector.connect(
                    host=self.host_ip,
                    user=self.username,
                    password=self.password,
                    database=self.database_name
                )
            self.cursor = self.connection.cursor()

        except:
            raise Exception("ERROR: could not connect to database,\n check internet connectivity and/or credenetials")


    def reconnect(self):
        '''
            Reruns the init (reconnects to the server, good use case is 
            if midway you need to change details of connection but can't change object)
        '''
        self.__init__(self.host_ip,self.username,self.password,self.database_name)


    def show_dbs(self):
        '''
            Returns a list of the databases in the server.

            Raises
            ------
            If the cursor can't for some reason execute the query,
            an error will be returned (usually will happen due to disconnection.)
        '''
        try:
            self.cursor.execute("SHOW DATABASES")
        except:
            return(f"ERROR: Either you disconnected, or there are no Databases")
        
        dbs =[]
        for db in self.cursor:
            dbs.append(db[0])
        return({"DBs": dbs})

    def show_tables(self):
        '''
            Returns a list of tables in your database.

            Raises
            ------
            If a connection can't be established, an error will be returned.
            If no db is specified in self.database_name, an error will be returned.
        '''
        if self.database_name == None:
            return("ERROR: No Database Defined, either add it upon initializing the object,\nOr by self.database_name='example' \nAnd apply it by executing reconnect()")
        else:
            try:
                sql_line = "SHOW TABLES"
                self.cursor.execute(sql_line)
            except:
                return(f"ERROR: Either you disconnected, or there are no tables in db: {self.database_name}")

            tables = []
            for table in self.cursor:
                tables.append(table[0])
            return({"DB": self.database_name, "Tables":tables})

    def show_columns(self, table_name: str):
        '''
            Returns a dict with the columns and the type of values they expect.

            Parameters
            ----------
            table_name: str
                A table to query.

            Raises
            ------
            If a connection can't be established, an error will be returned.
            If no db is specified in self.database_name, an error will be returned.
        '''
        if self.database_name == None:
            return("ERROR: No Database Defined, either add it upon initializing the object,\nOr by self.database_name='example' \nAnd apply it by executing reconnect()")
        else:
            try:
                sql_line = f"SHOW Columns in {table_name}"
                self.cursor.execute(sql_line)
            except:
                return(f"ERROR: Either you disconnected, or table: {table_name} doesn't exist")
            
            columns = {}
            for column in self.cursor:
                columns[column[0]] = column[1]
            return({"DB": self.database_name, "Table": table_name, "Columns": columns})


    def create_db(self, db_name: str):
        '''
            Creates a DB with the name given.
            Then assigns it to self.database_name

            Parameters 
            ----------
            db_name: str
                A DB to create.

            Raises
            ------
            If a connection can't be established, an error will be returned.
        '''
        sql_line = f"CREATE DATABASE {db_name}"
        try:
            self.cursor.execute(sql_line)
            self.database_name = db_name
            self.reconnect()
            return({'DB': self.database_name})
        except:
            return(f"ERROR: Either you disconnected, or a DB named {db_name} already exists")

    def create_table(self, table_name: str, columns: str):
        '''
            Creates a table with the name and columns given,
            Example of use:
            MSQL_object.create_table('test_table', '(id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), age INT)')

            Parameters 
            ----------
            table_name: str
                A table to create.
            columns: str
                contains the columns to create, and the types they expect.

            Raises
            ------
            If a connection can't be established, an error will be returned.
            If no DB is specified, an error will be returned.
        '''
        if self.database_name == None:
            return("ERROR: No Database Defined, either add it upon initializing the object,\nOr by self.database_name='example' \nAnd apply it by executing reconnect()")
        else:
            try:
                sql_line = f"CREATE TABLE {table_name} {columns}"
                self.cursor.execute(sql_line)
                return ({"DB": self.database_name, "Table": table_name})
            except:
                return(f"ERROR: Either you disconnected, or a table named {table_name} already exists, or the columns were given in an incorrect format.\nExample of use:\nMSQL_object.create_table('test_table', '(id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), age INT)')")


    def insert_record(self, table_name: str, columns: tuple, values: tuple):
        '''
            Inserts a record to table given, fills the supplied columns with the supplied values.
            Reformats the strings with help of the static methods.
            Return its ID.
            Example of use:
            insert_record('test_table', ('name', 'address'), ('ezra', 'jerusalem')) 

            OR 
            insert_record('test_table', ('name',), ('gabi',))

            Parameters 
            ----------
            table_name: str
                A table to insert to.
            columns: tuple
                Contains the columns to fill.
            values: tuple
                Contains the values to in correlation to the values.

            Raises
            ------
            If a connection can't be established, an error will be returned.
            If no DB is specified, an error will be returned.
            If the columns parameters ins't set correctly, an error will be returned.
        '''
        if self.database_name == None:
            return("ERROR: No Database Defined, either add it upon initializing the object,\nOr by self.database_name='example' \nAnd apply it by executing reconnect()")
        else:    
            try:
                column_string = MSQL.tpl_to_str_brckts(columns)
                if len(columns) == 1: 
                    column_string = column_string.replace(",","")
            except:
                return("ERROR: couldnt format the columns argument, it should look like so:\ninsert_record(columns=('age', 'name'))\nOR\ninsert_record(columns=('age',))")
            
            param_list = []
            for _ in range(len(columns)):
                param_list.append(r"%s")

            param_string = MSQL.lst_to_str_brckts(param_list)

            try:
                sql_line = f"INSERT INTO {table_name} {column_string} VALUES {param_string}"
                self.cursor.execute(sql_line, values)
                self.connection.commit()
                return {"id": str(self.cursor.lastrowid)}
            except:
                return(f"ERROR: Either you disconnected, or a table named {table_name} doesn't exist, or the values/parameters don't match the table\nExample of use:\ninsert_record('test_table', ('name', 'address'), ('ezra', 'jerusalem'))\nOR\ninsert_record('test_table', ('name',), ('gabi',))")

    def delete_record(self, table_name: str, record_id: str):
        '''
            Deletes a record from table given, according to the ID sent.
            Return its ID.
            
            Example of use:
            delete_record('test_table', '12345')

            Parameters 
            ----------
            table_name: str
                A table to delete from.
            record_id: str
                The ID of the record to delete.

            Raises
            ------
            If a connection can't be established, an error will be returned.
            If no DB is specified, an error will be returned.
        '''
        if self.database_name == None:
            return("ERROR: No Database Defined, either add it upon initializing the object,\nOr by self.database_name='example' \nAnd apply it by executing reconnect()")
        else:
            try:
                sql_line = f"delete from {table_name} where id=%s"
                value = (record_id,)

                self.cursor.execute(sql_line, value)
                self.connection.commit()
                return({"id": record_id})
            except:
                return(f"ERROR: Either you disconnected, or a table named {table_name} doesn't exist\nExample of use:\ndelete_record('test_table', 12345)")

    def find_record(self, table_name: str, record_id: str):
        '''
            Finds and returns a record from table given, according to the ID sent.

            Example of use:
            find_record('test_table', '12345')

            Parameters 
            ----------
            table_name: str
                A table to get a record from.
            record_id: str
                The ID of the record to find.

            Raises
            ------
            If a connection can't be established, an error will be returned.
            If no DB is specified, an error will be returned.
        '''
        if self.database_name == None:
            return("ERROR: No Database Defined, either add it upon initializing the object,\nOr by self.database_name='example' \nAnd apply it by executing reconnect()")
        else:
            try:
                sql_line = f"select * from {table_name} where id=%s"
                value = (record_id,)

                self.cursor.execute(sql_line, value)
                result = self.cursor.fetchone()
                return({"Record": result})
            except:
                return(f"ERROR: Either you disconnected, or a table named {table_name} doesn't exist\nExample of use:\nfind_record('test_table', 12345)")

    def find_records(self, table_name: str, column: str, value: str):
        '''
            Finds and returns a record(s) from table given, according to the columns/values given.

            Example of use:
            find_records('test_table', 'name', 'john')

            Parameters 
            ----------
            table_name: str
                A table to delete from.
            column: str
                A column to query by.
            value: str
                The value the column should be, to query by,

            Raises
            ------
            If a connection can't be established, an error will be returned.
            If no DB is specified, an error will be returned.
        '''
        if self.database_name == None:
            return("ERROR: No Database Defined, either add it upon initializing the object,\nOr by self.database_name='example' \nAnd apply it by executing reconnect()")
        else:
            try:
                sql_line = f"select * from {table_name} where {column}=%s"
                value = (value,)

                self.cursor.execute(sql_line, value)

                result = self.cursor.fetchall()
                return({"Records": result})
            except:
                return(f"ERROR: Either you disconnected, or a table named {table_name} doesn't exist, or the columns given don't exist\nExample of use:\nfind_records('test_table', 'name', 'john')")

    def update_record(self, table_name: str, columns: tuple, values: tuple, record_id: str):
        '''
            Update a record from table given, according to the columns/values given.
            Choose the record to update by the id specified,
            Uses static methods to reformat strings to fit an SQL query.
            Return the ID.

            Example of use:
            update_record('test_table',('name', 'address'), ('bob', 'new york'),'12345')
            
            OR
            insert_record('test_table', ('name',), ('gabi',), '1234')

            Parameters 
            ----------
            table_name: str
                A table to delete from.
            column: tuple
                Columns to update.
            value: tuple
                The value the column should be changed to.
            record_id: str
                Used to find which record to update.

            Raises
            ------
            If a connection can't be established, an error will be returned.
            If no DB is specified, an error will be returned.
        '''
        if self.database_name == None:
            return("ERROR: No Database Defined, either add it upon initializing the object,\nOr by self.database_name='example' \nAnd apply it by executing reconnect()")
        else:
            try:
                values_altered = MSQL.update_record_syntax_help(columns)
                sql_line = f'update {table_name} set ' + values_altered + r' where id = %s'
                values = values + (record_id,)

                self.cursor.execute(sql_line, values)
                self.connection.commit()
                return({"id": record_id})
            except:
                return(f"ERROR: Either you disconnected, or a table named {table_name} doesn't exist, or the values/parameters don't match the table\nExample of use:\nupdate_record('test_table',('name', 'address'), ('bob', 'new york'),'12345')\nOR\ninsert_record('test_table', ('name',), ('gabi',), '1234')")


    @staticmethod
    def lst_to_str_brckts(lst):
        '''
            Takes a list and returns a string with normal brackets instead, 
            dropping the quotings.
            ['bob','john'] => '(bob,john)'
        '''
        string = str(lst)
        string = string.replace('[','(')
        string = string.replace(']',')')
        string = string.replace("'","")
        return(string)

    @staticmethod
    def tpl_to_str_brckts(tpl):
        '''
            Takes a tuple and returns string with brackets.
            ('bob', 'john') => '(bob, john)'
        '''
        tpl_str = str(tpl)
        tpl_str = tpl_str.replace("'","")
        return tpl_str

    @staticmethod
    def update_record_syntax_help(columns):
        '''
            Helps formating a string to SQL syntax.
            Creates the name=%s,address=%s and so on pattern.
        '''
        column_n_value_syntax = ''
        for x in range(len(columns)):
            column_n_value_syntax = column_n_value_syntax + str(columns[x]) + r'= %s,'
        column_n_value_syntax=column_n_value_syntax.strip(',')
        return(column_n_value_syntax)