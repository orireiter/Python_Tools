import mysql.connector as connector


class MSQL:
    def __init__(self, host_ip: str, username: str, password: str, database_name: str=None):
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
        self.__init__(self.host_ip,self.username,self.password,self.database_name)


    def show_dbs(self):
        try:
            self.cursor.execute("SHOW DATABASES")
        except:
            return(f"ERROR: Either you disconnected, or there are no Databases")
        
        dbs =[]
        for db in self.cursor:
            dbs.append(db[0])
        return({"DBs": dbs})

    def show_tables(self):
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
        sql_line = f"CREATE DATABASE {db_name}"
        try:
            self.cursor.execute(sql_line)
            self.database_name = db_name
            self.reconnect()
            return({'DB': self.database_name})
        except:
            return(f"ERROR: Either you disconnected, or a DB named {db_name} already exists")

    def create_table(self, table_name: str, columns: str):
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
        string = str(lst)
        string = string.replace('[','(')
        string = string.replace(']',')')
        string = string.replace("'","")
        return(string)

    @staticmethod
    def tpl_to_str_brckts(tpl):
        tpl_str = str(tpl)
        tpl_str = tpl_str.replace("'","")
        return tpl_str

    @staticmethod
    def update_record_syntax_help(columns):
        column_n_value_syntax = ''
        for x in range(len(columns)):
            column_n_value_syntax = column_n_value_syntax + str(columns[x]) + r'= %s,'
        column_n_value_syntax=column_n_value_syntax.strip(',')
        return(column_n_value_syntax)