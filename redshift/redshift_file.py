from IPython.core.magic import (Magics, register_line_magic, magics_class, line_magic,
                                cell_magic, line_cell_magic, needs_local_scope)

try:
    from traitlets.config.configurable import Configurable
except ImportError:
    from IPython.config.configurable import Configurable

import sqlalchemy
import psycopg2
from pandas import DataFrame


# The class MUST call this class decorator at creation time
@magics_class
class Redshift(Magics, Configurable):

    def __init__(self, shell): # this is same for all magic extensions
        Configurable.__init__(self, config=shell.config)
        Magics.__init__(self, shell=shell)

        # Add ourself to the list of module configurable via %config
        self.shell.configurables.append(self)
        self.connection, self.engine = None, None
        self.username, self.password, self.account = None, None, None
        
    @needs_local_scope
    @cell_magic('redshift_write')
    def execute_write(self, line, cell=''):
        if len(line) < 1: 
            return "No pandas dataframe name specified"
        if len(cell) < 1:
            return "No table name to be made specified"
        dataframe = self.shell.user_ns.get(line) # get the data in pandas df here
        schema, table_name= cell.split(".") 
        try:
            dataframe.to_sql(table_name, self.engine, schema=schema, index=False)
            return f"Table {schema+'.'+table_name} has been created" 
        except Exception as e:
            return e
       
    @needs_local_scope
    @line_magic('redshift')
    @cell_magic('redshift')
    def execute(self, line, cell='', local_ns={}):        
        if len(line.split(" ")) > 4: # check if credentials have been passed or not
            line.strip() # remove unwanted spaces on either ends
            self.host, self.port, self.db, self.username, self.password = line.split(" ") 
            try:
                self.connection.close() # checking if connection previously established
                self.engine.displose() # if yes then close it and create new
            except:
                pass
            self.engine = sqlalchemy.create_engine(f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.db}") # this has been addded to enable writing back to redshift from pandas
            return "Connection string has been stored"
            
        if len(cell) > 0:            
            try:
                self.connection=psycopg2.connect(dbname= self.db, host=self.host ,port= self.port, user= self.username, password= self.password) # establishing connection each time as if only once done then after any error can't query further
                print("Querying Database")
                results = []
                if ';' not in cell: # if query not terminated by semicolon then we add it
                    cell = cell + ';'
                    
                for cmd in cell.split(";")[:-1]: # take all queries except last one as it would be blank when separated by ;
                    try:
                        cur = self.connection.cursor() # curson defined 
                        cur.execute(cmd) # query executed
                    except Exception as e:
                        return e
                    try:
                        result = DataFrame(cur.fetchall()) # if query returned results then this will give back all at once
                        colnames = [desc[0] for desc in cur.description] # to get the column headers
                        result.columns = colnames
                        results.append(result)    # result of every query appended to main object
                        
                    except psycopg2.ProgrammingError: # this will happen when create table or insert command executed as nothing to returned
                        print("Committing")
                        self.connection.commit() # they need the transaction to be committed
                        results.append("Committed")
                        pass
                    
                    except Exception as e: # in any other exception we just return it  
                        return e
                if len(line) > 0: # if variable specified where results to be writted
                    self.shell.user_ns.update({line: DataFrame(results[-1])}) # creating variable with results
                    return f"Result present in pandas dataframe - {line}"
            finally:
                try:
                    cur.close()
                except Exception as e:
                    return "Recheck the details passed as connection string" # occurs when not able to connect
                self.connection.close()
            return results[-1] # simply returns the results        
        else:
            return "Nothing to query" # when no query specified
        
    def __del__(self):
        self.connection.close()
        self.engine.displose()