from py_logger import py_logger
from sqlalchemy.engine import create_engine
import pandas 

class ETeller:
    """
        Purpose:    
            Class that handles data movement.

        ARGS: 
            N/A

        Example call:
            from ETeller import ETeller
            ETeller().<method_name>(<method_arg1>, ..., <method_argN>)    
    """

    def __init__(self):
        #Initialize logger
        self.etl_log = py_logger('ETeller', False).py_log()

    def ingest_csv(self, source: str, target: str):
        """
            Purpose:    
                Accepts a CSV file and outputs a Parquet File
            ARGS:
                source (required : string) - CSV file location
                target (required : string) - Folder location to drop the Parquet file
            Returns:
                N/A
        """
        self.etl_log.info(f'Reading passed file {source}')
        src_file_type = source.split('.')[-1]
        if src_file_type != 'csv':
            raise ValueError('Wrong file type dummy')
            
        csv_df = pandas.read_csv(source)

        #set all incoming columns to string type to override pandas assumed column types and avoid errors on incorrect assumption
        cols = list(csv_df)
        csv_df[cols] = csv_df[cols].astype(str)

        self.etl_log.info(f'Writing to {target}')
        csv_df.to_parquet(target.lower())

    def ingest_excel(self, source: str, target: str):
        """
            Purpose:    
                Accepts an Excel file and outputs a Parquet File
            ARGS:
                source (required : string) - Excel file location
                target (required : string) - Folder location to drop the Parquet file
            Returns:
                N/A

            *******STILL NEEDS TO BE TESTED*******
        """

        self.etl_log.info(f'Reading passed file {source}')
        src_file_type = source.split('.')[-1]
        if src_file_type != 'xlsx' or src_file_type != 'xls':
            raise ValueError('Wrong file type dummy')
            
        excel_df = pandas.read_excel(source)

        #set all incoming columns to string type to override pandas assumed column types and avoid errors on incorrect assumption
        cols = list(excel_df)
        excel_df[cols] = excel_df[cols].astype(str)

        self.etl_log.info(f'Writing to {target}')
        excel_df.to_parquet(target.lower())

    def ingest_db(self, query: str, db_url: str, target: str, credentials = None):
        """
            Purpose:    
                Accepts a database connection & SQL query, and outputs to parquet
            ARGS:
                query       (required : string) - Query to extract data from connected DB
                db_url      (required : string) - Database url information needed for SQLAlchemy engine creation
                target      (required : string) - Folder location to drop the Parquet file
                credentials (optional : JSON  ) - JSON file with DB authentication information 
            Returns:
                N/A
        """
        
        if credentials:
            engine = create_engine(db_url, credentials_info = credentials)
        else:
            engine = create_engine(db_url)
        
        self.etl_log.info(f'Querying DB...')
        with engine.connect() as conn, conn.begin():
            db_df = pandas.read_sql_query(query, conn)
        
        cols = list(db_df)
        db_df[cols] = db_df[cols].astype(str)

        self.etl_log.info(f'Writing to {target}')
        db_df.to_parquet(target.lower())
