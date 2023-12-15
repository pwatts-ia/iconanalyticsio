from google.cloud import storage
from py_logger import py_logger
from ETeller import ETeller

def  ingest_to_datalake(request):
    """
        Purpose:    
            Converts a CSV from a source location into a Parquet file stored in an IDP bucket. The folder structure after the source_loc
            bucket name will be maintained under the target_bucket location. If the target_bucket does not exist within IDP storage 
            it will be created.

        ARGS: 
            request (JSON): A JSON object that contains the parameters needed for this function to execute 
                * File Ingestions:
                    "source_loc"    (required : string) - The source location housing the files needed to convert into parquet
                    "target_loc"    (required : string) - The target location to store the converted parquet file within IDP's data lake
                * DB Ingestions:
                    "query"         (required : string) - Query to extract data from connected DB
                    "db_url"        (required : string) - Database url information needed for SQLAlchemy engine creation
                    "target_loc"    (required : string) - Folder location to drop the Parquet file
                    "credentials"   (optional : JSON  ) - JSON file with DB authentication information

        Return:
            A success message

        Enhancements:
            - Error handle bad records

        Example call:
            gcloud functions call ingest-to-datalake --data '{"source_loc":"<source_bucket_name>/<data_folder(s)>", "target_loc":"<target_bucket_name>"}'    
            gcloud functions call ingest-to-datalake --data '{"query":"<sql>", "db_url":"<url>", "target_loc":"<target_bucket_name>", "credentials":"<file_location>"}'
    """
    
    #establish logger & storage client, and Global variables
    py_log = py_logger('main', False, 'debug').py_log()
    blob_depot = storage.Client()
    gcs_uri = 'gs://'

    #Validate incoming parameters from the request arg. If valid parameters, assign their values to variables.
    content_type = request.headers["content-type"]
    if content_type == "application/json":
        request_json = request.get_json(silent=True)

        #Check if request is for flat source files
        if request_json and "source_loc" and "target_loc" in request_json:
            ingest_type = 'file'
            if len(request_json["source_loc"].split('/')) > 1:
                source_bucket = request_json["source_loc"].split('/', 1)[0]
                source_folder = request_json["source_loc"].split('/', 1)[1]
            else:
                source_bucket = request_json["source_loc"]
                source_folder = ''

            if len(request_json["target_loc"].split('/')) > 1:
                target_bucket = request_json["target_loc"].split('/', 1)[0]
                target_folder = request_json["target_loc"].split('/', 1)[1]
            else:
                target_bucket = request_json["target_loc"]
                target_folder = ''  

        #Check if request is for a DB connection
        elif request_json and "query" and "db_url" and "target_loc" in request_json:
            ingest_type = 'db'
            query = request_json["query"]
            db_url = request_json["db_url"]

            if len(request_json["target_loc"].split('/')) > 1:
                target_bucket = request_json["target_loc"].split('/', 1)[0]
                target_folder = request_json["target_loc"].split('/', 1)[1]
            else:
                target_bucket = request_json["target_loc"]
                target_folder = ''

            if "credentials" in request_json:
                credentials = request_json["credentials"]
            else:
                credentials = None
            
        else:
            py_log.error('Parameters were not properly configured.')
            py_log.error
            ("""
                ARGS: 
                    request (JSON/Dict): A JSON object that contains the parameters needed for this function to execute 
                        * File Ingestions:
                            "source_loc"    (required:string) - The source location housing the files needed to convert into parquet
                            "target_loc"    (required:string) - The target location to store the converted parquet file within IDP's data lake
                        * DB Ingestions:
                            "query"         (required : string) - Query to extract data from connected DB
                            "db_url"        (required : string) - Database url information needed for SQLAlchemy engine creation
                            "target_loc"    (required : string) - Folder location to drop the Parquet file
                            "credentials"   (optional : JSON  ) - JSON file with DB authentication information
            """)
            raise ValueError()
    else:
        py_log.error(f'Parameter content type unrecognized - {content_type}')
        raise ValueError()
    
    py_log.info('Parameters aqcuired, resuming...')

    if ingest_type == 'file':
        #Loop through files in the designated source_loc, then output to parquet in target_bucket.
        for blob in blob_depot.list_blobs(source_bucket, prefix = source_folder, delimiter = '/'):
            #Determine file type to send to ETeller
            src_file_type = blob.split('.')[-1]
            
            if src_file_type == 'csv':
                py_log.info(f'sending blob, {blob.name} to ETeller().ingest_csv...')
                ETeller().ingest_csv(gcs_uri + source_bucket + '/' + blob.name, gcs_uri + target_bucket + '/' + target_folder + '/' + blob.name.split('/')[-1].replace('.csv', '.parquet').lower())  

            elif src_file_type == 'xlsx' or src_file_type == 'xls':
                py_log.info(f'sending blob, {blob.name} to ETeller().ingest_excel...')
                ETeller().ingest_excel(gcs_uri + source_bucket + '/' + blob.name, gcs_uri + target_bucket + '/' + target_folder + '/' + blob.name.split('/')[-1].replace('.csv', '.parquet').lower())  

    elif ingest_type == 'db':
        #tbl_name used for output parquet file naming.
        tbl_name = query[query.lower().find('from ') + 5 : query[query.lower().find('from ') + 5:].find(' ') + query.lower().find('from ') + 5]

        py_log.info(f'Sending DB ingestion to ETeller().ingest_db...')
        ETeller().ingest_db(query=query, db_url=db_url, credentials=credentials, target=gcs_uri + target_bucket + '/' + target_folder + '/' + tbl_name)

    return f'Parquet file(s) uploaded to {target_bucket}/{target_folder}'
    