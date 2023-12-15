import logging
import google.cloud.logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler
from typing import Literal

#=====================================================#
#   Enhancements:
#       1. Add custom logging formats
#
#=====================================================#

class py_logger:
    """
        Purpose:    
            Class that code logging. Needed to segregate logging messages between class objects and main python scripts.

        ARGS: 
        log_name       (required:string)  - the name that will appear on the log messages
        include_stream (optional:boolean) - Option to include streaming logs. Default = FALSE
        log_level      (optional:string)  - Tells the handler what minimum level to log. Default = "info"

        Example call:
            from py_logger import py_logger
            log_obj = py_logger(<log_name>, [True/False], ['debug','info','warning','error','critical']).py_log()
    """

    #Set log level options
    levels = Literal['debug','info','warning','error','critical']

    def __init__(self, log_name: str, include_stream: bool = False, log_level: levels = 'info'):
        #initialize google logging client and object variables
        self.client = google.cloud.logging.Client()
        self.log_name = log_name
        self.filename = log_name + '.log'
        self.include_stream = include_stream
        self.log_level = logging.getLevelName(log_level.upper())
        
    def py_log(self):
        """
            Purpose:    Establishes the logger with the arguments provided by the user
            ARGS:       None
            Returns:    Logging object
        """
        logger = logging.getLogger(self.log_name)
        logger.setLevel(self.log_level)

        #Check if a handler already exists to avoid dupe logging
        if not len(logger.handlers):
            #Set Google Logging Handler
            goog_handler = CloudLoggingHandler(self.client)
            goog_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S'))
            goog_handler.setLevel(self.log_level)
            logger.addHandler(goog_handler)

            if self.include_stream:
                #Set streaming handler info
                strm_handler = logging.StreamHandler()
                strm_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S'))
                strm_handler.setLevel(self.log_level)
                logger.addHandler(strm_handler)

        return logger