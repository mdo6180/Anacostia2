from logging import Logger
import os

from utils.connection import ConnectionManager



class Producer:
    def __init__(self, name: str, directory: str, logger: Logger = None):
        self.name = name
        self.logger = logger
    
        self.directory = directory
        if os.path.exists(directory) is False:
            self.logger.info(f"Directory {directory} does not exist. Creating it.")
            os.makedirs(directory)
        
    def initialize_db_connection(self, filename: str):
        self.conn_manager = ConnectionManager(db_path=filename, logger=self.logger)

    def write(self, filename: str, content: str):
        path = os.path.join(self.directory, filename)
        with open(path, "a") as file:
            file.write(content)
        self.logger.info(f"{self.name} produced artifact: {path}")    # produced_artifact DB call in future