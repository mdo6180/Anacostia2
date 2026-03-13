from logging import Logger
import os
import shutil

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.


class FileSystemTransport:
    def __init__(self, name: str, dest_directory: str, dest_stream_name: str, logger: Logger = None):
        self.name = name
        self.logger = logger
        self.dest_stream_name = dest_stream_name

        self.dest_directory = dest_directory
        if os.path.exists(dest_directory) is False:
            os.makedirs(dest_directory)
    
    def send(self, filepath: str) -> None:
        filename = os.path.basename(filepath)
        dest_path = os.path.join(self.dest_directory, filename)
        
        # the protocol to actually send the artifact
        shutil.copy2(filepath, dest_path)
