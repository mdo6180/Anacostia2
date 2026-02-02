import logging
import os

class HTMLFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': 'gray',
        'INFO': 'black',
        'WARNING': '#ff9900', 
        'ERROR': 'red',
        'CRITICAL': 'white; background-color: red;'
    }

    def format(self, record):
        color = self.COLORS.get(record.levelname, 'black')
        msg = super().format(record)
        return f'<div style="color: {color}; font-family: monospace; border-bottom: 1px solid #eee; padding: 2px;">{msg}</div>'

def setup_html_logger(log_file="logs.html"):
    logger = logging.getLogger("HtmlLogger")
    logger.setLevel(logging.DEBUG)

    # Check if file exists to decide if we need a header
    file_exists = os.path.isfile(log_file)

    # Change mode to 'a' for Append
    fh = logging.FileHandler(log_file, mode='a')
    formatter = HTMLFormatter('<b>%(asctime)s</b> | [%(levelname)s] | %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    # Only write the HTML header if the file is new
    if not file_exists:
        with open(log_file, 'w') as f:
            f.write("<html><body style='background-color: #f4f4f4; padding: 20px;'><h2>Application Logs</h2>")
    
    return logger

# --- Usage ---
my_logger = setup_html_logger()
my_logger.info("This message will be added to the end of the existing file!")