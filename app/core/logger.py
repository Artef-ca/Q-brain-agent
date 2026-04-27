import logging
import os
from pythonjsonlogger import jsonlogger

# Try to get project ID from environment, or fallback to your default
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "prj-ai-dev-qic")

class GCPLogFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(GCPLogFormatter, self).add_fields(log_record, record, message_dict)
        
        log_record['severity'] = record.levelname
        
        # Look for trace_id passed in the 'extra' dict
        trace_id = getattr(record, 'trace_id', None)
        if trace_id:
            clean_trace_id = str(trace_id).replace("-", "")
            log_record['logging.googleapis.com/trace'] = f"projects/{PROJECT_ID}/traces/{clean_trace_id}"
            log_record.pop('trace_id', None)

def get_gcp_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    
    # Prevent adding multiple handlers if the logger is requested multiple times
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = GCPLogFormatter('%(asctime)s %(levelname)s %(name)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger