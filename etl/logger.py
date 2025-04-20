import logging 

def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        # force=True  # ensures clean reconfiguration when reloaded
    )