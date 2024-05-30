import logging

logger = logging.getLogger('openagua')
logger.setLevel(logging.INFO)

logging.getLogger('socketio').setLevel(logging.ERROR)
logging.getLogger('engineio').setLevel(logging.ERROR)


