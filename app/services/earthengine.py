import json
import ee
from app import logger

try:
    key_file = './secrets/ee-private-key.json'
    with open(key_file) as f:
        ee_keys = json.loads(f.read())
    service_account = ee_keys['client_email']
    credentials = ee.ServiceAccountCredentials(service_account, key_file)
    ee.Initialize(credentials)
    logger.info('Earth Engine API initialized')
except Exception as e:
    logger.warning(f'Earth Engine initialization error: {str(e)}')
