from os import getenv
from urllib.parse import quote as urlencode
import json

import ee
from app import logger

try:
    client_email = getenv("EE_CLIENT_EMAIL")
    parsed_client_email = urlencode(client_email)
    private_key = getenv("EE_PRIVATE_KEY").replace('\\n', '\n')
    key_data = {
        "type": "service_account",
        "project_id": getenv("EE_PROJECT_ID"),
        "private_key_id": getenv("EE_PRIVATE_KEY_ID"),
        "private_key": private_key,
        "client_email": client_email,
        "client_id": getenv("EE_CLIENT_ID"),
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{parsed_client_email}",
        "universe_domain": "googleapis.com"
    }
    credentials = ee.ServiceAccountCredentials(client_email, key_data=json.dumps(key_data))
    ee.Initialize(credentials)
    logger.info('Earth Engine API initialized')
except Exception as e:
    logger.warning(f'Earth Engine initialization error: {str(e)}')
