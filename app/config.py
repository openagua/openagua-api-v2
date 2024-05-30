from os import path, getenv, environ
import datetime as dt
from dotenv import load_dotenv, dotenv_values

load_dotenv('.env')


class Config:
    # Set the default values

    VERSION = dt.datetime.now().strftime('%y.%m.%d')

    APP_NAME = getenv('OA_APP_NAME', 'OpenAgua')
    APP_ROOT = path.dirname(path.abspath(__file__))
    INSTANCE_DIR = path.join(APP_ROOT, 'instance')
    DATA_DIR = path.join(APP_ROOT, 'data')

    ORGANIZATION = None
    SITE_ENCRYPTED = False

    DEBUG = False
    SECRET_KEY = getenv('OPENAGUA_SECRET_KEY', 'a deep, dark secret')
    WTF_CSRF_SECRET_KEY = SECRET_KEY

    # Key needed for server (multi-user) installs for encrypting/decrypting secret db values.
    # For key generation, use `from cryptography.fernet import Fernet`, `key = Fernet.generate_key()`
    SECRET_ENCRYPT_KEY = getenv('SECRET_ENCRYPT_KEY', 'another key')
    DEFAULT_DATABASE_URI = 'sqlite:///{}/openagua.sqlite'.format(DATA_DIR)
    DATABASE_URI = getenv('DATABASE_URI', DEFAULT_DATABASE_URI)

    KEYS_DIR = INSTANCE_DIR
    UPLOADED_FILES_DEST = INSTANCE_DIR

    # Email
    MAIL_API_ENDPOINT = getenv('MAIL_API_ENDPOINT')
    MAIL_API_KEY = getenv('MAIL_API_KEY')
    MAIL_API_TEST_SENDER = getenv('MAIL_API_TEST_SENDER')
    MAIL_API_TEST_RECIPIENT = getenv('MAIL_API_TEST_RECIPIENT')
    MAIL_USERNAME = getenv('MAIL_USERNAME')
    MAIL_PASSWORD = getenv('MAIL_PASSWORD')
    MAIL_SERVER = getenv('MAIL_SERVER')
    MAIL_PORT = getenv('MAIL_PORT', 587)
    MAIL_FROM = getenv('MAIL_FROM')
    MAIL_FROM_NAME = getenv('MAIL_FROM_NAME', 'OpenAgua')

    # Data Server
    DATA_URL = 'base'
    DATA_ROOT_USERNAME = getenv('DATA_ROOT_USERNAME', 'root')
    DATA_ROOT_PASSWORD = ''

    # Other Data Server-related settings
    DATA_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f000Z'  # must be the same as in data.ini
    DATA_SEASONAL_YEAR = 1678  # not used yet
    DEFAULT_SCENARIO_NAME = 'Baseline'
    DEFAULT_SCENARIO_DESCRIPTION = 'Default management option and scenario'

    POST_URL = 'http://127.0.0.1:5000'
    WEBSOCKET_URL = 'ws://127.0.0.1:9000'
    # WAMP_URL = 'ws://xxx.xxx.xxx.xxx:8080/ws'

    MESSAGE_PROTOCOL = 'pubnub'
    # MESSAGE_PROTOCOL = 'socketio'
    PUBNUB_SECRET_KEY = getenv('PUBNUB_SECRET_KEY')
    PUBNUB_SUBSCRIBE_KEY = getenv('PUBNUB_SUBSCRIBE_KEY')
    PUBNUB_PUBLISH_KEY = getenv('PUBNUB_PUBLISH_KEY')
    PUBNUB_UUID = getenv('PUBNUB_UUID')

    AWS_MODEL_KEY_NAME = getenv('AWS_MODEL_KEY_NAME')  # .pem key file used by OpenAgua for EC2 modeling

    HYDROLOGY_API_URL = "http://localhost:8000"
    ADD_REFERENCE_LAYER_URL = "/network/reference_layer"
    OPENAGUA_API_KEY = 'abc'

    SECURITY_PASSWORD_HASH = 'sha256_crypt'
    SECURITY_PASSWORD_SALT = 'salty'

    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Use Amazon Web Services for EC2?
    USE_AWS = False
    CLOUD_COMPUTER_TYPES = []
    AUTOTERMINATE_HOURS = 1

    # Include hydrology - change to True once OpenAgua API is set up
    INCLUDE_HYDROLOGY = False

    # Model running
    # URL passed to the model so it can "phone home" with it's status
    HEARTBEAT_ENT = '/model'
    # socket.io
    NETWORK_ROOM_NAME = '{source_id}-{network_id}'
    RUN_STUDY_ROOM_NAME = '{source_id}-{project_id}'

    # Necessary if install type is server and AWS EC2 machines are used
    AWS_ACCOUNT_ID = '123456789123'
    AWS_MODEL_EC2_USERNAME = 'ec2-user'  # this assumes an Amazon Linux machine
    AWS_DEFAULT_REGION = ''  # e.g., 'us-west-2'
    AWS_ACCESS_KEY_ID = getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = getenv('AWS_SECRET_ACCESS_KEY')
    AWS_MODEL_SECURITY_GROUP = ''  # Security group to SSH to EC2s

    # For map access...
    USE_GOOGLE = True
    PREFERRED_MAP_PROVIDER = 'mapbox'
    GOOGLE_PLACES_API_KEY = getenv('GOOGLE_PLACES_API_KEY')  # https://console.developers.google.com
    MAPBOX_ACCESS_TOKEN = ''  # Mapbox access token

    AWS_S3_BUCKET = getenv('AWS_S3_BUCKET')
    NETWORK_FILES_STORAGE_LOCATION = getenv('NETWORK_FILES_STORAGE_LOCATION', 's3')

    # For Google Earth Engine access
    # If either of these are set to True, must also set up machine-specific GEE credentials
    INCLUDE_GEOPROCESSING = False  # For advanced geoprocessing (not available yet)

    # Google Earth Engine
    EE_SERVICE_ACCOUNT_ID = getenv('EE_SERVICE_ACCOUNT_ID')  # 'my-service-account@...gserviceaccount.com'
    EE_PRIVATE_KEY = getenv('EE_PRIVATE_KEY')  # 'xxxxxxxxxxxxx.json'

    # Charts
    DEFAULT_CHART_RENDERER = 'plotly'

    CORS_ORIGIN = getenv('CORS_ORIGIN')

    RECAPTCHA_SITE_KEY = getenv('RECAPTCHA_SITE_KEY')
    RECAPTCHA_SECRET_KEY = getenv('RECAPTCHA_SECRET_KEY')

    SECURITY_EMAIL_SENDER = (
        getenv('OA_APP_NAME', 'OpenAgua'),
        getenv('SECURITY_EMAIL_SENDER')
    )

    # API Keys, etc.
    MAPBOX_USERNAME = getenv('MAPBOX_USERNAME')
    MAPBOX_DISCOVERY_TILESET_NAME = getenv('MAPBOX_DISCOVERY_TILESET_NAME')
    MAPBOX_CREATION_TOKEN = getenv('MAPBOX_CREATION_TOKEN')
    MAPBOX_DATASET_NAME = getenv('MAPBOX_DATASET_NAME')
    MAPBOX_UPDATE_ENDPOINT = getenv('MAPBOX_UPDATE_ENDPOINT')
    MAPBOX_DATASET_ID = getenv('MAPBOX_DATASET_ID')
    MAPBOX_DISCOVER_MAP = getenv('MAPBOX_DISCOVER_MAP')

    AWS_S3_BUCKET_IMAGES = getenv('AWS_S3_BUCKET_IMAGES')
    AWS_SSH_SECURITY_GROUP = getenv('AWS_SSH_SECURITY_GROUP')
    AMI_ID = getenv('AMI_ID')

    RABBITMQ_HOST = getenv('RABBITMQ_HOST', 'localhost')
    RABBITMQ_DEFAULT_USERNAME = getenv('RABBITMQ_DEFAULT_USERNAME')
    RABBITMQ_DEFAULT_PASSWORD = getenv('RABBITMQ_DEFAULT_PASSWORD')
    RABBITMQ_VHOST = getenv('RABBITMQ_VHOST')

    def __init__(self, mode=None):

        # This will override any default values

        # main .env variables
        for key, value in dotenv_values('.env').items():
            self.__setattr__(key, value)

        _mode = mode or getenv('DEPLOYMENT_MODE', 'production')

        for key, value in dotenv_values(f'.env.{_mode}').items():
            self.__setattr__(key, value)
            environ[key] = value

        if _mode == 'test':
            self.DATABASE_URI = 'sqlite:///./test.db'

        # cleanup
        self.MAIL_PORT = int(self.MAIL_PORT)


config = Config()
