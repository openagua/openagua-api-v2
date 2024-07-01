from datetime import datetime
from cryptography.fernet import Fernet

# Derived from flask-security

from os import getenv
import datetime as dt

from passlib.context import CryptContext
import random
import string
import base64
import hashlib
import hmac
from urllib.parse import urlparse
# from contextlib import contextmanager
from datetime import timedelta

from app import config

from fastapi_mail import FastMail, MessageSchema, MessageType, ConnectionConfig
import jwt
from itsdangerous import URLSafeTimedSerializer


def _(text):
    return text


_default_config = {
    'EMAIL_SUBJECT_REGISTER': _('Welcome'),
    'EMAIL_SUBJECT_CONFIRM': _('Please confirm your email'),
    'EMAIL_SUBJECT_PASSWORD_NOTICE': _('Your password has been reset'),
    'EMAIL_SUBJECT_PASSWORD_CHANGE_NOTICE': _(
        'Your password has been changed'),
    'EMAIL_SUBJECT_PASSWORD_RESET': _('Password reset instructions'),
}

SECRET_KEY = getenv('SECRET_KEY')
ACCESS_KEY_ENCRYPT_ALGORITHM = getenv('KEY_ENCRYPT_ALGORITHM', 'HS256')
SECURITY_PASSWORD_HASH = getenv('SECURITY_PASSWORD_HASH')

pwd_context = CryptContext(
    schemes=['bcrypt', 'sha256_crypt'],
    default=SECURITY_PASSWORD_HASH,
    deprecated=['auto']
)

hashing_context = CryptContext(schemes=['sha256_crypt'])

mail_conf = ConnectionConfig(
    MAIL_USERNAME=config.MAIL_USERNAME,
    MAIL_PASSWORD=config.MAIL_PASSWORD,
    MAIL_FROM=config.MAIL_FROM,
    MAIL_PORT=config.MAIL_PORT,
    MAIL_SERVER=config.MAIL_SERVER,
    MAIL_FROM_NAME=config.MAIL_FROM_NAME,
    MAIL_STARTTLS=True,
    MAIL_SSL_TLS=False,
    USE_CREDENTIALS=True,
    TEMPLATE_FOLDER='./app/templates/email'
)


def security_env_value(key):
    return getenv('SECURITY_' + key, _default_config.get(key))


def make_serializer(name):
    secret_key = SECRET_KEY
    # salt = security_env_value(f'{name.upper()}_SALT')
    salt = security_env_value('PASSWORD_SALT')
    return URLSafeTimedSerializer(secret_key=secret_key, salt=salt)


serializers = {key: make_serializer(key) for key in ['confirm', 'login', 'reset']}


# utility functions

def generate_random_alphanumeric_key(k=8):
    x = ''.join(random.choices(string.ascii_letters + string.digits, k=k))
    return x


def hash_api_key(plaintext):
    return hashlib.sha256(plaintext.encode()).hexdigest()


def get_hmac(password):
    salt = getenv('SECURITY_PASSWORD_SALT')
    h = hmac.new(encode_string(salt), encode_string(password), hashlib.sha512)
    return base64.b64encode(h.digest())


def hash_password(password):
    password = get_hmac(password)
    return pwd_context.hash(password)


def verify_password(password, hashed_password):
    password = get_hmac(password)
    return pwd_context.verify(password, hashed_password)


def create_access_token(email: str):
    payload = {
        'exp': dt.datetime.utcnow() + dt.timedelta(minutes=15),
        'iat': dt.datetime.utcnow(),
        'sub': email
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ACCESS_KEY_ENCRYPT_ALGORITHM)


def create_refresh_token(email: str):
    payload = {
        'exp': dt.datetime.utcnow() + dt.timedelta(days=7),
        'sub': email
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ACCESS_KEY_ENCRYPT_ALGORITHM)


def decode_access_token(token: str) -> dict:
    return jwt.decode(token, SECRET_KEY, algorithms=ACCESS_KEY_ENCRYPT_ALGORITHM)


def decode_refresh_token(token: str) -> dict:
    return jwt.decode(token, SECRET_KEY, algorithms=ACCESS_KEY_ENCRYPT_ALGORITHM)


# def verify_and_update_password(password, user):
#     """Returns ``True`` if the password is valid for the specified user.
#
#     Additionally, the hashed password in the database is updated if the
#     hashing algorithm happens to have changed.
#
#     :param password: A plaintext password to verify
#     :param user: The user to verify against
#     """
#     if use_double_hash(user.password):
#         verified = _pwd_context.verify(get_hmac(password), user.password)
#     else:
#         # Try with original password.
#         verified = _pwd_context.verify(password, user.password)
#
#     if verified and _pwd_context.needs_update(user.password):
#         user.password = hash_password(password)
#         _datastore.put(user)
#     return verified


def encode_string(s: str):
    return s.encode('utf-8') if isinstance(s, str) else s


def hash_data(data):
    return hashing_context.hash(encode_string(data))


def verify_hash(hashed_data, compare_data):
    return hashing_context.verify(encode_string(compare_data), hashed_data)


def get_within_delta(key):
    txt = security_env_value(key)
    values = txt.split()
    return timedelta(**{values[1]: int(values[0])})


def get_max_age(key):
    td = get_within_delta(key + '_WITHIN')
    return td.seconds + td.days * 24 * 3600


async def send_mail(subject: str, email: str, template: str, template_body: dict):
    message = MessageSchema(
        subject=subject,
        recipients=[email],
        template_body=template_body,
        subtype=MessageType.html,
    )

    fm = FastMail(mail_conf)
    result = await fm.send_message(message, template_name=f'{template}.html')
    return result


# def get_identity_attributes():
#     attrs = app.config['SECURITY_USER_IDENTITY_ATTRIBUTES']
#     try:
#         attrs = [f.strip() for f in attrs.split(',')]
#     except AttributeError:
#         pass
#     return attrs


# def use_double_hash(password_hash=None):
#     """Return a bool indicating whether a password should be hashed twice."""
#     single_hash = security_env_value('PASSWORD_SINGLE_HASH')
#     if single_hash and _security.password_salt:
#         raise RuntimeError('You may not specify a salt with '
#                            'SECURITY_PASSWORD_SINGLE_HASH')
#
#     if password_hash is None:
#         is_plaintext = _security.password_hash == 'plaintext'
#     else:
#         is_plaintext = _pwd_context.identify(password_hash) == 'plaintext'
#
#     return not (is_plaintext or single_hash)


# @contextmanager
# def capture_passwordless_login_requests():
#     login_requests = []
#
#     def _on(app, **data):
#         login_requests.append(data)
#
#     login_instructions_sent.connect(_on)
#
#     try:
#         yield login_requests
#     finally:
#         login_instructions_sent.disconnect(_on)


# @contextmanager
# def capture_registrations():
#     """Testing utility for capturing registrations.
#
#     :param confirmation_sent_at: An optional datetime object to set the
#                                  user's `confirmation_sent_at` to
#     """
#     registrations = []
#
#     def _on(app, **data):
#         registrations.append(data)
#
#     user_registered.connect(_on)
#
#     try:
#         yield registrations
#     finally:
#         user_registered.disconnect(_on)


# @contextmanager
# def capture_reset_password_requests(reset_password_sent_at=None):
#     """Testing utility for capturing password reset requests.
#
#     :param reset_password_sent_at: An optional datetime object to set the
#                                    user's `reset_password_sent_at` to
#     """
#     reset_requests = []
#
#     def _on(app, **data):
#         reset_requests.append(data)
#
#     reset_password_instructions_sent.connect(_on)
#
#     try:
#         yield reset_requests
#     finally:
#         reset_password_instructions_sent.disconnect(_on)


# =======

# def encode_string(string):
#     if isinstance(string, str):
#         string = string.encode('utf-8')
#     return string


# REGISTRATION CONFIRMATION

def generate_confirmation_link(user, origin):
    token = generate_confirmation_token(user)
    parsed_origin = urlparse(origin)
    origin = '{}/{}/confirm'.format(parsed_origin.scheme, parsed_origin.netloc)
    url = '{}?token={}'.format(origin, token)
    return url, token


async def send_confirmation_instructions(origin, user):
    confirmation_link, token = generate_confirmation_link(user, origin)
    body = dict(user=user, confirmation_link=confirmation_link)
    await send_mail(security_env_value('EMAIL_SUBJECT_CONFIRM'), user.email,
                    'confirmation_instructions', body)


def generate_confirmation_token(user):
    """Generates a unique confirmation token for the specified user.

    :param user: The user to work with
    """
    data = [str(user.id), hash_data(user.email)]
    return serializers['confirm'].dumps(data)


async def send_password_reset_notice(user):
    body = dict(user=user)
    await send_mail(security_env_value('EMAIL_SUBJECT_PASSWORD_NOTICE'), user.email, 'reset_notice', body)


def generate_reset_password_token(user):
    password_hash = hash_data(user.password) if user.password else None
    data = [str(user.id), password_hash]
    return serializers['reset'].dumps(data)


# CHANGE PASSWORD

async def send_password_changed_notice(user):
    subject = security_env_value('EMAIL_SUBJECT_PASSWORD_CHANGE_NOTICE')
    body = dict(user=user)
    await send_mail(subject, user.email, 'change_notice', body)


def encrypt(text):
    f = Fernet(config.SECRET_ENCRYPT_KEY)
    return f.encrypt(str.encode(text)).decode()


def decrypt(ciphertext, key):
    key = key
    f = Fernet(key)
    try:
        try:
            txt = f.decrypt(ciphertext).decode()
        except:
            txt = f.decrypt(bytes(ciphertext, 'utf-8')).decode()
    except:
        txt = None
    return txt


def get_utc():
    return int((datetime.utcnow() - datetime(1970, 1, 1, 0, 0, 0, 0)).total_seconds())
