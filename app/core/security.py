# Derived from flask-security

from os import environ as env

import datetime as dt

from itsdangerous import BadSignature, SignatureExpired
from sqlalchemy.exc import NoResultFound

from app.core.users import add_user, get_user, get_user_by_email
from app.core.hydra import root_connection
from app.models import User, APIKey

from app.core.utils import generate_random_alphanumeric_key, hash_api_key, serializers, get_max_age, \
    generate_confirmation_link, send_mail, verify_hash, security_env_value, generate_reset_password_token, \
    hash_password, send_password_changed_notice


def generate_api_key(db):
    """
    This follows the basic API key approach described in:
    https://www.freecodecamp.org/news/best-practices-for-building-api-keys-97c26eabfea9/
    :return: The newly generated, unhashed key.
    """
    # generate key

    while True:
        key_prefix = generate_random_alphanumeric_key(k=6)
        key_base = generate_random_alphanumeric_key(k=33)
        user_key = '.'.join([key_prefix, key_base])
        hashed_key_base = hash_api_key(key_base)
        db_key = '.'.join([key_prefix, hashed_key_base])

        try:
            db.query(APIKey).filter_by(id=db_key).one()
        except Exception as err:
            break

    return user_key, db_key


def verify_api_key(db, key):
    try:
        if '.' in key:
            key_prefix, key_base = key.split('.')
            hashed_key_base = hash_api_key(key_base)
            db_key = '.'.join([key_prefix, hashed_key_base])
            apikey = db.query(APIKey).get(db_key)
            if not apikey:
                return None
            user_id = apikey.user_id
            user = db.query(User).filter_by(id=user_id).one()
        elif key is not None and key == env.get('TEST_API_KEY'):
            test_username = env.get('TEST_USERNAME')
            user = db.query(User).filter_by(email=test_username).one()
        else:
            raise Exception('Invalid API key')

    except ValueError:
        raise Exception('Invalid API key')  # no dot (.) in key
    except NoResultFound:
        raise Exception('User not found')
    return user


def get_token_status(db, token, serializer, max_age=None, return_data=False):
    """Get the status of a token.

    :param token: The token to check
    :param serializer: The name of the seriailzer. Can be one of the
                       following: ``confirm``, ``login``, ``reset``
    :param max_age: The name of the max age config option. Can be on of
                    the following: ``CONFIRM_EMAIL``, ``LOGIN``,
                    ``RESET_PASSWORD``
    """
    serializer = serializers[serializer]
    max_age = get_max_age(max_age)
    user, data = None, None
    expired, invalid = False, False

    try:
        data = serializer.loads(token, max_age=max_age)
    except SignatureExpired:
        d, data = serializer.loads_unsafe(token)
        expired = True
    except (BadSignature, TypeError, ValueError):
        invalid = True

    if data:
        user = get_user(db, data[0])

    expired = expired and (user is not None)

    if return_data:
        return expired, invalid, user, data
    else:
        return expired, invalid, user


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


# REGISTRATION

async def register_user(db, email, password, origin=None):
    user = add_user(db, email, password)

    confirmation_link, token = generate_confirmation_link(user, origin=origin)
    body = dict(user=user, confirmation_link=confirmation_link)
    await send_mail(security_env_value('EMAIL_SUBJECT_REGISTER'), user.email,
                    'welcome', body)

    return user


def confirm_email_token_status(db, token):
    """Returns the expired status, invalid status, and user of a confirmation
    token. For example::

        expired, invalid, user = confirm_email_token_status('...')

    :param token: The confirmation token
    """
    expired, invalid, user, token_data = \
        get_token_status(db, token, 'confirm', 'CONFIRM_EMAIL', return_data=True)
    if not invalid and user:
        user_id, token_email_hash = token_data
        invalid = not verify_hash(token_email_hash, user.email)
    return expired, invalid, user


def confirm_user(db, user):
    """Confirms the specified user

    :param user: The user to confirm
    """
    if user.confirmed_at is not None:
        return False
    user.confirmed_at = dt.datetime.utcnow()
    db.commit()
    return True


# RECOVER PASSWORD

async def send_reset_password_instructions(db, origin, email):
    user = get_user_by_email(db, email)
    token = generate_reset_password_token(user)
    reset_link = '{}?token={}'.format(origin, token)

    body = dict(user=user, reset_link=reset_link)
    await send_mail(security_env_value('EMAIL_SUBJECT_PASSWORD_RESET'), user.email,
                    'reset_instructions', body)


def reset_password_token_status(db, token):
    """Returns the expired status, invalid status, and user of a password reset
    token. For example::

        expired, invalid, user, data = reset_password_token_status('...')

    :param token: The password reset token
    """
    expired, invalid, user, data = get_token_status(
        db, token, 'reset', 'RESET_PASSWORD', return_data=True
    )
    if not invalid:
        if user.password:
            if not verify_hash(data[1], user.password):
                invalid = True

    return expired, invalid, user


# CHANGE PASSWORD

async def update_password(db, user_id, hydra_user_id, new_password):
    """
    Update a user's password on both OpenAgua and Hydra Platform
    :param db:
    :param user_id:
    :param hydra_user_id:
    :param new_password:
    :return:
    """

    # Update the OpenAgua user
    user = get_user(db, user_id)
    user.password = hash_password(new_password).encode()
    db.commit()

    # update the Hydra user
    hydra_admin = root_connection()
    hydra_admin.call('update_user_password', hydra_user_id, new_password)

    await send_password_changed_notice(user)
