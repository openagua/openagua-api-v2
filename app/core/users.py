import json

from sqlalchemy.orm import Session

from app.models import DataUrl, DataUser, User

from app.core.hydra import root_connection
from app.core.utils import encrypt, hash_password
from app.config import config


async def get_users(db: Session, user_ids: list):
    users = db.query(User).filter(User.id.in_(user_ids)).all() if user_ids else []
    return {user.id: user.to_json() for user in users}


async def add_user(db: Session, email, password):
    hashed_password = hash_password(password).encode()
    user = User(email=email, password=hashed_password)
    db.add(user)
    db.commit()
    return user


async def get_user(db: Session, user_id: int):
    user = db.query(User).filter_by(id=user_id).one()
    return user


def get_user_by_email(db: Session, email: str):
    return db.query(User).filter_by(email=email).first()


def get_user_settings(db: Session, user_id: int):
    user = db.query(User).filter_by(id=user_id).one()
    return user.get_settings()


def get_user_setting(db: Session, user_id, key):
    user = db.query(User).filter(User.id == user_id).one()
    user_settings = user.get_settings()
    return user_settings.get(key, None)


def save_user_setting(db: Session, user_id, key, value):
    user = db.query(User).filter(User.id == user_id).one()
    settings = user.settings
    settings = json.loads(user.settings) if settings else {}
    settings[key] = value
    user.settings = json.dumps(settings)
    db.commit()


def save_user_settings(db: Session, user_id, settings):
    user = db.query(User).filter(User.id == user_id).one()
    if isinstance(settings, object):
        user.settings = json.dumps(settings)
    else:
        user.settings = settings
    db.commit()


def update_socketid(db: Session, user, value):
    user.socketid = value
    db.commit()


def update_datauser_sessionid(db: Session, datauser, session_id):
    return update_datauser(
        db,
        datauser,
        sessionid=session_id
    )


def add_dataurl(db: Session, url):
    dataurl = db.query(DataUrl).filter(DataUrl.url == url).first()
    if dataurl is None:  # this should be done via manage.py, not here
        dataurl = DataUrl(url=url)
        db.add(dataurl)
        db.commit()
    return db.query(DataUrl).filter(DataUrl.url == url).first()


def register_datauser(db: Session, user_id, username, password, url=None):
    url = url or config.DATA_URL

    # URL record for Hydra Platforms
    dataurl = add_dataurl(db, url)

    # add to Hydra Platform database
    hydra = root_connection()
    data_user = hydra.update_add_data_user(username, password)

    # add Hydrauser record
    datauser = add_update_datauser(
        db,
        user_id=user_id,
        dataurl_id=dataurl.id,
        username=username,
        userid=data_user.id
    )

    return datauser


def add_update_datauser(db: Session, **kwargs):
    datauser = db.query(DataUser).filter_by(
        username=kwargs['username'],
        dataurl_id=kwargs['dataurl_id']
    ).first()
    if datauser:  # update datauser
        datauser = update_datauser(db, datauser, **kwargs)
    else:  # add datauser
        datauser = add_datauser(db, **kwargs)
    return datauser


def update_datauser(db: Session, datauser, **kwargs):
    if 'password' in kwargs:
        kwargs['password'] = encrypt(kwargs['password'])
    fields = ['user_id', 'dataurl_id', 'username', 'userid', 'password', 'sessionid']
    for field in fields:
        if field in kwargs:
            exec('datauser.{} = "{}"'.format(field, kwargs[field]))
    db.commit()
    return datauser


def add_datauser(db: Session, **kwargs):
    if 'password' in kwargs:
        kwargs['password'] = encrypt(kwargs['password'])
    datauser = db.query(DataUser).filter_by(
        username=kwargs['username'],
        dataurl_id=kwargs['dataurl_id']
    ).first()
    if datauser is None:
        datauser = DataUser(
            user_id=kwargs['user_id'],
            dataurl_id=kwargs['dataurl_id'],
            userid=kwargs['userid'],
            username=kwargs['username'],
            password=kwargs.get('password', ''),
        )
        db.add(datauser)
        db.commit()

    return datauser


def delete_datauser(db: Session, user_id: int, dataurl_id: int):
    datauser = db.query(DataUser).filter_by(
        user_id=user_id,
        dataurl_id=dataurl_id
    ).first()
    db.delete(datauser)
    db.commit()


def get_datausers(db: Session, **kwargs):
    url = kwargs.get('url')
    dataurl_id = kwargs.get('dataurl_id')
    user_id = kwargs.get('user_id')
    if url:
        dataurl = get_dataurl(db, url)
        datausers = db.query(DataUser).filter_by(dataurl_id=dataurl.id).all()
    elif dataurl_id:
        datausers = db.query(DataUser).filter_by(dataurl_id=dataurl_id).all()
    elif user_id:
        datausers = db.query(DataUser).filter_by(user_id=user_id).all()
    else:
        datausers = None
    return datausers


def get_datauser(db: Session, id=None, user_id=None, username=None, url=None, dataurl_id=1):
    dataurl = None
    datauser = None

    if id:
        datauser = db.query(DataUser).filter_by(id=id).first()
    else:
        if url or username:
            dataurl = get_dataurl(db, url)
        elif dataurl_id:
            dataurl = get_dataurl_by_id(db, dataurl_id)
        elif user_id:
            dataurl = db.query(DataUrl).filter_by(user_id=user_id).first()
        if dataurl:
            if username:
                datauser = db.query(DataUser).filter_by(
                    username=username,
                    dataurl_id=dataurl.id
                ).first()
            elif user_id:
                datauser = db.query(DataUser).filter_by(
                    user_id=user_id,
                    dataurl_id=dataurl.id
                ).first()

    if datauser:
        dataurl = get_dataurl_by_id(db, datauser.dataurl_id)
        datauser.data_url = dataurl.url

    return datauser


def get_client_user(user):
    client_user = user.to_json()
    client_user['id'] = user.id
    client_user['is_admin'] = len(set(user.roles) | {'admin', 'superuser'}) > 0
    return client_user


def get_dataurl(db: Session, url: str):
    dataurl = db.query(DataUrl).filter_by(url=url or '').first()
    return dataurl


def get_dataurl_by_id(db: Session, id: int):
    dataurl = db.query(DataUrl).filter_by(id=id).first()
    return dataurl


def update_user_network_settings(db: Session, datauser_id: int, network_id: int, settings: dict):
    datauser = get_datauser(db, id=datauser_id)
    network_id = str(network_id)
    user_settings = json.loads(datauser.settings or '{}')
    user_settings['networks'] = user_settings.get('networks', {})
    user_settings['networks'][network_id] = user_settings['networks'].get(network_id, {})
    user_settings['networks'][network_id].update(settings)
    datauser.settings = json.dumps(user_settings)
    db.commit()
