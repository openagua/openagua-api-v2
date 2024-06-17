from os import environ as env
from app.models import DataUser, DataUrl, APIKey

from app.core.hydra import HydraConnection
from app.core.utils import decrypt
from app.core.users import add_dataurl, add_datauser, update_datauser, delete_datauser, get_datauser
from app.core.security import generate_api_key


def get_data_databases(db, user_id, base_url):
    datausers = db.query(DataUser).filter_by(user_id=user_id).all()

    databases = []
    for datauser in datausers:
        dataurl = db.query(DataUrl).filter_by(id=datauser.dataurl_id).first()
        if dataurl.url == base_url:
            continue
        databases.append(
            {
                'url': dataurl.url,
                'userid': datauser.userid,
                'username': datauser.username,
                'password': decrypt(datauser.password, env['SECRET_ENCRYPT_KEY'])
            }
        )

    return databases


def add_database(db, user_id, **kwargs):
    username = kwargs['username']
    password = kwargs['password']
    try:
        hydra = HydraConnection(url=kwargs['url'])
        result = hydra.call('login', username, password)
    except:
        raise Exception('Bad URI')
    else:
        if result != 'OK':
            raise Exception('Bad username or password')
        else:
            # add dataurl
            dataurl = add_dataurl(db, kwargs['url'])

            # get data user ID
            data_user = hydra.get_user_by_name(username)

            # add datauser
            add_datauser(
                user_id=user_id,
                dataurl_id=dataurl.id,
                userid=data_user.id,
                **kwargs
            )

    return {'userid': data_user.id}


def update_database(db, user_id, **kwargs):
    username = kwargs['username']
    password = kwargs['password']
    try:
        hydra = HydraConnection(url=kwargs['url'])
        result = hydra.call('login', username, password)
    except:
        raise Exception('Bad URI')
    else:
        if result != 'OK':
            raise Exception('Bad username or password')
        else:
            # add dataurl
            dataurl = add_dataurl(db, kwargs['url'])

            # get data user ID
            data_user = hydra.get_user_by_name(username)
            datauser = get_datauser(
                db,
                dataurl_id=dataurl.id,
                user_id=user_id
            )

            # add datauser
            update_datauser(
                datauser,
                user_id=user_id,
                dataurl_id=dataurl.id,
                userid=data_user.id,
                **kwargs
            )

    return {'userid': data_user.id}


def remove_database(db, user_id, url):
    dataurl = db.query(DataUrl).filter_by(url=url).first()
    delete_datauser(db, user_id=user_id, dataurl_id=dataurl.id)


def add_api_key(db, user_id):
    user_key, db_key = generate_api_key(db)
    apikey = APIKey(user_id=user_id, id=db_key)
    db.add(apikey)
    db.commit()
    return user_key


def get_api_keys(db, user_id):
    keys = db.query(APIKey).filter_by(user_id=user_id).all()
    prefixes = [key.id.split('.')[0] for key in keys]

    return prefixes


def delete_api_key(db, user_id):
    apikey = db.query(APIKey).filter_by(user_id=user_id).first()
    if apikey:
        db.delete(apikey)
        db.commit()
