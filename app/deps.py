from os import getenv
from fastapi import Request, HTTPException, Depends, Security
from fastapi.security import APIKeyHeader
import jwt
from app.core.utils import decode_access_token
from app.core.security import verify_api_key

from app.database import SessionLocal
from app.core.users import get_user_by_email, get_datauser
from app.core.studies import get_study
from app.core.hydra import HydraConnection, root_connection

api_key_header = APIKeyHeader(name='X-API-KEY', auto_error=False)

NotAuthorizedException = HTTPException(401, 'Not authorized')


class AppSession:
    db = None
    hydra = None
    current_user = None
    datauser = None
    is_public_user = False
    source_id = None
    project_id = None

    def __init__(self, db=None, current_user=None):
        self.db = db
        self.current_user = current_user


def get_db():
    db = SessionLocal()
    try:
        return db
    finally:
        db.close()


def get_pubnub():
    return None


def authorized_user(request: Request, access_token=None, api_key=None):
    db = get_db()
    access_token = access_token or request.cookies.get('access_token')
    api_key = api_key or request.headers.get('X-API-KEY')
    if access_token:
        try:
            payload = decode_access_token(access_token)

        except jwt.ExpiredSignatureError:
            raise HTTPException(401, 'Signature expired')
        except jwt.InvalidTokenError:
            raise HTTPException(401, 'Invalid token')
        try:
            user = get_user_by_email(db, payload['sub'])
        except:
            raise HTTPException(404)
    elif api_key:
        try:
            user = verify_api_key(db, api_key)
        except:
            raise NotAuthorizedException
    else:
        raise NotAuthorizedException

    return user


def get_g(request: Request, source_id: int = 1, project_id: int = 0, user: str = '', scope: str = '',
                public: bool = False, api_key=Security(api_key_header)):
    """
    The purpose of this is to return a Flask-like "g" object to attach arbitrary objects to (e.g., db, current_user, etc.)
    This is a protected route, in that the user must be authorized, if viewing something as a "public" user.
    """

    db = get_db()

    access_token = request.cookies.get('access_token')
    current_user = authorized_user(request, access_token, api_key=api_key)

    g = AppSession(db=db, current_user=current_user)

    g.is_public_user = user == 'public' or scope == 'public' or public is True

    if g.is_public_user:
        g.hydra = root_connection()

    else:
        if source_id:
            g.source_id = source_id
            if project_id:
                g.study = get_study(db, user_id=g.current_user.id, dataurl_id=source_id, project_id=project_id)
            datauser = get_datauser(db, user_id=g.current_user.id, dataurl_id=source_id)
            # dataurl = get_dataurl_by_id(db, source_id)
            g.datauser = datauser
            g.hydra = HydraConnection(
                id=datauser.dataurl_id,
                url=datauser.data_url,
                # session_id=datauser.sessionid,
                username=datauser.username,
                user_id=datauser.userid,
                app_name=getenv('APP_NAME')
            )

    return g


def get_s3():
    return None


def get_mq():
    """
    Get message queue, i.e. RabbitMQ
    :return:
    """
    return None
