from os import getenv
from fastapi import APIRouter, Depends, Request
from pydantic import HttpUrl

from app.deps import get_g
from app.schemas import Database

from app.core.security import generate_api_key
from app.core.account import get_data_databases, add_database, update_database, remove_database, get_api_keys, \
    delete_api_key

api = APIRouter(prefix='/accounts', tags=['User account'])


@api.get('/databases')
def _get_databases(g=Depends(get_g)):
    databases = get_data_databases(g.db, g.current_user.id, getenv('DATA_URL'))
    return databases


@api.post('/databases')
async def _add_database(request: Request, g=Depends(get_g)) -> Database:
    data = await request.json()
    result = add_database(
        g.db,
        g.current_user.id,
        url=data['url'],
        username=data['username'],
        password=data['password'],
        key=getenv('SECRET_ENCRYPT_KEY')
    )

    return result


@api.put('/databases/{url}')
async def _update_database(request: Request, url: HttpUrl, g=Depends(get_g)) -> Database:
    data = await request.json()
    result = update_database(
        g.db,
        g.current_user.id,
        url=data['url'],
        username=data['username'],
        password=data['password'],
        key=getenv('SECRET_ENCRYPT_KEY')
    )

    return result


@api.delete('/databases/{url}', status_code=204)
def _delete_database(url: HttpUrl, g=Depends(get_g)):
    remove_database(
        g.db,
        user_id=g.current_user.user_id,
        url=url,
    )


@api.get('/api_keys')
def _get_api_keys(g=Depends(get_g)):
    tokens = get_api_keys(g.db, g.current_user.id)
    return tokens


@api.post('/api_keys', status_code=201)
def _add_api_key(g=Depends(get_g)):
    full_token = generate_api_key(g.db)
    token = full_token.split('.')[0]
    return dict(token=token, full_token=full_token)


@api.delete('/api_keys/{token}', status_code=204)
def _delete_api_key(token: str, g=Depends(get_g)):
    delete_api_key(g.db, g.current_user.id)
