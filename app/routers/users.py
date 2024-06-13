import requests
from fastapi import APIRouter, Request, Depends, HTTPException, Response
from app.deps import get_g
from app.core.utils import verify_password
from app.core.security import update_password
from app.core.users import get_user, get_user_settings, get_user_setting, save_user_settings, get_datausers, \
    get_dataurl_by_id

NotAuthorizedException = HTTPException(403, 'You can only get your own settings')


def add_or_update(db, user_id, key, new_setting):
    all_settings = get_user_settings(db, user_id)
    all_settings.update({key: new_setting})
    save_user_settings(db, user_id, all_settings)


api = APIRouter(prefix='/users', tags=['Users'])


@api.get('/{user_id}')
async def _get_users(user_id: int, g=Depends(get_g)):
    if user_id != g.current_user.id:
        raise HTTPException(500, 'You cannot get other users.')
    user = await get_user(g.db, user_id)
    return user.to_json(include_id=False)


@api.get('/roles')
def _get_user_roles(g=Depends(get_g)):
    roles = g.hydra.call('get_all_roles')
    return roles


@api.get('/{user_id}/sources',
         description='Get Hydra data sources associated with a user. For now, only one source is enabled.')
def _get_user_sources(user_id: int, g=Depends(get_g)):
    sources = []
    for datauser in get_datausers(g.db, user_id=user_id):
        source = get_dataurl_by_id(g.db, datauser.dataurl_id)
        try:
            source.url != 'base' and requests.get(source.url, timeout=3)
            source = source.to_json()
            source['user_id'] = datauser.userid  # id of user on data web service
            sources.append(source)
        except:
            continue

    return sources


@api.get('/{user_id}/setting/{key}', status_code=200)
def _get_user_setting(user_id, key, g=Depends(get_g)):
    user_setting = get_user_setting(g.db, user_id, key)
    return user_setting


@api.post('/{user_id}/setting/{key}', status_code=201)
async def _add_user_setting(user_id: int, key: str, new_setting, g=Depends(get_g)):
    user_setting = get_user_setting(g.db, user_id, key)
    if user_setting is not None:
        raise HTTPException(status_code=405, detail='Setting already exists. Try PUT instead.')

    add_or_update(g.db, user_id, key, new_setting)


@api.put('/{user_id}/setting/{key}', status_code=201)
async def _update_user_setting(user_id, key, updated_setting, g=Depends(get_g)):
    user_setting = get_user_setting(g.db, user_id, key)
    if user_setting is None:
        raise HTTPException(status_code=405, detail='Setting does not exist yet')

    add_or_update(g.db, user_id, key, updated_setting)


@api.delete('/{user_id}/setting/{key}', status_code=201)
async def _delete_user_setting(user_id, key, g=Depends(get_g)):
    all_settings = get_user_settings(g.db, user_id)
    all_settings.pop(key, None)
    save_user_settings(g.db, user_id, all_settings)

    return {}


@api.get('/{user_id}/settings')
def _get_user_settings(user_id: int, g=Depends(get_g)):
    if g.current_user.id != user_id:
        raise NotAuthorizedException
    return g.current_user.get_settings()


@api.put('/{user_id}/settings')
async def _update_user_settings(request: Request, user_id: int, g=Depends(get_g)):
    if g.current_user.id != user_id:
        raise NotAuthorizedException
    new_settings = await request.json()
    settings = g.current_user.get_settings()
    settings.update(new_settings)

    save_user_settings(g.db, user_id, settings)

    return {}


@api.put('/{user_id}/password')
async def _change_password(request: Request, g=Depends(get_g)):
    data = await request.json()
    password = data['password']
    user = await get_user(g.db, g.current_user.id)
    if not verify_password(password, user.password):
        raise HTTPException(status_code=400, detail="Incorrect username or password")

    result = update_password(g.db, g.current_user.id, g.hydra.user_id, **data)
    if result:
        return Response(204)
    else:
        raise HTTPException(422, 'Old password incorrect')
