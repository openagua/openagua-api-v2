from fastapi import APIRouter, Depends
from typing import List
from app.deps import get_g
from app.schemas import Favorite

from app.core.users import get_dataurl
from app.core.favorites import get_favorites, validate_favorites, add_update_favorite, delete_favorite

api = APIRouter(tags=['Favorites'])


# TODO: move to network/{network_id}/favorites
@api.get('/favorites', description='Get favorites associated with a network')
def _get_network_favorites(network_id: int, project_id: int | None = None, g=Depends(get_g)) -> List[Favorite]:
    study_id = g.study.id if g.study else None
    dataurl = get_dataurl(g.db, g.hydra.url) if not study_id else None
    dataurl_id = dataurl.id if dataurl else None
    if not study_id and not project_id:
        network = g.hydra.call('get_network', network_id, include_resources=False, summary=True,
                               include_data=False)
        project_id = network['project_id']
    all_favorites = get_favorites(g.db, dataurl_id=dataurl_id, study_id=study_id, project_id=project_id,
                                  network_id=network_id)
    validated_favorites = validate_favorites(g.db, g.hydra, network_id=network_id, favorites=all_favorites)
    return validated_favorites


@api.post('/favorites', status_code=201)
async def _add_network_favorite(favorite: Favorite, g=Depends(get_g)) -> Favorite:
    study_id = g.study and g.study.id

    # TODO: fix this
    favorite['filters']['attr_data_type'] = 'timeseries'
    network_id = favorite['network_id']

    ret = add_update_favorite(g.db, study_id=study_id, network_id=network_id, favorite=favorite)

    return ret.to_json()


@api.put('/favorites/{favorite_id}')
async def _update_network_favorite(favorite: Favorite, favorite_id: int, g=Depends(get_g)) -> Favorite:
    study_id = g.study and g.study.id
    # TODO: fix this
    favorite['filters']['attr_data_type'] = 'timeseries'

    ret = add_update_favorite(g.db, study_id=study_id, favorite_id=favorite_id, favorite=favorite)

    return ret.to_json()


@api.delete('/favorites/{favorite_id}')
def _delete_network_favorite(favorite_id: int, g=Depends(get_g)):
    delete_favorite(g.db, favorite_id=favorite_id)
