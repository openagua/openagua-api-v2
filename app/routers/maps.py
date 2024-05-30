from os import getenv, environ as env
from fastapi import APIRouter

api = APIRouter(prefix='/maps', tags=['Maps'])


@api.get('/tiles_provider')
def _get_tiles_provider():
    google_key = getenv('GOOGLE_PLACES_API_KEY')
    mapbox_key = getenv('MAPBOX_ACCESS_TOKEN')
    preferred_map_provider = getenv('PREFERRED_MAP_PROVIDER', 'mapbox')
    if google_key and preferred_map_provider == 'google':
        key = google_key
    elif mapbox_key and preferred_map_provider == 'mapbox':
        key = mapbox_key
    else:
        key = None
    return {'provider': {'name': preferred_map_provider, 'key': key}}
