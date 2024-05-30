from fastapi import APIRouter
from pydantic import HttpUrl

from app.services import ee

api = APIRouter(prefix='/gui')


@api.get('/population_grid', tags=['Maps'])
def _get_population_grid() -> HttpUrl:
    density2020 = ee.Image('CIESIN/GPWv4/unwpp-adjusted-population-density/2020')
    palette = 'ffffde,509b92,03008d'
    logDensity = density2020.where(density2020.gt(0), density2020.log())
    # combined = composite(colorized, background)
    # antiAliased = combined.reduceResolution(ee.Reducer.mean(), true)
    antiAliased = logDensity.reduceResolution(ee.Reducer.mean(), True)
    image = antiAliased.getMapId({'min': 0, 'max': 8, 'palette': palette})
    # image = logDensity.getMapId({'min': 0, 'max': 8, 'palette': 'ffffde'})
    # image = logDensity.getMapId()
    url = image["tile_fetcher"]._url_format  # token no longer needed
    return url
