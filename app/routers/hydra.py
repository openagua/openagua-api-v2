from fastapi import APIRouter, Depends, HTTPException

from app.schemas import HydraCall
from app.deps import get_g

api = APIRouter(prefix='/hydra', tags=['Hydra Platform RPC'])


@api.post('/{function_name}',
          description='The Hydra Platform RPC consists of this single post call with any arbitrary Hydra function.')
def post_hydra_function(data: HydraCall, function_name: str, g=Depends(get_g)):
    try:
        hydra_args = data.args
        hydra_kwargs = data.kwargs
        if not hydra_args and not hydra_kwargs:
            hydra_kwargs = data
        hydra_kwargs['uid'] = hydra_kwargs.pop('uid', g.datauser.userid)
        resp = g.hydra.call(function_name, *hydra_args, **hydra_kwargs)
        return resp
    except Exception as err:
        return HTTPException(500, err)
