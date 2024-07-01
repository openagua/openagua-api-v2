from os import getenv

from fastapi import FastAPI, Depends
from fastapi.middleware import Middleware
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from app.deps import authorized_user

from app.routers import (
    auth, users, accounts, maps, gui,
    projects, networks, templates, scenarios,
    favorites, dashboards, modelruns, files, data, hydra
)

allowed_origins = [
    getenv('CORS_ORIGIN'),
    'http://localhost',
    'http://localhost:8080',
    'http://localhost:3000'
]

middleware = [Middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    # allow_headers=['Content-Type', 'Authorization', 'X-API-KEY']
)]

app = FastAPI(
    title='OpenAgua API',
    description='The core API for OpenAgua',
    version='0.1',
    middleware=middleware
)


@app.get('/', tags=['Default'])
async def homepage() -> str:
    return "Hello, world!"


api_prefix = '/v2'
app.include_router(auth.api, prefix=api_prefix)

protected_routers = [users, accounts, maps, gui, projects, networks, templates, scenarios, dashboards,
                     favorites, modelruns, files, data, hydra]
for protected_router in protected_routers:
    app.include_router(protected_router.api, prefix=api_prefix, dependencies=[Depends(authorized_user)])

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
