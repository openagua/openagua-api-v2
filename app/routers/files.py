from os import getenv
from os import path
import uuid

from typing import List
from fastapi import APIRouter, Request, HTTPException, Depends

from app.deps import get_g
from app import config

from app.services import s3

from app.core.files import get_file_list, add_folder, \
    generate_presigned_post, generate_presigned_url, generate_presigned_urls, duplicate_objects, rename_object, \
    delete_objects

api = APIRouter(prefix='/files', tags=['Files'])


@api.get('/network_files')
def _get_network_files(network_key: str, prefix: str):
    bucket_name = config.AWS_S3_BUCKET
    if len(network_key) < 12 or network_key != prefix[:len(network_key)]:
        folders = files = []
    else:
        folders, files = get_file_list(bucket_name, prefix, s3=s3)

    return dict(folders=folders, files=files)


@api.route('/network_folder')
async def _add_network_folder(request: Request):
    data = await request.json()
    network_key = data.get('network_key', '')
    prefix = data.get('prefix', '')
    bucket_name = config.AWS_S3_BUCKET
    if len(network_key) < 12 or network_key != prefix[:len(network_key)]:
        return 'Network key must be provided', 400
    else:
        folder_key = add_folder(bucket_name, prefix, s3=s3)
        return folder_key


@api.post('/presigned_post', status_code=201)
async def _add_presigned_post(request: Request):
    data = await request.json()
    dest = data.get('dest')
    file_name = data.get('file_name')
    file_type = data.get('file_type')
    if dest == 'images':
        ext = path.splitext(file_name)[-1]
        file_name = uuid.uuid4().hex + ext
        bucket_name = getenv('AWS_S3_BUCKET_IMAGES')
    else:
        bucket_name = config.AWS_S3_BUCKET
    region = getenv('AWS_DEFAULT_REGION')

    presigned_post = generate_presigned_post(region, bucket_name, file_name, file_type, dest=dest)
    public_url = presigned_post['url'] + file_name
    return dict(public_url=public_url, **presigned_post)


@api.get('/presigned_urls')
def _get_presigned_urls(paths: List[str] = [], urls: List[str] = [], key: str = '',
                        client_method: str = 'get_object'):
    try:
        bucket_name = config.AWS_S3_BUCKET
    except:
        raise HTTPException(500, 'No AWS_S3_BUCKET environment variable found')
    keys = ['{}/{}'.format(key, path) for path in paths] + urls

    urls = generate_presigned_urls(bucket_name, keys, client_method=client_method)

    return urls


@api.get('/presigned_url')
def _get_presigned_url(key: str, client_method: str = 'get_object'):
    bucket_name = config.AWS_S3_BUCKET
    url = generate_presigned_url(bucket_name, key, client_method=client_method)
    return url


@api.put('/network/object/name')
async def _update_network_file_name(request: Request, g=Depends(get_g)):
    data = await request.json()
    network_key = data.get('network_key')
    old_key = data.get('old_key', '')
    new_key = data.get('new_key', '')
    bucket_name = config.AWS_S3_BUCKET
    if len(network_key) < 12 or network_key != new_key[:len(network_key)]:
        raise HTTPException(400, 'Network key must be provided')
    updated_object = rename_object(bucket_name, old_key, new_key, s3=s3)
    return updated_object


@api.post('/network/move_objects', status_code=204)
async def _move_objects(request: Request, g=Depends(get_g)):
    data = await request.json()
    network_key = data.get('network_key')
    bucket_name = config.AWS_S3_BUCKET
    objects = data.get('objects', [])
    for object in objects:
        old_key = object.get('old_key', '')
        new_key = object.get('new_key', '')
        if len(network_key) < 12 or network_key != new_key[:len(network_key)]:
            return 'Network key must be provided', 400
        rename_object(bucket_name, old_key, new_key, s3=s3)


@api.post('/network/duplicate_objects')
async def _duplicate_objects(request: Request, g=Depends(get_g)):
    data = await request.json()
    network_key = data.get('network_key')
    bucket_name = config.AWS_S3_BUCKET  # TODO: move all env vars to consolidated place
    objects = data.get('objects', [])
    try:
        new_files, new_folders = duplicate_objects(bucket_name, objects, network_key, s3)
        return dict(files=new_files, folders=new_folders)
    except Exception as err:
        raise HTTPException(400, str(err))


@api.delete('/network/objects')
def _delete_objects(files: list[str], folders: list[str]) -> None:
    bucket_name = config.AWS_S3_BUCKET
    delete_objects(bucket_name, files, folders)
