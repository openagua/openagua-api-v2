from fastapi import APIRouter, Request, Depends, Response, HTTPException

from app import schemas, config
from app.deps import get_g
from app.services import s3

from app.core.studies import get_study, delete_study, add_star, remove_star, get_stars
from app.core.files import delete_all_network_files
from app.core.users import get_dataurl_by_id
from app.core.sharing import set_resource_permissions, share_resource
from app.core.projects import prepare_project_for_client, prepare_projects_for_client, copy_project

api = APIRouter(tags=['Projects'])


@api.get('/projects', description='Get a list of projects, filtered by various options.')
async def _get_projects(is_public: bool = False, page: int = 0, max_per_page: int = 10, include_networks: bool = True,
                        search: str = '', g=Depends(get_g)):
    # if g.is_public_user:
    #     user_id = g.hydra.user_id

    # else:
    #     user_id = g.datauser.userid
    user_id = g.datauser.userid

    projects = g.hydra.call('get_projects', user_id, user_id=user_id, summary=True, page=page,
                            public_only=is_public, search=search,
                            max_per_page=max_per_page, include_networks=include_networks)

    if projects is None:
        raise HTTPException(511)
    if 'error' in projects:
        return []

    projects = prepare_projects_for_client(g.db, g.hydra, projects, g.source_id, user_id, include_models=True)

    return projects


@api.post('/projects', status_code=201)
def add_project(data: schemas.Project, purpose: str = '', g=Depends(get_g)):
    project = data
    if purpose == 'copy':
        proj = copy_project(g.hydra, project)
    else:
        proj = g.hydra.call('add_project', project)

    if 'error' in proj:
        return proj
    else:
        project = proj

    source_id = g.datauser.dataurl_id
    source_url = g.hydra.url
    source_user_id = g.datauser.userid
    project = prepare_project_for_client(g.db, g.hydra, project=project, source_id=source_id,
                                         source_user_id=source_user_id, data_url=source_url, include_models=True)

    # and add a first project note while we're here, to hold the long description
    note = {'ref_key': 'PROJECT', 'ref_id': project.id, 'value': b''}
    g.hydra.call('add_note', note)

    return project


@api.get('/projects/count')
def _get_projects_count(page: int = 1, search: str = '', g=Depends(get_g)):
    projects_count = g.hydra.call('get_public_projects_count', search=search) if page == 1 else None
    return projects_count


@api.get('/projects/{project_id}')
def _get_project(project_id: int, include_networks: bool = False, g=Depends(get_g)):
    project = g.hydra.call('get_project', project_id, include_networks=include_networks)
    if project is None:
        raise HTTPException(511)
    if 'error' in project:
        raise HTTPException(501, project['error'])

    source_id = g.datauser.dataurl_id
    source_user_id = g.datauser.userid
    source_url = g.hydra.url
    project = prepare_project_for_client(g.db, g.hydra, project, source_id, source_user_id, data_url=source_url,
                                         include_models=True)

    return project


@api.put('/projects/{project_id}')
def _update_project(project: schemas.Project, project_id: int, g=Depends(get_g)):
    g.hydra.call('update_project', project)

    return Response(204)


@api.get('/starred_projects')
def _get_starred_project(g=Depends(get_g)):
    stars = get_stars(g.db, user_id=g.current_user.id)

    return stars


@api.patch('/projects/{project_id}')
def patch_project(request: Request, project_id: int, g=Depends(get_g)):
    data = request.json()
    project = g.hydra.call('get_project', project_id)
    project.update(data)
    g.hydra.call('update_project', project)
    return Response(204)


@api.delete('/projects/{project_id}')
def _delete_project(project_id, g=Depends(get_g)):
    dataurl = get_dataurl_by_id(g.db, id=g.source_id)
    project = g.hydra.call('get_project', project_id)

    if not project or not hasattr(project, 'networks'):
        return '', 410

    bucket_name = config.AWS_S3_BUCKET
    for network in project.networks:
        delete_all_network_files(network, bucket_name, s3=s3)
        g.hydra.call('delete_network', network['id'], purge_data=True)

    templates = g.hydra.call('get_templates', project_id=project_id)
    for template in templates:
        g.hydra.call('delete_template', template_id=template['id'])

    resp = g.hydra.call('delete_project', project_id, purge_data=True)
    if resp == 'OK':
        study = get_study(g.db, url=dataurl.url, project_id=project_id)
        delete_study(g.db, study_id=study.id)

    return Response(204)


@api.get('/projects/{project_id}/notes')
async def _get_project_notes(project_id: int, g=Depends(get_g)):
    notes = g.hydra.call('get_notes', 'PROJECT', project_id)
    for note in notes:
        try:
            note.pop('project', None)
        except:
            print(notes)
        try:
            if isinstance(note['value'], bytes):
                note['value'] = note['value'].decode()
        except:
            print('Something went wrong processing note: ')
            print(note)
    return notes


@api.post('/projects/{project_id}/notes', status_code=201)
async def _add_project_note(request: Request, project_id: int, g=Depends(get_g)):
    note = await request.json()
    note['value'] = note['value'].encode()
    note['ref_key'] = 'PROJECT'
    note['ref_id'] = project_id
    note = g.hydra.call('add_note', note)
    note['value'] = note.get('value').decode()
    return note


@api.put('/projects/{project_id}/notes/{note_id}')
async def _update_project_note(request: Request, project_id: int, note_id: int, g=Depends(get_g)):
    note = await request.json()
    note['value'] = note.get('value', '').encode()
    note['ref_key'] = 'PROJECT'
    note['ref_id'] = project_id
    note = g.hydra.call('update_note', note)
    note['value'] = note.get('value', b'').decode()
    return note


@api.post('/projects/{project_id}/permissions', status_code=201)
async def _add_project_permission(request: Request, project_id: int, g=Depends(get_g)):
    data = await request.json()
    emails = data['emails']
    permissions = data['permissions']
    message = data.get('message', '')
    results = share_resource(g.db, g.hydra, g.current_user.id, 'project', project_id, emails, permissions,
                             message=message)
    return results


@api.put('/projects/{project_id}/permissions', status_code=204)
async def put(request: Request, project_id: int, g=Depends(get_g)):
    data = await request.json()
    permissions = data['permissions']
    for username, _permissions in permissions.items():
        results = set_resource_permissions(g.hydra, 'project', project_id, username, _permissions)


@api.post('/projects/{project_id}/star', status_code=201)
def _add_project_star(project_id: int, source_id: int = 1, g=Depends(get_g)):
    try:
        add_star(g.db, g.current_user.id, source_id, project_id)
    except:
        raise HTTPException(500, 'Unable to add star')


@api.delete('/projects/{project_id}/star', status_code=204)
def delete(project_id: int, source_id: int = 1, g=Depends(get_g)):
    remove_star(g.db, g.current_user.id, source_id, project_id)
