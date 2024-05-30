from fastapi import APIRouter, Request, Depends
from typing import List
from app.deps import get_g, get_mq, get_pubnub

from app.core.modeling import get_model, get_models, delete_model, update_model, add_model_template, add_model, get_network_model
from app.core.model_control import start_model_run, pause_model_run, cancel_model_run, add_ping, ProcessState, \
    end_model_run, get_run_records, delete_run_record, delete_run_records

# api = Namespace('Model engines API', path='/models', description='Operations related to model engines.')

api = APIRouter(prefix='/models', tags=['Modeling'])


@api.get('/engines')
def _get_engine(project_id: int, network_ids: List[int] | None = None, engine_ids: List[int] | None = None,
                g=Depends(get_g)):
    if engine_ids:
        models = get_models(g.db, user_id=g.current_user.id, engine_ids=engine_ids)
    elif project_id and network_ids:
        dataurl_id = g.datauser.dataurl_id
        models = get_models(g.db, dataurl_id=dataurl_id, project_id=project_id, network_ids=network_ids)
    elif project_id:
        dataurl_id = g.datauser.dataurl_id
        models = get_models(g.db, dataurl_id=dataurl_id, project_id=project_id)
    elif network_ids:
        dataurl_id = g.datauser.dataurl_id
        models = get_models(g.db, dataurl_id=dataurl_id, network_ids=network_ids)

    else:

        for scope in ['public', 'shared', 'private']:

            models = get_models(scope, project_id=project_id, user_id=g.current_user.id)

            for m in models:

                if not m['templates']:
                    template = g.hydra.call('get_template_by_name', m.name)
                    if template:
                        modeltemplate = add_model_template(g.db, g.hydra.url, m.id, template)
                        m['templates'] = [modeltemplate.to_json()]
                    else:
                        m['templates'] = []
                else:
                    for t in m['templates']:
                        if t.get('template_name'):
                            template = g.hydra.call('get_template_by_name', t.template_name)
                            if not template:
                                del t

    return models


@api.post('/engines', status_code=201)
async def _add_engine(request: Request, g=Depends(get_g)):
    data = await request.json()
    template_id = data.get('template_id')
    model = data.get('model')
    model['user_id'] = g.current_user.id
    model['study_id'] = g.study.id if hasattr(g, "study") else None
    model = add_model(g.db, g.hydra.url, model, template_id)
    ret_model = model.to_json(include_templates=True)
    return ret_model


@api.get('/engines/{model_id}')
def _get_engine(model_id: int, network_id: int | None = None, g=Depends(get_g)):
    if network_id:
        network_model = get_network_model(db=g.db, url=g.hydra.url, network_id=network_id)
        if network_model:
            model = get_model(g.db, id=network_model.model_id)
        else:
            model = None
    else:
        model = get_model(g.db, id=model_id)

    return model.to_json(include_templates=True) if model else None


@api.put('/engines/{model_id}')
async def _update_engine(request: Request, model_id: int, g=Depends(get_g), mq=Depends(get_mq)):
    data = await request.json()
    model = data.get('model')
    template_id = data.get('template_id')
    model.pop('templates', None)
    updated_model = update_model(g.db, mq, **model)
    ret_model = updated_model.to_json(include_templates=True)
    ret_model['project_id'] = model.get('project_id')
    return ret_model


@api.delete('/engines/{model_id}', status_code=204)
def _delete_engine(model_id: int, g=Depends(get_g)):
    delete_model(g.db, model_id)


@api.post('/run_configurations', status_code=201)
async def _add_run_configuration(request: Request, g=Depends(get_g)):
    data = await request.json()
    network_id = data.get('network_id')
    new_config = data.get('config')
    new_config.pop('id', None)
    network = g.hydra.call('get_network', network_id, include_resources=False, include_data=False, summary=True)
    configs = network['layout'].get('run_configurations', [])

    config = dict(
        id=max([config['id'] for config in configs]) + 1 if configs else 1,
        **new_config
    )
    configs.append(config)
    network['layout']['run_configurations'] = configs
    g.hydra.call('update_network', network)

    return config


@api.put('/run_configurations/{config_id}')
async def _update_run_configuration(request: Request, config_id: int, g=Depends(get_g)):
    data = await request.json()
    network_id = data.get('network_id')
    config = data.get('config')
    network = g.hydra.call('get_network', network_id, include_resources=False, include_data=False, summary=True)
    current_configurations = network['layout'].get('run_configurations', [])
    new_configurations = [config if c['id'] == config['id'] else c for c in current_configurations]
    network['layout']['run_configurations'] = new_configurations
    g.hydra.call('update_network', network)

    return config


@api.delete('/run_configurations/{config_id}', status_code=204)
def _delete_run_configuration(config_id: int, network_id: int | None = None, g=Depends(get_g)):
    network = g.hydra.call('get_network', network_id, include_resources=False, include_data=False, summary=True)
    network['layout']['run_configurations'] = [c for c in network['layout'].get('run_configurations', []) if
                                               c['id'] != config_id]
    g.hydra.call('update_network', network)


@api.post('/runs', description='Run a model based on project/network settings and user input.', status_code=201)
async def _add_model_run(request: Request, g=Depends(get_g), mq=Depends(get_mq)):
    data = await request.json()
    network_id = data.get('network_id')
    guid = data.get('guid')
    computer_id = data.get('computer_id')
    config = data.get('config', {})
    scenarios = data.get('scenarios', [])
    host_url = request.url
    ret = start_model_run(g.db, g.hydra, g.current_user.email, host_url, network_id, guid, config, scenarios,
                          computer_id=computer_id, mq=mq)
    return ret


@api.delete('/runs/{sid}', status_code=204)
async def _delete_model_run(sid: str, source_id: int, network_id: int, name: str | None = None, scids: List[int] | None = None,
                 progress: int | None = None, g=Depends(get_g), pubnub=Depends(get_pubnub)):
    cancel_model_run(g.db, pubnub, sid)
    data = dict(
        sid=sid,
        name=name,
        source_id=source_id,
        network_id=network_id,
        scids=scids or [],
        progress=progress
    )
    end_model_run(g.db, sid, ProcessState.CANCELED, data)


@api.post('/runs/{sid}/actions/{action}', status_code=201)
async def _add_model_run_action(request: Request, sid: str, action: str, source_id: int, network_id: int,
                                 g=Depends(get_g), pubnub=Depends(get_pubnub)):
    data = await request.json()
    if action == 'start':
        data.pop('status', None)
        data.pop('sid', None)
        ping = add_ping(sid, ProcessState.STARTED, **data)
        # emit_progress(source_id=source_id, network_id=network_id, ping=ping.to_json())

    elif action == 'save':
        # emit_progress(source_id=source_id, network_id=network_id, ping=data)
        pass

    elif action == 'error':
        end_model_run(g.db, sid, ProcessState.ERROR, data)

    elif action == 'done':
        end_model_run(g.db, sid, ProcessState.FINISHED, data)

    elif action == 'stop':
        end_model_run(g.db, sid, ProcessState.CANCELED, data)

    elif action == 'pause':
        pause_model_run(g.db, pubnub, sid)
        pass  # TODO: update

    elif action == 'resume':
        pass  # TODO: update

    elif action == 'clear':
        pass  # TODO: is this needed?


@api.get('/runs/records')
def _get_model_run_record(network_id: int, g=Depends(get_g)):
    source_id = g.datauser.dataurl_id
    records = get_run_records(g.db, source_id=source_id, network_id=network_id)
    return records


@api.delete('/runs/records', status_code=204)
def _delete_model_run_records(network_id: int | None = None, record_id: int | None = None, g=Depends(get_g)):
    if record_id:
        delete_run_record(g.db, record_id)
    else:
        source_id = g.datauser.dataurl_id
        delete_run_records(g.db, source_id=source_id, network_id=network_id)


@api.delete('/runs/records/{record_id}')
def _delete_model_run_record(record_id: int, g=Depends(get_g)):
    delete_run_record(g.db, record_id)
