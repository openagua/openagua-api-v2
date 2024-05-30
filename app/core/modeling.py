import json

from app.core.studies import get_study
from app.core.users import get_dataurl
from app.models import Model, NetworkModel, ModelTemplate, DataUrl


def add_model(db, url, model, template_id):
    m = Model()
    for key, value in model.items():
        setattr(m, key, value)
    db.add(m)
    db.commit()

    mt = add_model_template(db, url, m.id, template_id)
    ret = get_model(db, id=m.id)
    return ret


def update_model(db, mq, **kwargs):
    model_id = kwargs.pop('id')
    model = db.query(Model).filter_by(id=model_id).one()

    key = kwargs.get('key')
    model_name = kwargs.get('name')

    if key != model.key:
        vhost_template = 'model-{}'
        user_template = '{}'

        # delete old vhost and user
        old_vhost = vhost_template.format(model.key)
        old_user = user_template.format(model.key)

        resp = mq.delete_vhost(old_vhost)
        resp = mq.delete_user(old_user)

        # add new vhost and user
        new_vhost = vhost_template.format(key)
        new_user = user_template.format(key)
        vhost_kwargs = {"description": "For model ID {}".format(model.id), "tags": "production", "tracing": False}
        resp = mq.add_vhost(new_vhost, vhost_kwargs)
        resp = mq.update_user(vhost=new_vhost, user=new_user)

    for k, v in kwargs.items():
        setattr(model, k, v)
    db.commit()

    return model


def delete_model(db, model_id):
    model = db.query(Model).get(id=model_id).first()
    db.delete(model)
    db.commit()


def add_model_template(db, url, model_id, template_id):
    dataurl = db.query(DataUrl).filter_by(url=url).first()
    mt = db.query(ModelTemplate).filter_by(model_id=model_id, dataurl_id=dataurl.id, template_id=template_id).first()
    if not mt:
        mt = ModelTemplate(model_id=model_id, dataurl_id=dataurl.id, template_id=template_id)
        db.add(mt)
        db.commit()
    return mt


def get_model(db, id=None, source_id=None, project_id=None, name=None):
    if id:
        return db.query(Model).filter_by(id=id).first()
    elif source_id and name:
        study = get_study(db, project_id=project_id, dataurl_id=source_id)
        return db.query(Model).filter_by(study_id=study.id, name=name).first()
    else:
        return None


def get_network_models(db, dataurl_id, network_id):
    network_models = db.query(NetworkModel).filter_by(dataurl_id=dataurl_id, network_id=network_id).all()
    models = [nm.model.to_json() for nm in network_models]

    return models


def get_active_network_model(db, dataurl_id, network_id):
    network_model = db.query(NetworkModel).filter_by(
        dataurl_id=dataurl_id,
        network_id=network_id,
        active=True
    ).first()
    return network_model


def get_models(db, dataurl_id=None, engine_ids=None, project_id=None, network_ids=None, scope='public', user_id=None):
    study = get_study(db, user_id=user_id, dataurl_id=dataurl_id, project_id=project_id)
    # if project_id is not None and network_ids is not None:
    #     network_models = db.query(NetworkModel).filter_by(dataurl_id=dataurl_id).filter(
    #         NetworkModel.network_id.in_(network_ids)).all() if network_ids else []
    #     engine_ids = [nm.model_id for nm in network_models]
    #     models = db.query(Model).filter(Model.id.in_(engine_ids)) if engine_ids else []
    # elif project_id:
    if project_id:
        models = db.query(Model).filter_by(study_id=study.id).all()
    elif engine_ids is not None:
        models = db.query(Model).filter(Model.id.in_(engine_ids)).all() if engine_ids else []
    elif user_id and scope == 'private':
        models = db.query(Model).filter_by(scope=scope, user_id=user_id).all()
    else:
        models = db.query(Model).filter_by(scope=scope).all()
    ret = []
    for model in models:
        m = model.to_json(include_templates=True, include_network_ids=True)
        m['project_id'] = project_id
        ret.append(m)
    return ret


def set_active_model(db, hydra, source_id, network):
    update_template = False
    template_id = network['layout'].get('active_template_id')
    if template_id is None:
        update_template = True
        template_id = network.types[0]['template_id']
    elif type(template_id) != int:
        update_template = True
        template_id = network['layout']['template_id']
    if update_template:
        network['layout']['active_template_id'] = template_id

    # get model
    # This is a bit convoluted. General logic is:
    # 1. Get model from network_model (deployment-specific)
    # 2. Look for / create model from network settings (active_model_name)
    # 3. Finally, create model from template name
    model = None
    network_model = get_network_model(db, url=hydra.url, network_id=network['id'])
    model_name = None
    if network_model:
        model = get_model(db, id=network_model.model_id)
        if model is None:
            template = hydra.call('get_template', template_id)
            if model_name is None:
                model_name = template['name']
            model = get_model(source_id=source_id, project_id=network['project_id'], name=model_name)
            if model is None:
                model = add_model(
                    db,
                    url=hydra.url,
                    model={'name': model_name, 'scope': 'public'},
                    template_id=template['id']
                )
                add_network_model(db, url=hydra.url, model_id=model.id, network_id=network['id'])

    if update_template:
        network = hydra.call('update_network', network)

    return network


def get_network_model(db, url=None, network_id=None, model_id=None):
    dataurl = get_dataurl(db, url)
    if url and network_id and model_id:
        return db.query(NetworkModel).filter_by(dataurl_id=dataurl.id, model_id=model_id, network_id=network_id).first()
    elif url and network_id:
        return db.query(NetworkModel).filter_by(dataurl_id=dataurl.id, network_id=network_id).first()
    else:
        return None


def add_network_model(db, url, model_id, network_id, settings={}):
    dataurl = get_dataurl(db, url)
    nm = get_network_model(db, url=url, network_id=network_id, model_id=model_id)
    if nm is None:
        if isinstance(settings, dict):
            settings = json.dumps(settings)
        nm = NetworkModel(
            dataurl_id=dataurl.id,
            model_id=model_id,
            network_id=network_id,
            settings=settings
        )
        db.add(nm)
        db.commit()


def update_network_model(db, url, network_id, model_id, settings=None):
    dataurl = get_dataurl(db, url=url)
    network_models = db.query(NetworkModel).filter_by(dataurl_id=dataurl.id, network_id=network_id).all()
    if network_models:
        for nm in network_models[1:]:
            db.delete(nm)
        nm = network_models[0]
        nm.model_id = model_id
        nm.active = True
    else:
        if isinstance(settings, dict):
            settings = json.dumps(settings)
        nm = NetworkModel(
            dataurl_id=dataurl.id,
            model_id=model_id,
            network_id=network_id,
            settings=settings,
            active=True
        )
        db.add(nm)
    db.commit()
