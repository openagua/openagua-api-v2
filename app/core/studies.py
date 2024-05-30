import json

from app.models import Study, Star
from app.core.users import get_dataurl, get_datausers


def add_study(db, created_by, dataurl_id, project_id):
    study = Study()
    study.created_by = created_by
    study.dataurl_id = dataurl_id
    study.project_id = project_id
    db.add(study)
    db.commit()

    return study


def update_study(db, id, user_id, **updates):
    study = get_study(db, user_id=user_id, id=id)
    settings = json.loads(study.settings if study.settings else '{}')
    settings.update(updates)
    study.settings = json.dumps(settings)
    db.commit()


def delete_studies(db, userid, network_id):
    db.query(Study).filter_by(userid=userid, network_id=network_id).delete()
    db.commit()


def delete_study(db, **kwargs):
    study = get_study(db, **kwargs)
    if study:
        study.delete()
        db.commit()


def get_study(db, **kwargs):
    user_id = kwargs.get('user_id')
    study_id = kwargs.get('id')
    project_id = kwargs.get('project_id')
    # datauser_id = kwargs.get('datauser_id')
    dataurl_id = kwargs.get('dataurl_id')
    url = kwargs.get('url')

    if study_id:
        study_id = kwargs['id']
        study = db.query(Study).filter_by(id=study_id).first()

    elif project_id:
        if url is not None:
            dataurl = get_dataurl(db, url)
            dataurl_id = dataurl.id
        if dataurl_id:
            studies = db.query(Study).filter_by(project_id=project_id, dataurl_id=dataurl_id).all()
            if not studies:
                datausers = get_datausers(db, dataurl_id=dataurl_id)
                datauser_ids = [datauser.id for datauser in datausers]
                try:
                    studies = db.query(Study).filter(Study.project_id == project_id,
                                                     Study.datauser_id.in_(datauser_ids)).all() if datauser_ids else []
                except:
                    studies = []
                if not studies:
                    study = add_study(db, created_by=user_id, project_id=project_id, dataurl_id=dataurl_id)
                else:
                    for i, s in enumerate(studies):
                        if i == 0:
                            s.created_by = user_id
                            s.dataurl_id = dataurl_id
                            study = s
                        else:
                            db.delete(study)
                    db.commit()
            else:
                study = studies[0]

    else:
        study = None

    return study


def get_studies(db, **kwargs):
    studies = []
    if 'datauser_id' in kwargs:
        studies = db.query(Study).filter_by(datauser_id=kwargs['datauser_id'])
    elif 'url' in kwargs:
        datausers = get_datausers(url=kwargs['url'])
        for datauser in datausers:
            studies.extend(db.query(Study).filter_by(datauser_id=datauser.id))
    return studies


def add_default_project(dapi, user):
    project_name = user.email
    project_description = 'Default project created for {} {} ({})'.format(
        user.firstname,
        user.lastname,
        user.email
    )

    # add project
    project = dapi.call('add_project', {'name': project_name, 'description': project_description})

    return project


def get_stars(db, user_id):
    stars = db.query(Star).filter_by(user_id=user_id).all()
    ret = {}
    for star in stars:
        source_id = star.study.dataurl_id
        project_id = star.study.project_id
        if source_id not in ret:
            ret[source_id] = []
        ret[source_id].append(project_id)
    return ret


def add_star(db, user_id, source_id, project_id):
    star = Star()
    study = get_study(db, user_id=user_id, dataurl_id=source_id, project_id=project_id)
    star.user_id = user_id
    star.study_id = study.id
    db.add(star)
    db.commit()


def remove_star(db, user_id, source_id, project_id):
    study = get_study(db, user_id=user_id, dataurl_id=source_id, project_id=project_id)
    star = db.query(Star).filter_by(user_id=user_id, study_id=study.id)
    star.delete()
    db.commit()
