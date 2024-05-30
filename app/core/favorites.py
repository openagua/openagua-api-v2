from app.core.studies import get_study
from app.models import Favorite


# favorites

def add_favorite(db, study_id, name, description, type, thumbnail, filters, pivot, content):
    favorite = Favorite()
    favorite.study_id = study_id
    favorite.name = name
    favorite.description = description
    favorite.type = type
    favorite.thumbnail = thumbnail
    favorite.filters = filters
    favorite.pivot = pivot
    favorite.content = content

    db.add(favorite)
    db.commit()

    db.close()

    return favorite.id


def add_update_favorite(db, study_id=None, network_id=None, favorite_id=None, favorite=None):
    favorite = favorite or {}
    if favorite_id:
        f = db.query(Favorite).filter_by(id=favorite_id).first()
    else:
        f = Favorite()

        f.study_id = study_id
        if network_id:
            f.network_id = network_id

    f.name = favorite.get('name')
    f.description = favorite.get('description')
    f.thumbnail = favorite.get('thumbnail', '')
    f.type = favorite.get('type')
    f.filters = favorite.get('filters')
    f.pivot = favorite.get('pivot')
    f.analytics = favorite.get('analytics')
    f.content = favorite.get('content')
    f.provider = favorite.get('provider')

    if not favorite_id:
        db.add(f)
    db.commit()

    return f


def get_favorite(db, favorite_id=None):
    if favorite_id and type(favorite_id) == int:
        favorite = db.query(Favorite).filter_by(id=favorite_id).first()
    else:
        favorite = None

    return favorite


def get_favorites(db, dataurl_id=None, project_id=None, study_id=None, network_id=None):
    favorites = []
    if dataurl_id and project_id:
        study = get_study(db, dataurl_id=dataurl_id, project_id=project_id)
        study_id = study.id
    if study_id and network_id:
        favorites = db.query(Favorite).filter_by(study_id=study_id, network_id=network_id).all()
    elif study_id:
        favorites = db.query(Favorite).filter_by(study_id=study_id)

    return [favorite.to_json() for favorite in favorites]


def validate_favorites(db, hydra, network_id=None, favorites=()):
    # Check if favorite is still valid
    network = hydra.call('get_network', network_id, summary=False, include_data=False, include_resources=False)
    try:
        scenario_ids = set([s.id for s in network['scenarios']])
    except:
        print(network)
    ret = []
    for favorite in favorites:
        if favorite['filters'].get('results'):
            favorite['filters']['scenarios'] = favorite['filters']['results']
            del favorite['filters']['results']
            add_update_favorite(db, favorite=favorite)
        favorite_scenarios = set(favorite['filters'].get('scenarios', []))
        if favorite_scenarios and favorite_scenarios.issubset(scenario_ids):

            ret.append(favorite)
        else:
            delete_favorite(db, favorite_id=favorite['id'])
    return ret


def delete_favorite(db, favorite_id=None):
    try:
        db.query(Favorite).filter_by(id=favorite_id).delete()
        db.commit()
        error = 0
    except:
        error = 1
    return error


def delete_favorites(db, favorite_ids=None):
    try:
        favorites = db.query(Favorite).filter(Favorite.id.in_(favorite_ids)).all() if favorite_ids else []
        for favorite in favorites:
            favorite.delete()
        db.commit()
        error = 0
    except:
        error = 1
    return error
