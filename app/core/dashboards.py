import json
from app.models import Dashboard, StudyDashboards, Card, DashboardCards
from app.core.studies import get_study
import bleach


def add_dashboard(db, study_id=None, network_id=None, dashboard=None):
    new_dashboard = Dashboard()
    layout = dashboard.get('layout')
    if type(layout) != str:
        layout = json.dumps(layout)

    new_dashboard.title = dashboard.get('title', '')
    new_dashboard.description = dashboard.get('description')
    new_dashboard.layout = layout
    db.add(new_dashboard)
    db.commit()

    add_dashboard_to_study(db, study_id=study_id, network_id=network_id, dashboard_id=new_dashboard.id)

    return new_dashboard


def add_dashboard_to_study(db, study_id, network_id, dashboard_id):
    study_dashboard = StudyDashboards()
    study_dashboard.study_id = study_id
    study_dashboard.network_id = network_id
    study_dashboard.dashboard_id = dashboard_id
    db.add(study_dashboard)
    db.commit()

    return study_dashboard


def get_dashboards(db, **kwargs):
    dashboards = []
    if 'project_id' in kwargs and 'datauser_id' in kwargs:
        study = get_study(db, project_id=kwargs['project_id'], datauser_id=kwargs['datauser_id'])
        dashboards = study.dashboards
    elif 'study_id' in kwargs:
        study = get_study(db, id=kwargs['study_id'])
        dashboards = study.dashboards
    elif 'network_id' in kwargs:
        # TODO: fix this - sqlalchemy should handle this better
        dashboards = db.query(Dashboard).join(StudyDashboards).filter(
            StudyDashboards.network_id == kwargs['network_id']).all()

    dashboards = [dashboard.to_json() for dashboard in dashboards]
    return dashboards


def get_dashboard(db, dashboard_id):
    return db.query(Dashboard).filter_by(id=dashboard_id).first()


def delete_dashboard(db, dashboard_id):
    dashboard = db.query(Dashboard).filter_by(id=dashboard_id).first()
    db.delete(dashboard)
    db.commit()
    return


def update_dashboard(db, updated):
    dashboard = get_dashboard(db, updated.id)
    if updated.title:
        dashboard.title = updated.title
    if updated.description:
        dashboard.description = updated.description
    if updated.layout:
        dashboard.layout = updated.layout
    db.commit()

    if updated.cards:
        ids = []
        for card in updated.cards:
            if card.get('id') <= 0:
                card = add_card(**card)
                add_card_to_dashboard(db, dashboard.id, card.id)
            else:
                card = update_card(**card)
            ids.append(card.id)

        # remove deleted ids
        dashboard = get_dashboard(db, dashboard.id)
        for card in dashboard.cards:
            if card.id not in ids:
                remove_card_from_dashboard(db, dashboard.id, card.id)

    dashboard = get_dashboard(db, dashboard.id)
    return dashboard


def remove_card_from_dashboard(db, dashboard_id, card_id):
    db_card = DashboardCards.query.filter_by(dashboard_id=dashboard_id, card_id=card_id).first()
    db.delete(db_card)
    db.commit()


def cleaned(text):
    return bleach.clean(text, tags=bleach.sanitizer.ALLOWED_TAGS + ['sup', 'sub'])


def add_card(db, **kwargs):
    card = Card()
    card.title = kwargs['title']
    card.description = cleaned(kwargs.get('description', ''))
    card.type = kwargs['type']
    card.layout = json.dumps(kwargs['layout'])
    card.content = json.dumps(kwargs['content'])
    card.favorite_id = kwargs.get('favorite_id')

    db.add(card)
    db.commit()

    return card


def update_card(db, **kwargs):
    card = db.query(Card).filter_by(id=kwargs['id']).first()
    card.title = kwargs['title']
    card.description = cleaned(kwargs.get('description', ''))
    card.type = kwargs['type']
    card.layout = json.dumps(kwargs['layout'])
    card.content = json.dumps(kwargs['content'])
    card.favorite_id = kwargs.get('favorite_id')

    # db.update(card)
    db.commit()

    return card


def add_card_to_dashboard(db, dashboard_id, card_id):
    dc = DashboardCards()
    dc.dashboard_id = dashboard_id
    dc.card_id = card_id
    db.add(dc)
    db.commit()

    return dc
