from fastapi import APIRouter, Depends
from typing import List
from app.deps import get_g
from app.schemas import Dashboard
from app.core.dashboards import get_dashboard, get_dashboards, add_dashboard, update_dashboard, delete_dashboard

# import blueprint definition
api = APIRouter(tags=['Visualization'])


@api.get('/dashboards')
def _get_dashboards(project_id: int = 0, network_id: int = 0, g=Depends(get_g)) -> List[Dashboard]:
    if network_id:
        dashboards = get_dashboards(g.db, network_id=network_id)
    elif network_id:
        dashboards = get_dashboards(g.db, project_id=project_id)
    else:
        dashboards = []
    return dashboards


@api.post('/dashboards', status_code=201)
def post(dashboard: Dashboard, g=Depends(get_g)) -> Dashboard:
    dashboard = add_dashboard(g.db, study_id=g.study.id, dashboard=dashboard)
    return dashboard.to_json()


@api.get('/dashboards/{dashboard_id}')
def _get_dashboard(dashboard_id, g=Depends(get_g)) -> Dashboard:
    dashboard = get_dashboard(g.db, dashboard_id)
    return dashboard.to_json()


@api.put('/dashboards/{dashboard_id}', status_code=200)
def _update_dashboard(dashboard: Dashboard, dashboard_id: int, g=Depends(get_g)) -> Dashboard:
    dashboard = update_dashboard(g.db, dashboard)
    return dashboard.to_json()


@api.delete('/dashboards/{dashboard_id}', status_code=204)
def _delete_dashboard(dashboard_id, g=Depends(get_g)):
    delete_dashboard(g.db, dashboard_id=dashboard_id)
