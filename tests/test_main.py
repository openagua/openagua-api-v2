import pytest
from fastapi.testclient import TestClient
from app.main import app, api_prefix
from app.deps import get_db, get_g
from app import config
from tests.overrides import override_get_db, override_get_g

app.dependency_overrides[get_db] = override_get_db
# app.dependency_overrides[get_g] = override_get_g

client = TestClient(app, headers={'X-API-KEY': config.TEST_API_KEY})
client.base_url = f'{client.base_url}{api_prefix}'


@pytest.fixture
def project():
    return {
        'id': 0,
        'name': 'Test project',
        'description': 'Test project description',
    }


@pytest.fixture
def network():
    return {
        'id': None,
        'name': 'Test network',
        'description': 'Test network description',
    }


@pytest.fixture
def nodes():
    return [
        {'name': 'Node 1', 'description': 'First node', 'x': 0, 'y': 0},
        {'name': 'Node 2', 'description': 'Second node', 'x': 1, 'y': 1},
    ]


@pytest.fixture
def link():
    return {'name': 'Link 1', 'description': 'The first link'}


def test_add_project(project):
    resp = client.post('projects')
    assert resp.status_code == 201
    data = resp.json()
    assert 'id' in data


# def test_get_project():
#     resp = client.get(f'/project/{project_id}')
#     assert resp.status_code == 200
#     data = resp.json()
#     assert data['id'] == project['id']
#

def test_get_projects():
    resp = client.get('projects')
    data = resp.json()
    assert isinstance(data, list) and data[0]['id'] == project['id']

#
# def test_update_project():
#     _project = project.copy()
#     _project['name'] = 'Updated project name'
#     resp = client.put(f'/project/{project_id}', data=_project)
#     data = resp.json()
#     assert data['name'] == _project['name']
#
#
# def test_delete_project():
#     resp = client.delete('/project', params={'project_id': project['id']})
#     assert resp.status_code == 200
#
#     resp = client.get('/project', params={'project_id': project['id']})
#     assert resp.status_code == 404
#
#
# def test_add_network():
#     _network = test_network.copy()
#     _network['project_id'] = project['id']
#     resp = client.post('/network', data=_network)
#     assert resp.status_code == 201
#     data = resp.json()
#     assert 'id' in data
#
#
# def test_get_network():
#     resp = client.post(f'/network/{network_id}')
#     assert resp.status_code == 200
#     data = resp.json()
#     assert data['id'] == network['id']
#
#
# def test_update_network():
#     _network = network.copy()
#     _network['name'] = _network['name'] = 'Updated network name'
#     resp = client.put(f'/network/{network_id}', data=_network)
#     assert resp.status_code == 200
#     data = resp.json()
#     assert data['name'] == _network['name']
#
#
# def test_delete_network():
#     network_id = network['id']
#     resp = client.delete(f'/network/{network_id}')
#     assert resp.status_code == 200
#
#     resp = client.get(f'/network/{network_id}')
#     assert resp.status_code == 404
#
#     test_add_network()
#
#
# def test_add_node():
#     _node = test_nodes[0]
#     _node['network_id'] = network_id
#     resp = client.post('/node', data=_node)
#     assert resp.status_code == 201
#
#     data = resp.json()
#     assert 'id' in data
#
#
# def test_delete_node():
#     resp = client.delete(f'/node/{node_id}')
#     assert resp.status_code == 200
#
#     resp = client.get(f'/node/{node_id}')
#     assert resp.status_code == 404
#
#     test_add_node()
