from fastapi import APIRouter, Depends
from typing import List
from app.deps import get_g
from app.schemas import Scenario, ResourceGroupItem
from app.core.scenarios import delete_data_scenario

api = APIRouter(prefix='/scenarios', tags=['Scenarios'])


@api.post('/')
def _post_scenarios(scenario: Scenario, g=Depends(get_g)) -> Scenario:
    network_id = scenario.network_id
    parent_id = scenario.get('parent_id')

    if parent_id:
        new_scenario = g.hydra.call('create_child_scenario', parent_id, scenario['name'])
        new_scenario.update(scenario)
        new_scenario = g.hydra.call('update_scenario', new_scenario)
    else:
        new_scenario = g.hydra.call('add_scenario', network_id, scenario, return_summary=True)

    return new_scenario


@api.put('/', status_code=204)
def _update_scenarios(scenarios: List[Scenario], g=Depends(get_g)):
    for scenario in scenarios:
        g.hydra.call('update_scenario', scenario)


@api.get('/{scenario_id}')
def _get_scenario(scenario_id: int, include_data: bool = False, g=Depends(get_g)) -> Scenario:
    scenario = g.hydra.call('get_scenario', scenario_id, include_data=include_data)
    return scenario


@api.put('/{scenario_id}')
def _update_scenario(scenario: Scenario, scenario_id: int, return_summary: bool = False, g=Depends(get_g)) -> Scenario:
    updated_scenario = g.hydra.call('update_scenario', scenario)
    return updated_scenario


@api.patch('/{scenario_id}')
def _patch_scenario(scenario_id: int, updates: dict, g=Depends(get_g)):
    scenario = g.hydra.call('get_scenario', scenario_id)
    scenario.update(updates)
    scenario = g.hydra.call('update_scenario', scenario)
    return scenario


@api.delete('/{scenario_id}', status_code=204)
def _delete_scenario(scenario_id: int, scenario_class: str = 'input', g=Depends(get_g)) -> None:
    if scenario_class == 'result':
        study_id = g.study.id
        result = delete_data_scenario(g.db, g.hydra, scenario_id, study_id)
    else:
        result = delete_data_scenario(g.db, g.hydra, scenario_id)


@api.post('/{scenario_id}/resource_group_items', status_code=201)
def _post_scenario_resource_group_item(scenario_id: int, items: List[ResourceGroupItem], g=Depends(get_g)) -> \
        List[ResourceGroupItem]:
    scenario = g.hydra.call('get_scenario', scenario_id)
    scenario['resourcegroupitems'] = items
    result = g.hydra.call('update_scenario', scen=scenario)
    ret_items = result.resourcegroupitems[-len(items):]
    return ret_items


@api.delete('/{scenario_id}/resource_group_items', status_code=204)
def delete(scenario_id, ids: List[int], g=Depends(get_g)):
    for item_id in ids:
        g.hydra.call('delete_resourcegroupitem', item_id=item_id)
