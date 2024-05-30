from os import environ as env
from app.core.favorites import get_favorites, add_update_favorite, delete_favorite


def get_scenarios(hydra, network_id, scenario_type, include_baseline=True):
    return get_data_scenarios(hydra, network_id, scenario_type, include_baseline=include_baseline)


def delete_from_data_scenario(hydra, network_id, scenario_id, member_type):
    data_scenarios = get_data_scenarios(hydra, network_id, member_type)
    for data_scenario in data_scenarios:
        if scenario_id in data_scenario.get('definition', []):
            data_scenario['layout']['definition'].remove(scenario_id)
            hydra.call('update_scenario', scen=data_scenario)


def get_data_scenarios_class(hydra, network_id, scenario_class=None):
    all_scenarios = get_data_scenarios(hydra, network_id, scenario_class=scenario_class)

    for scen in all_scenarios:
        if 'parent' in scen['layout']:
            update_data_scenario(hydra, scen)

    all_scenarios = get_data_scenarios(hydra, network_id, scenario_class=scenario_class)

    return all_scenarios


def get_data_scenario(hydra, scenario_id):
    return hydra.call('get_scenario', scenario_id)


def get_data_scenarios(hydra, network_id, scenario_class=None, include_baseline=True):
    '''Get a list of data scenarios associated with a network.'''

    network = hydra.call('get_network', network_id, summary=False, include_data=False,
                         include_resources=False)
    scenarios = network.scenarios
    scenarios_subset = []
    for scenario in scenarios:
        if scenario_class is not None:
            if type(scenario_class) != list:
                scenario_class = [scenario_class]
            if scenario['layout'].get('class') in scenario_class:
                scenarios_subset.append(scenario)

            elif include_baseline and (
                    scenario['layout'].get('class') == 'baseline' or scenario['name'] == env['DEFAULT_SCENARIO_NAME']):
                update = False
                if scenario['layout'].get('class') != 'baseline':
                    scenario['layout']['class'] = 'baseline'
                    update = True
                if 'definition' not in scenario['layout']:
                    scenario['layout']['definition'] = []
                    update = True
                if 'type' not in scenario['layout']:
                    scenario['layout']['type'] = 'other'
                    update = True
                if update:
                    hydra.call('update_scenario', scenario)

                scenarios_subset.append(scenario)
        else:
            scenarios_subset = scenarios

    return scenarios_subset


def get_strategies(network):
    '''Get a list of strategies associated with a network.'''
    return network['layout'].get('strategies', [])


def update_data_scenario(hydra, scenario):
    stranded_parent_id = scenario['layout'].pop('parent')
    if stranded_parent_id:
        scenario = hydra.call('create_child_scenario', stranded_parent_id, scenario['name'])
        scenario.update(scenario)
    ret = hydra.call('update_scenario', scenario)
    return ret


def delete_data_scenario(db, hydra, scenario_id, study_id=None):
    all_favorites = []

    def delete_scenario(scenario_id):
        # Delete children scenarios...
        scenario = hydra.call('get_scenario', scenario_id)
        result = hydra.call('purge_scenario', scenario_id, delete_children=True)

        # update related favorites
        if study_id:
            scenario_class = scenario['layout'].get('class')
            for favorite in all_favorites:
                favorite_scenarios = favorite['filters'].get(scenario_class, [])
                if scenario_id in favorite_scenarios:
                    favorite_scenarios = [sid for sid in favorite_scenarios if sid != scenario_id]
                    if favorite_scenarios:
                        favorite['filters'][scenario_class] = favorite_scenarios
                        add_update_favorite(study_id=favorite['study_id'], network_id=network_id, favorite=favorite)
                    else:
                        delete_favorite(favorite['id'])

        return result

    this_scenario = get_data_scenario(hydra, scenario_id)

    if study_id:
        network_id = this_scenario.get('network_id')
        all_favorites = get_favorites(db, study_id=study_id, network_id=network_id)

    response = delete_scenario(scenario_id)

    return response
