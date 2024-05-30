from os import environ as env

from fastapi import APIRouter, Depends, Request
import json

from typing import List

from app.deps import get_g
from app.schemas import ResourceScenarioData

from app.core.data import get_scenarios_data, make_eval_data, filter_input_data, prepare_dataset, \
    filter_results_data
from app.core.favorites import get_favorite
from app.core.pivot import save_pivot_input

api = APIRouter(prefix='/data', tags=['Data'])


# Data API for the OpenAgua app. This varies significantly from Hydra, so Hydra is probably better for simple operations.


# params={
#     'network_id': 'The network ID',
#     'res_type': 'The resource type (node, link or network)',
#     'res_id': 'The resource ID',
#     'attr_id': 'The attribute ID',
#     'type_id': 'The template type ID',
#     'data_type': 'The data type. This is needed to populate with default data.',
#     'nblocks': 'The number of blocks (or columns) in the data, such as for piecewise linear data.',
#     'language': 'The default language input for text-based rules (default=python)。',
#     'flavor': 'Language flavor. This can help with interpreting input for preview.',
#     'settings': 'Time settings (start, end and step)',
#     'network_folder': 'Network folder. This is from the network layout.'
# }

@api.get('/resource_scenario_data', description='Get data from a resource scenario.')
def _get_resource_scenario_data(
        network_id: int,
        res_type: str,
        res_id: int | None = None,
        attr_id: int | None = None,
        lineage: List[int] = [],
        type_id: int | None = None,
        data_type: str = '',
        nblocks: int = 0,
        settings: str = '{}',
        language: str = 'python',
        flavor: str = 'openagua',
        network_folder: str | None = None,
        g=Depends(get_g)
):
    time_settings = json.loads(settings)
    files_path = network_folder

    attr = g.hydra.call('get_attribute_by_id', attr_id)

    kwargs = dict(
        scenario_ids=lineage,
        network_id=network_id,
        resource_type=res_type,
        resource_id=res_id,
        type_id=type_id,
        attr_id=attr_id,
        data_type=data_type,
        time_settings=time_settings,
        files_path=files_path,
        nblocks=nblocks,
        flavor='json',
        for_eval=True,
        function_language=(language, flavor)
    )
    attr_data = get_scenarios_data(g.hydra, **kwargs)

    return dict(attr_data=attr_data, attr=attr)


@api.post('/resource_scenario_data', description='Add data to a resource scenario.')
def _post_resource_scenario_data(data: ResourceScenarioData, g=Depends(get_g)):
    action = data['action']
    resource_type = data['resource_type']
    resource_id = data['resource_id']
    data_type = data.get('data_type', 'timeseries')
    scenario_data = data['scenario_data']
    attr_id = data['attr_id']
    attr_is_var = data.get('attr_is_var')
    res_attr_id = data.get('res_attr_id')
    unit_id = data.pop('unit_id', None)
    variation = data.pop('variation', None)
    time_settings = data.pop('settings', None)
    language = data.pop('language', None)
    flavor = data.pop('flavor', None)

    network_folder = data.get('network_folder')
    files_path = network_folder

    scenario_id = scenario_data.get('id')

    # PREPARE DATA
    attr = g.hydra.call('get_attribute_by_id', attr_id)
    user_id = g.datauser.id
    user_email = g.current_user.email
    dataset = prepare_dataset(scenario_data, unit_id, attr, data_type, user_id, user_email)
    res_attr = None

    # SAVE DATA
    if action == 'save':
        # TODO: In the future the dataset should be created in the client machine, and this can be just a pass-through,
        # so no need for an extra save_data function

        # add resource attribute if it doesn't exist
        if not res_attr_id:
            # def add_resource_attribute(resource_type, resource_id, attr_id, is_var, error_on_duplicate=True, **kwargs):
            res_attr = g.hydra.call(
                'add_resource_attribute',
                resource_type.upper(),
                resource_id,
                attr_id,
                False,
            )
            res_attr_id = res_attr['id']

        if variation:
            scenario = g.hydra.call('get_scenario', scenario_id)
            variations = scenario['layout'].get('variations', [])
            if variation.get('id'):
                variations = [variation if v.get('id') == variation['id'] else v for v in variations]
            else:
                variation['id'] = variations[-1].get('id', 0) + 1 if variations else 1
                variations.append(variation)
            scenario['layout']['variations'] = variations
            g.hydra.call('update_scenario', scenario)

        # result = g.hydra.call('add_data_to_attribute', scenario_id, res_attr_id, dataset)
        resource_scenarios = [dict(
            resource_attr_id=res_attr_id,
            dataset=dataset
        )]
        result = g.hydra.call('update_resourcedata', scenario_id, resource_scenarios)

        if 'error' in result:
            status = -1
            errmsg = json.dumps(result)

            result = {
                # 'id': scenario_id,
                'status': status,
                'errcode': -1,
                'errmsg': errmsg,
                'eval_value': None
            }

            return dict(result=result)

        else:
            status = 1

    else:
        status = 0  # no save attempt - just report error

    # CHECK DATA

    scen_id = scenario_data['id']
    errcode = 0
    errmsg = ''
    try:
        eval_value = make_eval_data(
            scenario_id=scen_id,
            hydra=g.hydra,
            data_type=data_type,
            files_path=files_path,
            time_settings=time_settings,
            dataset=dataset,
            function_language=(language, flavor)
        )
    except Exception as err:
        if hasattr(err, 'code'):
            errcode = err.code
            errmsg = err.message
        else:
            errcode = -1
            errmsg = str(err)
        eval_value = None

    result = {
        'id': scen_id,
        'status': status,
        'errcode': errcode,
        'errmsg': errmsg,
        'eval_value': eval_value
    }

    return dict(result=result, res_attr=res_attr, variation=variation)


@api.get('/pivot_input')
def _get_pivot_input(template_id: int, network_id: int, favorite_id: int = 0, filters: str = '{}', g=Depends(get_g)):
    if favorite_id:
        favorite = get_favorite(g.db, favorite_id=favorite_id)
        if favorite:
            filters = favorite.filters
            pivot = favorite.pivot
        else:
            return dict(error=1)  # no favorite found
    else:
        filters = json.loads(filters)
        pivot = {'aggregatorName': 'Unique Values'}  # renderer is defined in Utilities.js

        input_method = filters.get('input_method')

        data_type = filters.get('data_type', 'timeseries')

        if input_method == 'native':
            if data_type == 'timeseries':  # TODO: add more types
                pivot['cols'] = ['Scenario', 'Feature type', 'Feature', 'Variable']
                pivot['rows'] = ['Date']  # TODO: customize according to the model timestep
            if data_type in ['scalar', 'descriptor']:  # TODO: add more types
                pivot['cols'] = ['Scenario', 'Variable']
                pivot['rows'] = ['Feature type', 'Feature']  # TODO: customize according to the model timestep
        else:
            pivot['cols'] = ['Scenario', 'Variable']
            pivot['rows'] = ['Feature type', 'Feature']

    # filter and organize the data
    result = filter_input_data(
        g.hydra,
        network_id=network_id,
        template_id=template_id,
        filters=filters,
        maxrows=100000,
    )

    if not favorite_id:
        if 'Block' in result:
            blocks = result['Block'].unique()
            if not (len(blocks) == 1 and blocks[0] == 'None'):
                pivot['cols'].append('Block')
        pivot['vals'] = ['value']

    pivot['hiddenFromAggregators'] = [c for c in result['columns'] if c != 'value']

    data = result.to_dict(orient='records')

    return dict(data=data, pivot=pivot)


@api.put('/pivot_input')
async def _update_pivot_input(request: Request, g=Depends(get_g)):
    error = 0
    request_data = await request.json()
    network_id = request_data.get('network_id')
    pivot = request_data.get('pivot')
    filters = request_data.get('filters')
    data = request_data.get('data')

    network = g.hydra.call('get_network', network_id, include_resources=True, include_data=False,
                           summary=False)
    template = g.hydra.call('get_template', network['layout'].get('active_template_id'))

    error = save_pivot_input(pivot, filters, data, network, template, env['DATA_DATETIME_FORMAT'])

    return dict(error=error)


@api.get('/pivot_results')
def _get_pivot_results(network_id: int, template_id: int, project_id: int, favorite_id: int = 0, filters: str = '{}',
                       g=Depends(get_g)):
    filters = json.loads(filters)

    agg = filters.get('agg', {})

    if not favorite_id:
        filters['attr_data_type'] = 'timeseries'  # TODO: get from user filters
    data_type = filters.get('attr_data_type', 'timeseries')

    # filter and organize the data
    data, perturbations = filter_results_data(
        g.hydra, filters=filters, network_id=network_id, template_id=template_id,
        project_id=project_id, maxrows=500000, include_tags=False)

    if type(data) == int:
        return dict(error=data)
    elif data is None:
        return dict(error=-3)

    if favorite_id:
        favorite = get_favorite(g.db, favorite_id=favorite_id)
        if favorite:
            pivot = favorite.pivot
        else:
            return dict(error=1)  # no favorite found
    else:
        default_chart_renderer = env['DEFAULT_CHART_RENDERER']
        pivot = {
            'renderer': default_chart_renderer,
            'rendererName': 'Line Chart',
            'aggregatorName': 'Average',
            'rows': [],
            'cols': [],
            'type': 'results'
        }

        time_step = agg.get('time', {}).get('step')
        if len(filters.get('scenarios', [])) > 1:
            pivot['rows'].append('Scenario')

        if data_type == 'timeseries':  # TODO: add more types
            pivot['renderer'] = default_chart_renderer,
            if len(filters.get('resources', [])) > 1 and not agg.get('space'):
                pivot['rows'].append('Feature')
            if not filters.get('unstack'):
                pivot['rows'].append('Variable')
            if 'block' in data and len(set(data.block)) > 1:
                pivot['rows'].append('Block')

            if time_step == 'year':
                pivot['cols'] = ['Year']
            else:
                pivot['cols'] = ['Date']

        if perturbations:
            pivot['rows'].extend(perturbations)

    columns = list(data.columns)
    data = data.to_json(orient='values', date_format='iso')

    return dict(columns=columns, values=data, pivot=pivot, error=None)


@api.get('/table')
def _get_data_table(
        scenarios: list[str],
        network: str = '',
        nodes: list[str] = [],
        links: list[str] = [],
        attributes: list[str] = [],
        resource_attributes: list[str] = []
):
    table = None
    return table
