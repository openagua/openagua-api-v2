from os import environ as env

import json
from copy import copy
from itertools import product
from ast import literal_eval
import queue

from threading import Thread

from datetime import datetime
# import dask.dataframe as dd
import pandas as pd

from app.core.evaluators import OpenAguaEvaluator, PywrEvaluator
from app.core.evaluators.utils import make_default_value, empty_data_timeseries, make_timesteps

from app.core.templates import get_tattrs
from app.core.scenarios import get_data_scenarios


def make_eval_data(hydra=None, dataset=None, function_language=('python', 'openagua'), **kwargs):
    evaluator = get_evaluator(function_language, hydra=hydra, **kwargs)

    try:
        data_type = kwargs.get('data_type')
        eval_value = evaluator.eval_data(dataset=dataset, fill_value=None)
        if 'timeseries' in data_type and type(eval_value) in [int, float] or eval_value is None:
            eval_value = make_default_value(data_type=data_type, dates=evaluator.dates, default_value=eval_value)
        return eval_value
    except:
        raise


def get_evaluator(function_language, **kwargs):
    if function_language == ('python', 'openagua'):
        evaluator = OpenAguaEvaluator(**kwargs)
    elif function_language == ('python', 'pywr'):
        evaluator = PywrEvaluator(**kwargs)
    else:
        evaluator = OpenAguaEvaluator(**kwargs)

    return evaluator


def get_scenarios_data(hydra, scenario_ids, function_language=('python', 'openagua'), **kwargs):
    evaluator = get_evaluator(function_language, hydra=hydra, **kwargs)
    scenarios_data = []
    for i, scenario_id in enumerate(scenario_ids):
        evaluator.scenario_id = scenario_id
        scenario_data = get_scenario_data(evaluator, **kwargs)

        # scenario_data['note'] = ''

        if scenario_data['dataset'] is None and i:
            scenario_data = copy(scenarios_data[i - 1])
            scenario_data.update({
                'id': scenario_id,
                'dataset': None,
                'error': 2,
            })

        elif scenario_data['dataset']:
            scenario_data['note'] = scenario_data['dataset']['metadata'].get('note', '')

        scenarios_data.append(scenario_data)

    return scenarios_data


def get_scenario_data(evaluator, **kwargs):
    kwargs['scenario_id'] = [evaluator.scenario_id]
    # for_eval = kwargs.get('for_eval', False)
    res_attr_data = evaluator.hydra.get_res_attr_data(**kwargs)
    data_type = kwargs.get('data_type')
    # eval_value = None
    time_step = kwargs.get('time_settings', {}).get('time_step')
    error = 0
    if res_attr_data and 'error' not in res_attr_data:
        dataset = res_attr_data[0]['dataset']
        data_type = data_type or dataset.type

        try:
            eval_value = evaluator.eval_data(
                dataset=dataset,
                data_type=kwargs.get('data_type'),
                flatten=False,
                # for_eval=for_eval,
            )
        except:
            eval_value = None
            error = 1

        if eval_value is None:
            eval_value = make_default_value(data_type=data_type, dates=evaluator.dates, default_value=None,
                                            time_step=time_step)
        elif type(eval_value) in [int, float] and 'timeseries' in data_type:
            eval_value = make_default_value(data_type=data_type, dates=evaluator.dates, default_value=eval_value,
                                            time_step=time_step)

        # metadata = json.loads(dataset['value']['metadata'])
        # metadata['use_function'] = metadata.get('use_function', 'N')
        # metadata['function'] = metadata.get('function', '')
        # dataset['value']['metadata'] = metadata

        scenario_data = {
            'dataset': dataset,
            'eval_value': eval_value,
            'error': error
        }

    else:
        if data_type in ['timeseries', 'periodic timeseries']:
            eval_value = make_default_value(data_type, evaluator.dates, default_value=None, time_step=time_step)
        elif data_type == 'array':
            eval_value = make_default_value(data_type)
        else:
            eval_value = None

        empty_dataset = {
            'value': None,
            'metadata': {}
        }

        scenario_data = {'dataset': empty_dataset, 'eval_value': eval_value, 'error': 1}

    scenario_data['id'] = evaluator.scenario_id

    return scenario_data


def prepare_dataset(scenario_data, unit_id, attr, data_type, user_id, user_email):
    dataset = scenario_data['dataset']
    # dataset = data['value'] if data is not None and 'value' in data else {}
    data_type = 'timeseries' if data_type == 'periodic timeseries' else data_type
    metadata = dataset.get('metadata', {})
    now_str = '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now())
    # input_method = metadata.get('input_method')
    # url = metadata.get('url', '{}') if input_method == 'url' else '{}'
    metadata.update({
        'note': scenario_data.get('note', ''),
        'modified_date': now_str,
        'modified_by': user_id
    })
    if 'function' in metadata:
        metadata['function'] = ''
    if 'use_function' in metadata:
        metadata['use_function'] = ''
    if 'source' not in metadata:
        metadata['source'] = 'OpenAgua/%s' % user_email
    if 'cr_date' not in metadata:
        metadata['cr_date'] = now_str

    emptyvalue = {
        'timeseries': '{}',
        'array': '[[],[]]',
        'scalar': '0',
        'descriptor': ''
    }

    # NB: results should be stored in a different resource scenario.
    # However, per a user's request, the eval data is also stored here
    # value = scenario_data.get('value', emptyvalue[data_type])
    value = dataset['value']
    if type(value) != str:
        if data_type == 'timeseries':
            value = json.dumps(value)
        elif data_type == 'array':
            def parse_row(item):
                if type(item) != list:
                    try:
                        val = literal_eval(item)
                        if type(val) in [float, int]:
                            return val
                        else:
                            return item
                    except:
                        return item
                else:
                    return [parse_row(v) for v in item]

            new_array = parse_row(value)
            value = json.dumps(new_array)
        elif data_type == 'scalar':
            value = str(value)

    dataset.update({
        'id': None,
        'name': attr['name'],
        'unit_id': unit_id,
        'dimension': attr.dimension_id,
        'type': data_type,
        'value': value,
        'metadata': json.dumps(metadata, ensure_ascii=True).encode()
    })

    return dataset


def get_parent_scenarios(hydra, network_id, scenario_id):
    all_scenarios = get_data_scenarios(hydra, network_id)
    all_scenarios = {s['id']: s for s in all_scenarios}

    scenarios = [scenario_id]

    def add_parents(sid):
        scen = all_scenarios[sid]
        if 'parent' in scen['layout'] and scen['layout']['parent'] is not None:
            parent_id = scen['layout']['parent']
            scenarios.append(parent_id)
            add_parents(parent_id)

    add_parents(scenario_id)

    return scenarios


def get_sorted_scenarios(scenarios, flavor='list'):
    sorted_scenarios = {}
    for c in ['baseline', 'option', 'scenario']:
        sorted_scenarios[c] = []
    for scenario in scenarios:
        if scenario['name'] == 'Baseline':
            sorted_scenarios['baseline'].append(scenario)
        else:
            scenario_class = scenario['layout'].get('class')
            if scenario_class:
                if scenario_class not in ['option', 'scenario']:
                    continue
                else:
                    sorted_scenarios[scenario_class].append(scenario)
            else:
                if 'unclassified' not in scenarios:
                    sorted_scenarios['unclassified'] = []
                sorted_scenarios['unclassified'].append(scenario)

    if flavor == 'list':
        scenarios = [{'name': name, 'children': sorted_scenarios[name]} for name in sorted_scenarios]
    else:
        scenarios = sorted_scenarios

    return scenarios


class net:
    def __init__(self, hydra=None, scenario_id=None):
        self.hydra = hydra
        self.scenario_id = scenario_id

    def network(self, res_type, res_id, type_id):
        data = self.hydra.call('get_{}_data'.format(res_type), {
            '{}_id'.format(res_type): res_id,
            'scenario_id': self.scenario_id,
            'type_id': type_id
        })

        return data


def save_data(hydra, res_attr_id, attr, unit, data_type, scenario_data, metadata):
    if metadata['use_function'] == 'Y' and data_type == 'timeseries':
        value = '{}'  # results should be stored in a different resource scenario
    else:
        value = scenario_data['value']
        if data_type == 'timeseries' and type(value) != str:
            value = json.dumps(value)

    dataset = {
        'id': None,
        'name': attr['name'],
        'unit': unit,
        'dimension': attr.dimen,
        'type': data_type,
        'value': value,
        'metadata': json.dumps(metadata)
    }

    kwargs = {'scenario_id': scenario_data['id'],
              'resource_attr_id': res_attr_id,
              'dataset': dataset}
    result = hydra.call('add_data_to_attribute', **kwargs)

    if 'faultcode' in result.keys():
        returncode = -1
    else:
        returncode = 0
    return returncode


def filter_input_data(hydra, network_id, template_id, filters, maxrows=None, include_tags=False):
    scenarios = filters.get('scenarios', [])
    attrs = filters.get('attrs')
    ttypes = filters.get('ttypes')
    input_method = filters.get('input_method', 'native')
    use_function = filters.get('use_function') or input_method == 'function'

    data_type = filters.get('data_type', 'timeseries')

    dfs = []
    # nRows = 0

    if input_method != 'native':
        idx_names = ['Scenario', 'Feature type', 'Feature', 'Variable']
    else:
        if data_type == 'timeseries':
            idx_names = ['Date', 'Year', 'Month', 'Scenario', 'Feature type', 'Feature', 'Variable', 'Block']
        elif data_type in ['scalar', 'descriptor']:
            idx_names = ['Scenario', 'Feature type', 'Feature', 'Variable']
        elif data_type == 'periodic timeseries':
            idx_names = ['Year', 'Month', 'Scenario', 'Feature type', 'Feature', 'Variable', 'Block']

    tag_names = []

    # get resource_attributes
    template = hydra.call('get_template', template_id)

    ttype_dict = {tt['id']: tt for tt in template.templatetypes}
    tattr_dict = get_tattrs(template)

    res_info = {}
    scenario_data_kwargs = {'scenario_id': scenarios}
    networks = []
    nodes = []
    links = []

    resources = hydra.call('get_resources_of_type', network_id=network_id, type_id=ttypes)
    for resource in resources:
        ref_key = resource['ref_key']
        if ref_key == 'LINK':
            links.append(resource['id'])
        elif ref_key == 'NODE':
            nodes.append(resource['id'])
        else:
            networks.append(resource['id'])
        type_id = [t['id'] for t in resource['types'] if t['template_id'] == template['id']][0]
        for ra in resource.attributes:
            # if not incl_vars and ra['attr_is_var'] == 'Y':
            #     continue
            if ra['attr_id'] not in attrs:
                continue
            if type_id not in ttypes:
                continue
            # res_attr = {'res_name': resource['name'], 'res_type': ttype}
            # res_attr.update(tattrs[ra['attr_id']])
            res_info[ra['id']] = {
                'name': resource['name'],
                'type_name': ttype_dict[type_id]['name'],
                'attr_name': tattr_dict[ra['attr_id']]['attr']['name']
            }

    if networks:
        scenario_data_kwargs['network_ids'] = networks
    if nodes:
        scenario_data_kwargs['node_ids'] = nodes
    if links:
        scenario_data_kwargs['link_ids'] = links
    if attrs:
        scenario_data_kwargs['attr_id'] = attrs
    if ttypes:  # doesn't appear to have any effect in Hydra
        scenario_data_kwargs['type_id'] = ttypes

    scens = hydra.call('get_scenarios_data', **scenario_data_kwargs)

    scenario_lookup = {}
    if 'timeseries' in data_type:
        network = hydra.call('get_network', network_id, include_data=True, include_resources=False,
                             summary=False)
        scenario_lookup = {s['id']: s for s in network.scenarios}

    nrows = 0
    for sc in scens:
        scen_name = sc['name']

        if 'timeseries' in data_type:
            start_time = sc.get('start_time')
            end_time = sc.get('end_time')
            time_step = sc.get('time_step')

            child = sc

            while not (start_time and end_time and time_step):
                if child['layout'].get('class') == 'baseline':
                    return None
                else:
                    parent_id = child['layout'].get('parent')
                    ancestor = scenario_lookup.get(parent_id)
                    start_time = ancestor.get('start_time')
                    end_time = ancestor.get('end_time')
                    time_step = ancestor.get('time_step')
                    child = ancestor

                empty_timeseries = make_empty_timeseries(scenario=sc)

        resourcescenarios = {rs.resource_attr_id: rs for rs in sc.resourcescenarios}

        for ra_id, res in res_info.items():
            value = None
            if ra_id in resourcescenarios.keys():
                rs = resourcescenarios[ra_id]
                metadata = json.loads(rs.value.metadata)

                if input_method != 'native':
                    if input_method in metadata:
                        value = metadata[input_method]
                    else:
                        value = metadata['data']
                else:
                    if data_type == 'timeseries':
                        value = pd.read_json(rs.value.value or '{}')
                        if value.empty:
                            value = empty_timeseries.copy(deep=True)
                    else:
                        value = rs.value.value


            else:
                if input_method != 'native':
                    value = ''
                elif data_type == 'timeseries':
                    value = empty_timeseries.copy(deep=True)
                elif data_type == 'scalar':
                    value = ''
                elif data_type == 'descriptor':
                    value = ''
                metadata = {}
                # continue

            if input_method != 'native' or data_type not in ['timeseries', 'periodic timerseries', 'array']:
                df = pd.DataFrame([value], columns=['value'])

            # the following needs updating if more than one timeseries item, but it is otherwise effective
            elif data_type in ['timeseries', 'periodic timeseries']:
                # df = pd.read_json(value)
                df = value
                df.index.name = 'date'
                df.reset_index(inplace=True)
                df = pd.melt(df, id_vars=['date'], var_name='Block', value_name='value')
                # df.set_index(['date','Block'])

                if data_type == 'timeseries':
                    df['Date'] = df.date.dt.strftime('%Y-%m-%d')
                df['Year'] = df.date.dt.year
                df['Month'] = df.date.dt.month  # TODO: get smallest time unit from settings?

                # TODO: Delete these - "has_blocks" should be specified in template attribute layout
                attr_name = res['attr_name'].lower()
                if 'demand' not in attr_name or 'priority' not in attr_name:
                    df['Block'] = 'None'

            df['Scenario'] = scen_name
            df['Feature'] = res['name']
            df['Feature type'] = res['type_name']
            df['Variable'] = res['attr_name']
            if 'date' in df:
                del df['date']

            nrows += len(df)
            if maxrows and nrows > maxrows:
                return -1

            # add value tags
            if include_tags:
                value_tags = get_value_tags(hydra, sc.id)

                if value_tags:
                    for vt in value_tags:
                        df[vt['name']] = vt.value
                        tag_names.append(vt['name'])

            dfs.append(df)

    if dfs:
        data = pd.concat(dfs)
        data.fillna('', inplace=True)

        data.set_index(idx_names, inplace=True)

        # variables = data.index.get_level_values('Variable').unique()

        data.reset_index(col_level=0, inplace=True)

        if type(data) == int and data == -1:
            return

        return data


def make_empty_timeseries(scenario, dates_as_string=None, res_info=None):
    empty_timeseries = None
    if dates_as_string:
        empty_timeseries = empty_data_timeseries(dates_as_string, flavor='pandas')
    elif res_info:
        for rs in scenario.get('resourcescenarios', []):
            res = res_info.get(rs['resource_attr_id'])
            if not res:
                continue

            val = json.loads(rs['value']['value'])
            if val:
                empty_timeseries = pd.read_json(rs['value']['value']) * 0
                break

    return empty_timeseries


def get_data_from_hydra(hydra, network_id, scenarios, networks, nodes, links, ttypes, attrs, include_tags=True,
                        maxrows=100000):
    # 1. Get the data
    scenario_ids = list(set(scenarios))

    attr_ids = attrs if attrs else None
    type_ids = ttypes if ttypes else None

    kwargs = {}
    kwargs['network_ids'] = networks
    kwargs['node_ids'] = nodes
    kwargs['link_ids'] = links

    scens = hydra.call('get_scenarios_data', scenario_ids, attr_ids, type_ids, **kwargs)

    if 'error' in scens:
        return -2

    # 2. Organize the data to send back to the client

    dfs = []
    tag_names = []
    empty_timeseries = None

    res_attrs = {}

    source_scenario_ids = []
    source_scenarios = {}
    for sc in scens:
        for v in sc['layout'].get('variation', []):
            if v['scenario_id'] not in source_scenario_ids:
                source_scenario_ids.append(v['scenario_id'])
    if source_scenario_ids:
        source_scenarios = hydra.call('get_scenarios', network_id, scenario_ids=source_scenario_ids)
        source_scenarios = {s['id']: s for s in source_scenarios}

    nrows = 0

    perturbations = []

    for sc in scens:
        # scen_name = sc['name']

        variation = sc['layout'].get('variation', [])  # variation = perturbation
        for v in variation:
            source_scenario = source_scenarios[v['scenario_id']]
            variation_sets = {v['id']: v for v in source_scenario['layout'].get('variation_sets', [])}
            variation_set = variation_sets[v['variation_set_id']]
            variation_set_name = variation_set['name']
            if variation_set_name not in perturbations:
                perturbations.append(variation_set_name)

        for rs in sc['resourcescenarios']:
            dataset = rs.get('dataset')
            dataset_id = rs.get('dataset_id')
            if not dataset and dataset_id:
                # todo: fix in Hydra, since clearly this is an error
                dataset = hydra.call('get_dataset', dataset_id)
            if not dataset:
                continue
            value = dataset['value']
            if not json.loads(value):
                if empty_timeseries is None:
                    timesteps = make_timesteps(start=sc['start_time'], end=sc['end_time'], span=sc['time_step'])
                    dates_as_string = [t.date_as_string for t in timesteps]
                    empty_timeseries = make_empty_timeseries(scenario=sc, dates_as_string=dates_as_string)
                df = empty_timeseries
            else:
                # the following needs updating if more than one timeseries item, but it is otherwise effective
                df = pd.read_json(value)
            df.index.name = 'date'
            df.reset_index(inplace=True)
            df['scenario_id'] = sc['id']
            df['resource_attr_id'] = rs['resource_attr_id']

            if rs['resource_attr_id'] not in res_attrs:
                res_attrs[rs['resource_attr_id']] = hydra.call('get_resource_attribute', rs['resource_attr_id'])
            df['attr_id'] = res_attrs[rs['resource_attr_id']]['attr_id']

            # # add variations/perturbations
            for v in variation:
                source_scenario = source_scenarios[v['scenario_id']]
                variation_sets = {v['id']: v for v in source_scenario['layout'].get('variation_sets', [])}
                variation_set = variation_sets[v['variation_set_id']]
                variation_set_name = variation_set['name']
                variation_val = v['variation']
                if isinstance(variation_val, dict):
                    variation_val = variation_val['name']

                # variation_name = f'{variation_set_name} {variation_val:02}'
                df[variation_set_name] = variation_val

            id_vars = ['scenario_id'] + perturbations + ['resource_attr_id', 'attr_id', 'date']
            df = pd.melt(df, id_vars=id_vars, var_name='block', value_name='value')

            nrows += len(df)
            if maxrows and nrows > maxrows:
                return -1

            # add value tags
            if include_tags:
                value_tags = get_value_tags(hydra, sc['id'])

                if value_tags:
                    for vt in value_tags:
                        df[vt['name']] = vt.value
                        tag_names.append(vt['name'])

            dfs.append(df)

    if include_tags:
        tag_names = set(tag_names)
        for i in range(len(dfs)):
            for tag_name in tag_names:
                if tag_name not in dfs[i]:
                    dfs[i][tag_name] = None
    else:
        tag_names = []

    return dfs, perturbations, tag_names


def get_data_from_store(hydra, network, template_id, scenario, version, network_ids, node_ids, link_ids, attr_ids,
                        root_key,
                        data_location='s3', include_tags=False, maxrows=100000):
    bucket_name = env['AWS_S3_BUCKET']

    run_name = scenario['layout'].get('run')
    human_readable = version.get('human_readable', False)

    node_names = []
    link_names = []
    attr_names = []
    attr_id_lookup = {}
    res_id_lookup = {}
    rt_lookup = {}
    names = {'node': [], 'link': [], 'network': []}

    # if human_readable:

    def populate_lists(res_type, resource_ids):
        kwargs = {'{}_ids'.format(res_type): resource_ids}
        if res_type == 'network':
            resources = [network]
        else:
            resources = hydra.call('get_{}s'.format(res_type), **kwargs)
        for resource in resources:
            names[res_type].append(resource['name'])
            res_id_lookup[(res_type, resource['name'])] = resource['id']
            types = [rt for rt in resource['types'] if rt['template_id'] == template_id]
            if types:
                rt = types[0]
                rt_lookup[
                    (res_type, resource['name'] if human_readable else resource['id'])] = rt[
                    'name'] if human_readable else rt['id']
            for ra in resource['attributes']:
                if ra['attr_id'] in attr_ids:
                    if ra['name'] not in attr_names:
                        attr_names.append(ra['name'])
                        attr_id_lookup[ra['name']] = ra['attr_id']

    if node_ids:
        populate_lists('node', node_ids)
    if link_ids:
        populate_lists('link', link_ids)
    elif network_ids:
        populate_lists('network', network_ids)

    # 1. Get the data
    if network_ids:
        csv_path_template = '{scenariokey}/{subscenario}/{type}/{attr}.csv'
    else:
        csv_path_template = '{scenariokey}/{subscenario}/{type}/{subtype}/{resource}/{attr}.csv'
    if data_location == 'hdf5':
        csv_path_template = csv_path_template.replace('.csv', '')
        run_name = run_name.replace(' ', '_')

    version_id = version.get(scenario['layout'].get('version_key', 'date'))

    if '127.0.0.1' in current_app.s3fs.client_kwargs.get('endpoint_url', ''):
        base_path = '/mnt/data/'
    else:
        base_path = 's3://'

    scenariokey = '{base_path}{bucket_name}/{root_key}/results/{run_name}/{version}/{scenario}'.format(
        base_path=base_path,
        bucket_name=bucket_name,
        root_key=root_key,
        run_name=run_name,
        version=version_id,
        scenario=scenario['name'] if human_readable else scenario['id']
    )

    scenario_key = None
    subscenarios = [1]  # default if no variations
    perturbations = None
    if version.get('variations'):
        # scenario_key_path = '{}/scenario_key.csv'.format(scenariokey)
        scenario_key_path = '{}/scenario_key.csv'.format(scenariokey)
        if scenario_key_path[0] == '/':
            scenario_key = pd.read_csv(scenario_key_path, index_col=0)
        else:
            scenario_key = pd.read_csv(current_app.s3fs.open(scenario_key_path, mode='rb'), index_col=0)
        subscenarios = scenario_key.index
        perturbations = list(scenario_key.columns)

    empty_timeseries = None

    # timesteps = make_timesteps(start=scenario.start_time, end=scenario.end_time, span=scenario.time_step)
    # dates_as_string = [t.date_as_string for t in timesteps]
    # empty_timeseries = make_empty_timeseries(scenario=scenario, dates_as_string=dates_as_string)

    # def bulk_download_data2(s3fs, resource_type, combos, scenario_key):
    #     names = ['date'] + [0]
    #     for resource_id, attr_id in combos:
    #         path = csv_path_template.format(
    #             scenariokey=scenariokey,
    #             subscenario='*',
    #             type=resource_type,
    #             resource=resource_id,
    #             attr=attr_id
    #         )
    #         df = dd.read_csv(path, skiprows=1, names=names)
    #     dfs = []
    #
    #     return dfs

    def bulk_download_data(s3fs, resource_type, combos, scenario_key):

        dfs = []

        def get_single_csv(s3fs, scenario_id, empty_timeseries, combo, q=None):
            subscenario, resource_id, attr_id = combo

            key = csv_path_template.format(
                scenariokey=scenariokey,
                subscenario=subscenario,
                type=resource_type,
                subtype=rt_lookup[(resource_type, resource_id)],
                resource=resource_id,
                attr=attr_id
            )

            # if human_readable:
            #     key = csv_path_template.format(
            #         scenariokey=scenariokey,
            #         subscenario=subscenario,
            #         type=resource_type,
            #         subtype=rt_lookup[(resource_type, resource_id)],
            #         resource=resource_id,
            #         attr=attr_id
            #     )
            # else:
            #     key = csv_path_template.format(
            #         scenariokey=scenariokey,
            #         subscenario=subscenario,
            #         type=resource_type,
            #         subtype=rt_lookup[(resource_type, resource_id)],
            #         resource=resource_id,
            #         attr=attr_id
            #     )

            # df = empty_timeseries
            try:
                if data_location == 's3':
                    if scenariokey[0] == '/':
                        df = pd.read_csv(key, skiprows=1, names=['date', 0])
                    else:
                        df = pd.read_csv(s3fs.open(key, mode='rb'), skiprows=1, names=['date', 0])
                elif data_location == 'hdf5':
                    df = pd.read_hdf('~/store.hdf5', key.replace(base_path, ''))
            except:
                if empty_timeseries is None:
                    dates_as_string = make_timesteps(start=scenario['start_time'], end=scenario['end_time'],
                                                     span=scenario['time_step'], format='iso')
                    empty_timeseries = make_empty_timeseries(scenario=scenario, dates_as_string=dates_as_string)
                    empty_timeseries[0] = None
                    empty_timeseries.reset_index(inplace=True)
                df = empty_timeseries

            # df['scenario_id'] = scenario_id
            #
            # if human_readable:
            #     resource_id = res_id_lookup.get((resource_type, resource_id))
            #     attr_id = attr_id_lookup.get(attr_id)
            # df['resource_key'] = '%s/%s' % (resource_type, resource_id)
            # df['attr_id'] = attr_id
            #
            # id_vars = ['scenario_id']
            # if scenario_key is not None:
            #     for col in scenario_key.columns:
            #         df[col] = scenario_key[col][subscenario]
            #         id_vars.append(col)
            # df = pd.melt(df, id_vars=id_vars + ['resource_key', 'attr_id', 'date'], var_name='block',
            #              value_name='value')

            if q:
                q.put(df)
            else:
                return df

        if scenario_key_path[0] == '/':
            dfs = [get_single_csv(s3fs, scenario['id'], empty_timeseries, combo) for combo in combos]

        else:
            processes = []
            q = queue.Queue()
            for combo in combos:
                thread = Thread(target=get_single_csv, args=(s3fs, scenario['id'], empty_timeseries, combo, q,))
                thread.start()
                processes.append(thread)
            for p in processes:
                p.join()

            while not q.empty():
                df = q.get()
                dfs.append(df)

        updated = []
        for i, df in enumerate(dfs):
            subscenario, resource_id, attr_id = combos[i]

            df['scenario_id'] = scenario['id']

            if human_readable:
                resource_id = res_id_lookup.get((resource_type, resource_id))
                attr_id = attr_id_lookup.get(attr_id)
            df['resource_key'] = '%s/%s' % (resource_type, resource_id)
            df['attr_id'] = attr_id

            id_vars = ['scenario_id']
            if scenario_key is not None:
                for col in scenario_key.columns:
                    df[col] = scenario_key[col][subscenario]
                    id_vars.append(col)
            df = pd.melt(df, id_vars=id_vars + ['resource_key', 'attr_id', 'date'], var_name='block',
                         value_name='value')
            updated.append(df)

        return updated

    tag_names = []
    nrows = 0

    all_dfs = []

    def get_resource_type_data(resource_type, resources):
        combos = list(product(*[subscenarios, resources, attr_names if human_readable else attr_ids]))
        return bulk_download_data(current_app.s3fs, resource_type=resource_type, combos=combos,
                                  scenario_key=scenario_key)
        # combos = list(product(*[resources, attr_names if human_readable else attr_ids]))
        # return bulk_download_data2(current_app.s3fs, resource_type=resource_type, combos=combos,
        #                            scenario_key=scenario_key)

    if node_ids or link_ids or network_ids:
        dfs = []
        if node_ids:
            dfs = get_resource_type_data('node', names['node'] if human_readable else node_ids)
        elif link_ids:
            dfs = get_resource_type_data('link', names['link'] if human_readable else link_ids)
        elif network_ids:
            dfs = get_resource_type_data('network', names['network'] if human_readable else network_ids)
        all_dfs.extend(dfs)

    tag_names = []

    return all_dfs, perturbations, tag_names


def aggregate_data(data, agg, idx_names):
    if agg:

        data = data.reset_index()
        data['date'] = pd.to_datetime(data['date'])

        # time filter
        range = agg.get('range', {})
        if range and range.get('mode') == 'custom':
            start = range.get('start')
            end = range.get('end')
            if start or end:
                data.set_index(['date'])
                if start and end:
                    mask = (data['date'] >= start) & (data['date'] <= end)
                elif start and not end:
                    mask = data['date'] >= start
                else:
                    mask = data['date'] <= end
                data = data.loc[mask]
                data.reset_index()

        # spatial aggregation
        spatial = agg.get('space', {})
        f = spatial.get('function')
        if f in ['sum', 'mean']:
            idx_names.remove('resource_key')
            data = data.groupby(idx_names).agg(f).reset_index()

        # temporal aggregation
        temporal = agg.get('time', {})
        f = temporal.get('function')
        p = temporal.get('step')

        if p and f in ['sum', 'mean']:
            # https://stackoverflow.com/questions/35295689/how-to-aggregate-data-by-date-and-string-multiindex-using-pandas-then-print-to
            idx_names.remove('date')
            if p == 'month':
                time_group = ['year', 'month', 'day']
                data['year'] = data['date'].dt.year
                data['month'] = data['date'].dt.month
                data['day'] = 1
                data = data.groupby(idx_names + time_group).agg(f).reset_index()
                data['date'] = pd.to_datetime(data[time_group])
                cols = list(data.columns)
                cols.remove('date')
                for tg in time_group:
                    cols.remove(tg)
                cols.insert(cols.index('block'), 'date')
                data = data[cols]
            elif p == 'year':
                data['Year'] = data['date'].dt.year
                del data['date']
                data = data.groupby(idx_names + ['Year']).agg(f).reset_index()
                cols = list(data.columns)
                cols.remove('Year')
                cols.insert(cols.index('block'), 'Year')
                data = data[cols]

        new_cols = list(data.columns)
        new_cols.remove('value')
        data.set_index(new_cols, inplace=True)

    return data


def filter_results_data(hydra, filters, project_id=None, network_id=None, template_id=None, maxrows=None,
                        include_tags=False):
    # TODO: Move this to Hydra or otherwise improve Hydra functions to make these queries as efficient as possible

    nodes = filters.get('nodes', [])
    links = filters.get('links', [])
    networks = filters.get('networks', [])
    scenarios = filters.get('scenarios', [])
    versions = filters.get('versions', {})
    attrs = filters.get('attrs')
    ttypes = filters.get('ttypes')
    unstack = filters.get('unstack', False)
    agg = filters.get('agg', {})

    data = []
    perturbations = None
    tag_names = []

    for scenario_id in scenarios:
        scenario = hydra.call('get_scenario', scenario_id, include_data=False)

        layout = scenario['layout']
        run_name = layout.get('run')
        data_location = layout.get('data_location', 'source')
        all_versions = scenario['layout'].get('versions', [])

        if data_location == 'source':

            parent_ids = layout.get('parent_ids')
            if parent_ids:
                # check if child scenarios exist
                child_scenarios = hydra.call('get_scenarios', network_id=network_id, parent_id=scenario_id)
                if child_scenarios:
                    scenario_ids = [s['id'] for s in child_scenarios]
                else:
                    scenario_ids = [scenario_id]

            else:
                scenario_ids = [scenario_id]

            dfs, perturbations, tag_names = get_data_from_hydra(
                hydra, network_id, scenario_ids, networks, nodes, links, ttypes, attrs,
                include_tags=include_tags, maxrows=maxrows)
            data.extend(dfs)

        elif data_location in ['s3', 'hdf5']:

            root_key = None
            network = None
            if network_id:
                network = hydra.call('get_network', network_id, include_data=False, summary=False,
                                     include_resources=False)
                root_key = network['layout'].get('storage', {}).get('folder')

            if not versions:
                if not all_versions:
                    version = None
                else:
                    version = all_versions[-1]

                dfs, perturbations, tag_names = get_data_from_store(hydra, network, template_id, scenario, version,
                                                                    networks,
                                                                    nodes, links,
                                                                    attrs, root_key,
                                                                    data_location=data_location,
                                                                    include_tags=include_tags, maxrows=maxrows)
                data.extend(dfs)

            else:
                version_lookup = {version['number']: version for version in all_versions}
                for version_id in versions.get(str(scenario_id)):
                    version = version_lookup.get(version_id)
                    dfs, perturbations, tag_names = get_data_from_store(hydra, network, template_id, scenario, version,
                                                                        networks,
                                                                        nodes, links,
                                                                        attrs, root_key,
                                                                        data_location=data_location,
                                                                        include_tags=include_tags, maxrows=maxrows)
                    data.extend(dfs)

    if data:

        data = pd.concat(data)
        data.fillna('', inplace=True)

        idx_names = [c for c in data.columns if c != 'value'] + tag_names
        data.set_index(idx_names, inplace=True)

        if agg:
            data = aggregate_data(data, agg, idx_names=idx_names)

        # reorganization
        if unstack:
            data = data.unstack('attr_id')
            data.reset_index(col_level=1, inplace=True)
            data.columns = data.columns.droplevel()
        else:
            data.reset_index(col_level=0, inplace=True)
            # del data['attr_id']

    else:
        data = -3

    return data, perturbations


def get_value_tags(hydra, scenario_id):
    value_tags = []

    def collect_tags_from(scenario_id):
        scenario = hydra.call('get_scenario', scenario_id)
        if 'value_tags' in scenario['layout']:
            for vt in scenario['layout']['value_tags']:
                value_tags.append(vt)
        if 'sources' in scenario['layout']:
            for source_id in scenario['layout']['sources']:
                collect_tags_from(source_id)
        if 'parent' in scenario['layout']:
            collect_tags_from(scenario['layout']['parent'])

    collect_tags_from(scenario_id)

    return value_tags
