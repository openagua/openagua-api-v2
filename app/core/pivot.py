import json
import pandas as pd
import numpy as np

NULL_VALUES = {
    'timeseries': '{}',
    'array': '[[]]',
}


def save_pivot_input(pivot, filters, data, network, template, data_date_format):

    error = 0

    # attributes
    pfilters = pivot['rows'] + pivot['cols']

    # for now, this is a requirement
    if not {'Scenario', 'Feature', 'Variable'}.issubset(pfilters):
        return -1

    data = np.array(data)

    # get template types
    # ttype_lookup = {(tt.resource_type, tt['name']) for tt in template.templatetypes}
    ttype_lookup = {tt['id']: tt for tt in template.templatetypes}
    resource_type = ttype_lookup[filters['ttypes'][0]]['resource_type']

    input_method = filters.get('input_method')
    data_type = filters.get('data_type', 'timeseries')

    # convert the data to a pandas dataframe for easier processing
    if input_method == 'function':
        dtype = str
    else:
        dtype = object
    df = hot_to_pd(data, pivot['cols'], pivot['rows'], dtype)

    # inclusions = pivot['inclusions'] # unclear why this was here
    colnames = df.columns.names
    rownames = df.index.names

    def get_items(class_name):
        if class_name in pivot['cols']:
            return set(df.columns.levels[colnames.index(class_name)])
        elif class_name in pivot['rows']:
            idx = pivot['rows'].index(class_name)
            return set(data[len(pivot['cols']) + 1:, idx])

    scenario_names = get_items('Scenario')
    attr_names = get_items('Variable')

    # GET RESOURCES
    if 'Feature' in pfilters:
        resource_names = get_items('Feature')
    else:
        resources = []
        ttypes = filters.get('ttypes', [])
        # TODO: change the below to get from df instead
        resource_names = [r['name'] for r in resources if len([t['id'] for t in r['types'] if t['id'] in ttypes])]

    scenario_lookup = {s['name']: s for s in network.scenarios}

    # resource lookup
    if resource_type == 'NODE':
        resources = network['nodes']
    elif resource_type == 'LINK':
        resources = network['links']
    else:
        resources = [network]

    resource_lookup = {r['name']: r for r in resources}

    # create dictionary to store resource attribute information (units, etc.)
    # NOTE: For larger networks, we may want to use a more efficient way of getting resource attribute information,
    # rather than loading all network information.
    # res_attrs_all = get_res_attrs(network, template)
    # res_attrs = {}
    # for id, r in res_attrs_all.items():
    #     r.update({'id': id})
    #     res_attrs[(r.obj_type, r.res_name, r.attr_name)] = r

    # ===============
    # main processing
    # ===============
    # scenario_ids = {s['name']: s['id'] for s in network.scenarios}

    updated_scenarios = {}
    values = {}

    # organize the data
    if input_method != 'native' or data_type != 'timeseries':
        # process as a function
        # organize the data

        for x in ['Feature', 'Feature type', 'Variable', 'Scenario']:
            if type(df) == pd.DataFrame and x in df.columns.names:
                df = df.stack(x)
        df.replace(to_replace='None', value=np.nan, inplace=True)
        df.dropna(inplace=True)
        # df = df.reset_index()
        if 'Feature type' in df.index.names:
            df.index = df.index.droplevel(['Feature type'])
        df = df.reorder_levels(order=['Scenario', 'Feature', 'Variable'])
        values = df

    elif data_type == 'timeseries':

        if 'Date' in pfilters:
            # process as a time series

            # reshape dataframe
            dtvars = ['Date']
            for x in dtvars:
                if x in pivot['cols']:
                    df = df.stack(x)
            for x in pivot['rows']:
                if x not in dtvars:
                    df = df.unstack(x)
            df.dropna(axis=0, inplace=True)
            df.dropna(axis=1, inplace=True)

            dates = pd.DatetimeIndex(df.index.levels[0]).strftime(data_date_format)
            df['date'] = dates
            df.set_index('date', inplace=True)

            cols = df.columns.names
            for col in df:
                scenario = col[cols.index('Scenario')]
                resource_name = col[cols.index('Feature')]
                variable = col[cols.index('Variable')]

                ts = {date: val for date, val in df[col].iteritems()}

                idx = (scenario, resource_name, variable)
                if idx not in values:
                    values[idx] = {}
                if 'Block' in cols:
                    block = col[cols.index('Block')]
                else:
                    block = '0'
                values[idx][block] = ts
            for idx, value in values.items():
                values[idx] = json.dumps(value)

    for idx, value in values.items():
        (scenario_name, resource_name, variable) = idx

        scenario = scenario_lookup[scenario_name]
        if scenario['id'] not in updated_scenarios:
            updated_scenarios[scenario['id']] = scenario
            updated_scenarios[scenario['id']]['resourcescenarios'] = []  # only send back added data

        # dataset name
        name = '{n} - {r} - {v} ({s})'.format(n=network['name'], r=resource_name, v=variable, s=scenario_name)

        # get res_attr info (units and dimension)
        # res_attr = res_attrs[(filters.type, resource_name, variable)]
        # js equivalent: rattr = network[filters.type.lower + 's'].find(r => r['name']===resource_name).attributes.find(ra => ra.attr_name===variable)
        # TODO: revert back to lookup? It's unclear which is more efficient: an initial lookup, or on-the-fly-filtering
        resource = resource_lookup[resource_name]
        rtype = list(filter(lambda rt: rt['template_id']== template['id'], resource.types))[0]
        ttype = list(filter(lambda tt: tt['id'] == rtype['id'], template.templatetypes))[0]
        tattr = list(filter(lambda ta: ta['attr']['name'] == variable, ttype['typeattrs']))[0]
        rattr = list(filter(lambda ra: ra['attr_id'] == tattr['attr_id'], resource.attributes))[0]

        metadata = {
            'input_method': input_method,
            'data': value
        }

        rs = {
            'dataset_id': None,
            'resource_attr_id': rattr['id'],
            'attr_id': tattr['attr_id'],
            'value': {
                'type': tattr.data_type,
                'name': name,
                'unit': tattr.unit,
                'dimension': tattr.dimension,
                'value': NULL_VALUES.get(data_type, '') if input_method != 'native' else value,
                'metadata': json.dumps(metadata)
            }
        }

        updated_scenarios[scenario['id']]['resourcescenarios'].append(rs)

    if not error:
        for sid, scenario in updated_scenarios.items():
            g.hydra.call('update_scenario', scenario)

    return error


def hot_to_pd(data, cols, rows, dtype=object):
    '''Convert Hansontable data to a Pandas dataframe'''

    # columns index
    arrays = data[:len(cols), len(rows):]
    tuples = list(zip(*arrays))
    colindex = pd.MultiIndex.from_tuples(tuples, names=cols)

    # rows index
    arrays = data[len(cols):, :len(rows)]
    tuples = [tuple(l) for l in arrays]
    rowindex = pd.MultiIndex.from_tuples(tuples, names=rows)

    values = data[len(cols):, len(rows):]
    df = pd.DataFrame(data=values, index=rowindex, columns=colindex, dtype=dtype)

    return df
