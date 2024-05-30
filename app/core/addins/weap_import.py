import copy
import json
import os
import zipfile
import sys
import tempfile
from datetime import datetime
import pendulum
from ast import literal_eval
import re
import pandas
import shapefile
from pypxlib import Table

from flask import current_app, url_for

from app.core.addins.utils import readfromzip, shapefile2geojson, get_resource_name, coords_to_string

from openagua.security import current_user
from app.core.files import bulk_upload_data
from app.core.templates import get_default_types, prepare_template_for_import
from app.core.networks import add_network, make_node

from app.core.addins.misc import striprtf

from hashlib import md5

WEAP_ARGS_FNS = [],
WEAP_ARGSTRING_FNS = ['F040', 'F048', 'F087']

SIMPLE_EXPRESSIONS = {
    'F026': 'min',
    'F056': 'log'
}


def import_from_weap(hydra, file, project_id, network_name=None, template_name=None):
    # template_id = get_templates(g.hydra, current_app.config['ENABLED_TEMPLATES'])[0].id

    storage_location = current_app.config.get('NETWORK_FILES_STORAGE_LOCATION')
    bucket_name = current_app.config.get('AWS_S3_BUCKET')

    # prepare template
    template = hydra.call('get_template_by_name', template_name)
    new_template = prepare_template_for_import(template, internal=True)
    new_template['layout']['project_id'] = project_id

    base_name = '{} for {}'.format(template_name, network_name)
    new_name = base_name
    i = 1
    while True:
        new_template['name'] = new_name
        template = hydra.call('add_template', new_template)
        if template.get('id'):
            break
        elif i <= 50:
            new_name = '{} ({})'.format(base_name, i)
            i += 1
        else:
            return None, None

    type_lookup_path = './openagua' + url_for('static', filename='other/WEAP_OpenAgua_types.csv')
    attr_lookup_path = './openagua' + url_for('static', filename='other/WEAP_OpenAgua_attributes.csv')
    filename = file.filename

    with tempfile.TemporaryDirectory() as tmpdir:

        zipfilepath = '{}/{}'.format(tmpdir, filename)
        file.save(zipfilepath)

        dir = os.path.splitext(zipfilepath)[0]
        with zipfile.ZipFile(zipfilepath, 'r') as weapzipfile:
            weapzipfile.extractall(dir)

        network = weap_to_hydra(hydra, dir, filename, project_id, network_name, template, type_lookup_path,
                                attr_lookup_path, storage_location=storage_location, bucket_name=bucket_name)

        if network:
            network['links'] = []
            network['nodes'] = []
            network['scenarios'] = []

        return network, template


def weap_to_hydra(hydra, dir, filename, project_id, network_name, template, type_lookup_path,
                  attr_lookup_path, storage_location=None, bucket_name=None):
    # TODO: move to a proper plugin

    # get the weap data
    area = WEAPArea(filename, dir)

    weapzipfile = zipfile.ZipFile(dir + '.zip', 'r')

    # load the template
    default_types = get_default_types(template)

    template_id = template['id']

    # get information about the template
    ttype_lookup = {}
    tattr_lookup = {}
    for tt in template['types']:
        tt['tattr_lookup'] = {ta['attr']['name']: ta for ta in tt['typeattrs']}
        tt['tattr_lookup_by_id'] = {ta['attr_id']: ta for ta in tt['typeattrs']}
        tattr_lookup.update(tt['tattr_lookup'])
        ttype_lookup[tt['name']] = tt
    type_lookup = weap2openagua_lookup(ttype_lookup, type_lookup_path, 'TypeID')
    attr_lookup = weap2openagua_lookup(tattr_lookup, attr_lookup_path, 'VariableID')

    # 1. create network

    # prepare network attributes, including adding key assumptions as network attributes
    network_name = network_name or os.path.splitext(filename)[0]
    network_types = [tt for tt in template.templatetypes if tt.resource_type == 'NETWORK']
    if not network_types:
        network_type = {'id': None, 'template_id': template_id, 'typeattrs': [], 'resource_type': 'NETWORK'}
    else:
        network_type = network_types[0]

    network_attributes = []
    tattrs = {ta['attr']['name']: ta for ta in network_type.get('typeattrs', [])}
    new_tattrs = []
    for ka in area.key_assumptions:
        ta = tattrs.get(ka['Name'])
        if ta:
            rattr = dict(
                attr_id=ta['attr_id'],
                attr_is_var=ta['is_var']
            )
        else:
            attr = hydra.call('add_attribute', ka['Name'], 'dimensionless')
            if attr['id'] in [ra['attr_id'] for ra in network_attributes]:
                continue
            rattr = dict(
                attr_id=attr['id'],
                is_var='N'
            )
            key_expression = area.key_data.get(ka['BranchID'], '')
            new_tattrs.append({
                'attr_id': attr['id'],
                'unit': '-',
                'data_type': 'descriptor' if key_expression and len(key_expression) and key_expression[
                    0] == ';' else 'scalar',
                'properties': {'has_blocks': False, 'BranchID': ka['BranchID']},
            })
        network_attributes.append(rattr)

    # update template
    if not network_type['id']:
        network_type['typeattrs'] = new_tattrs
        network_type = hydra.call('add_templatetype', network_type)
        template['types'] = [network_type]
    else:
        if new_tattrs:
            network_type['typeattrs'].extend(new_tattrs)
            network_type = hydra.call('update_templatetype', network_type)

    template['types'] = [network_type if tt['id'] == network_type['id'] else tt for tt in template.templatetypes]
    network_typeattr_lookup = {ta.get('properties', {}).get('BranchID'): ta for ta in network_type['typeattrs']}

    # update other template types
    current_types = {tt['name']: tt for tt in template['types']}
    updated_types = {}
    for typeid, user_variables in area.UserVariables.items():
        ttype = type_lookup.get(typeid)
        if not ttype:
            continue
        typeattrs = ttype['tattr_lookup_by_id']
        new_tattrs = []
        for v in user_variables:
            attr = hydra.call('add_attribute', v['DisplayLabel'], 'dimensionless')
            if attr['id'] in typeattrs:
                continue
            new_tattrs.append({
                'attr_id': attr['id'],
                'unit': '-',
                'data_type': 'timeseries',
                'properties': {'has_blocks': False, 'VariableID': v['FieldID'], 'category': 'Custom'},
            })
        if new_tattrs:
            ttype['typeattrs'].extend(new_tattrs)
            updated_type = hydra.call('update_templatetype', ttype)
            updated_types[updated_type['id']] = updated_type
            type_lookup[typeid] = updated_type
            for ta in [ta for ta in updated_type['typeattrs'] if ta['properties'].get('VariableID')]:
                attr_lookup[ta['properties']['VariableID']] = ta

    if updated_types:
        template['types'] = [updated_types.get(tt['id'], tt) for tt in template['types']]

    # create the network
    net = {
        'name': network_name,
        'description': 'Imported from WEAP Area',
        'project_id': project_id,
        'layout': {
            'active_template_id': template['id'],
        },
        'types': [dict(id=network_type['id'], template_id=template_id)],
        'attributes': network_attributes
    }

    # add scenarios (needed first to add data to objects)
    # Get time step info

    start_year = area.Areadata['BaseYear']
    end_year = area.Areadata['EndYear']
    time_step = area.Areadata['TSTitle']

    start_ts = area.Areadata['FirstTS']
    if time_step == 'day':
        start = pendulum.parse('{}-{:3}'.format(start_year, start_ts))
    elif time_step == 'week':
        start = pendulum.parse('{}-W{}'.format(start_year, start_ts))
    else:
        start = pendulum.datetime(start_year, start_ts, 1)

    start_time = start.to_datetime_string()
    years = end_year - start_year + (1 if start.month == 1 else 0)
    end = start.add(years=years).subtract(seconds=1)
    end_time = end.to_datetime_string()

    # create scenarios
    scenarios = []

    for Scenario in area.Scenarios:
        sid = Scenario['ScenarioID']
        scenario = dict(
            id=-sid,
            name=Scenario['Name'],
            description=Scenario['Description'],
            layout={'ScenarioID': sid, 'MainParentScenarioID': Scenario['MainParentScenarioID']},
            resourcescenarios=[]  # data is added below
        )
        if Scenario['Name'] == 'Current Accounts':
            scenario.update(
                start_time=start_time,
                end_time=end_time,
                time_step=time_step,
            )
            scenario['layout'].update({'class': 'baseline'})
        else:
            scenario['layout'].update({'class': 'option'})
        scenarios.append(scenario)

    nodes = []
    links = []

    weap_template_types = {}

    # 2. add nodes
    node_names = []
    shp = readfromzip(weapzipfile, network_name, 'WEAPNode.shp')
    dbf = readfromzip(weapzipfile, network_name, 'WEAPNode.dbf')
    if shp and dbf:
        sfreader = shapefile.Reader(shp=shp, dbf=dbf)
        features = shapefile2geojson(sfreader)
        for feature in features:
            objid = feature['properties']['ObjID']
            typeid = feature['properties']['TypeID']
            ttype = type_lookup.get(typeid)
            weap_template_types[objid] = ttype
            base_name = area.Objects[objid].Name
            name = get_resource_name(base_name, node_names)
            node_names.append(name)
            coords = coords_to_string(feature['geometry']['coordinates'])

            node = dict(
                id=-feature['properties']['ObjID'],
                name=name,
                description=name,
                x=coords[0],
                y=coords[1],
                types=[dict(id=ttype['id'], template_id=template_id)],
                attributes=[{
                    'attr_id': ta['attr_id'],
                    'attr_is_var': ta['attr_is_var']
                } for ta in ttype['typeattrs']]
            )

            feature['properties'] = {}
            node.update(layout={
                'geojson': copy.deepcopy(feature),
                'ObjID': objid,
                'BranchID': area.BranchLookup.get(objid, {}).get('BranchID'),
                'TypeID': typeid
            })

            nodes.append(node)

    # 3. add links
    shp = readfromzip(weapzipfile, network_name, 'WEAPArc.shp')
    dbf = readfromzip(weapzipfile, network_name, 'WEAPArc.dbf')
    if shp and dbf:
        sfreader = shapefile.Reader(shp=shp, dbf=dbf)

        features = shapefile2geojson(sfreader)
        links = []
        link_names = []
        node_lookup_by_coords = {(node['x'], node['y']): node for node in nodes}
        max_node_id = max([abs(node['id']) for node in nodes])

        link_id = -1
        for feature in features:
            objid = feature['properties']['ObjID']
            typeid = feature['properties']['TypeID']
            base_name = area.Objects[objid].Name
            ttype = type_lookup.get(typeid)

            weap_template_types[objid] = ttype

            parent_link_id = link_id

            coordinates = feature['geometry']['coordinates']

            # get first node
            node_1_idx = 0
            scoords = coords_to_string(coordinates[0])
            node_1 = node_lookup_by_coords.get(scoords)
            if node_1:
                node_1_id = node_1['id']
            else:
                # there is no WEAP node here, just the beginning of a link (diversion or river)
                # we need to create a new node here

                max_node_id += 1
                node_1_id = -max_node_id
                node_1_name = get_resource_name(base_name + " Inflow", node_names)
                node_names.append(node_1_name)

                node_1 = make_node(
                    template_id=template['id'],
                    ttype=default_types.inflow,
                    node_name=node_1_name,
                    node_description='Inflow node for {}'.format(node_1_name),
                    x=scoords[0],
                    y=scoords[1],
                    id=node_1_id
                )
                nodes.append(node_1)
                node_lookup_by_coords[scoords] = node_1

            L = len(coordinates)
            river_order = 0
            for i, coords in enumerate(coordinates):

                scoords = coords_to_string(coords)

                node_2_id = node_lookup_by_coords.get(scoords, {}).get('id')
                if node_2_id and node_2_id != node_1_id or i == L - 1:

                    if not node_2_id:
                        # this is an outflow node
                        max_node_id += 1
                        node_2_id = -max_node_id
                        node_2_name = get_resource_name(base_name + " Outflow", node_names)
                        node_names.append(node_2_name)
                        node_2 = make_node(
                            template_id=template['id'],
                            ttype=default_types.outflow,
                            node_name=node_2_name,
                            node_description='Outflow node for {}'.format(node_2_name),
                            x=scoords[0],
                            y=scoords[1],
                            id=node_2_id
                        )
                        nodes.append(node_2)

                    reachid = area.LinkChildren.get((objid, river_order))
                    if reachid:
                        reach = area.Objects[reachid]
                        reach_name = reach.Name
                    else:
                        reach_name = base_name

                    river_id = area.Objects.get(objid, {}).get('RiverID')
                    branch_id = area.BranchLookup.get(river_id, {}).get('BranchID')

                    name = get_resource_name(reach_name, link_names)
                    link_names.append(name)

                    geojson = copy.deepcopy(feature)
                    geojson['properties'] = {}
                    geojson['geometry']['coordinates'] = coordinates[node_1_idx:i + 1]

                    link = {
                        'id': link_id,
                        'name': name,
                        'description': reach_name,
                        'node_1_id': node_1_id,
                        'node_2_id': node_2_id,
                        'types': [{'id': ttype['id'], 'template_id': template_id}],
                        'attributes': [{
                            'attr_id': ta['attr_id'],
                            'attr_is_var': ta['attr_is_var']
                        } for ta in ttype['typeattrs']]
                    }

                    link['layout'] = {
                        'display_name': base_name,
                        'geojson': geojson,
                        'parent': parent_link_id,
                        'is_parent': river_order == 0,
                        'ObjID': objid,
                        'BranchID': branch_id,
                        'TypeID': typeid
                    }

                    links.append(link)

                    link_id -= 1
                    river_order += 2
                    node_1_id = node_2_id
                    node_1_idx = i

    net['nodes'] = nodes
    net['links'] = links
    net['scenarios'] = scenarios

    network = add_network(hydra, net, location=storage_location, template_id=template['id'], add_baseline=False,
                          return_summary=False)

    # get data

    # to lookup resource codes
    resource_code_lookup = {}
    network_attr_lookup = {ra['attr_id']: ra['id'] for ra in network.attributes}

    # to lookup resources by BranchID
    resource_lookup = {}

    def update_resource_lookups(resource, resource_class):
        branch_id = resource.get('layout', {}).get('BranchID')
        if branch_id:
            resource_code_lookup[branch_id] = '{}/{}'.format(resource_class, resource.id)
            resource_lookup[branch_id] = resource

    update_resource_lookups(network, 'network')
    for node in network['nodes']:
        update_resource_lookups(node, 'node')
    for link in network['links']:
        update_resource_lookups(link, 'link')

    converter = WEAPConversion(
        network_id=network['id'],
        resource_code_lookup=resource_code_lookup,
        attr_lookup=attr_lookup,
        network_typeattr_lookup=network_typeattr_lookup
    )

    weap_scenarios = [s['ScenarioID'] for s in area.Scenarios]

    errors = [['Resource', 'Attribute', 'Error']]
    for (weap_variable_id, weap_branch_id, weap_scenario_id), data in area.Data.items():

        if data is None:
            continue

        if weap_branch_id <= 50:
            continue  # these don't get imported yet

        tattr = None
        res_attr_id = None
        resource = None
        error = None

        # network variables
        if weap_variable_id == 27:  # key assumption
            tattr = network_typeattr_lookup.get(weap_branch_id)
            res_attr_id = network_attr_lookup.get(tattr['attr_id']) if tattr else None

            if not tattr:
                error = 'No Key Assumption template attribute found.'
            elif not res_attr_id:
                error = 'No Key Assumption network attribute found.'


        else:

            resource = resource_lookup.get(weap_branch_id)

            # normal resources
            if resource and resource.get('attributes') is not None:
                tattr = attr_lookup.get(weap_variable_id)
                if not tattr:
                    continue
                rattrs = [ra for ra in resource['attributes'] if ra['attr_id'] == tattr['attr_id']]
                res_attr_id = rattrs[0]['id'] if rattrs else None

            if not tattr:
                error = 'No template attribute found. Check template and WEAP to OpenAgua conversion table.'
            elif not res_attr_id:
                error = 'No resource attribute found. Check resource attributes.'

        if error:
            errors.append([
                'WEAP BranchID {}'.format(weap_branch_id),
                'WEAP VariableID {}'.format(weap_variable_id), error
            ])
            continue

        rs, error = weap_to_resourcescenario(tattr, res_attr_id, data, converter)
        if rs:
            network['scenarios'][weap_scenarios.index(weap_scenario_id)]['resourcescenarios'].append(rs)

        if error:
            error = '\"{}\"'.format(error)
            if resource:  # node or link
                error = [resource['name'], tattr['attr_name'], error]
            else:  # key assumption
                error = ['General attributes', tattr['attr_name'], error]
            errors.append(error)

    # update scenarios relational structure
    for scenario in network['scenarios']:
        parent = [s for s in network['scenarios'] if
                  s['layout']['ScenarioID'] == scenario['layout']['MainParentScenarioID']]
        if parent:
            parent = parent[0]
            scenario['layout']['parent'] = parent['id']
        del scenario['layout']['MainParentScenarioID']

    # update link parent information
    parent_lookup = {link['layout']['parent']: link['id'] for link in network['links'] if link['layout']['is_parent']}
    for link in network['links']:
        link['layout']['parent'] = parent_lookup[link['layout']['parent']]

    network['nodes'] = []  # we aren't deleting nodes, just not sending any to the server for update

    network = hydra.call('update_network', network)

    # 4. Save files in WEAP folder
    # NB: just send back signed post keys...

    data = {}
    for f in weapzipfile.filelist:
        filename = os.path.basename(f.filename)
        dir = os.path.dirname(f.filename)
        base, ext = os.path.splitext(filename)
        if f.filename[-1] == '/':
            continue
        elif not dir and (
                ext.lower() in ['.db', '.px', '.mb', '.ini', '.bin', '.jgw', '.yes'] or
                ext[-4:-2] in ['.X', '.Y'] and ext[-2:-1].isdigit() or
                ext[-4:-1] in ['.XG', '.YG'] or
                base in ['WEAPArc', 'WEAPNode'] or
                f.filename in ['DownstreamNodes.csv', 'UpstreamPoints.csv', 'Schematic.jpg',
                               'WEAPAutomationErrors.txt', 'infeasible.lp'] or
                base[:7] == 'Result_'
        ):
            continue
        else:
            data[f.filename] = readfromzip(weapzipfile, network_name, f.filename)

    if len(errors) > 1:
        data['WEAP_import_errors.csv'] = '\n'.join([','.join(row) for row in errors]).encode()

    bulk_upload_data(network, bucket_name, data)

    return network


def weap2openagua_lookup(lookup_table, lookup_path, id_field):
    df = pandas.read_csv(lookup_path, index_col=False)

    lookup = {}
    for row in df.iterrows():
        weap_id = row[1][id_field]
        openagua_name = row[1]['OpenAgua']
        if lookup_table.get(openagua_name):
            lookup[weap_id] = lookup_table[openagua_name]

    return lookup


def weap_to_resourcescenario(tattr, res_attr_id, data, converter):
    source = 'WEAP/%s' % current_user.email

    expression = data
    function, error = converter.convert(expression)

    metadata = {
        'source': source,
        'function': function,
        'note': '',
        'use_function': 'Y',
        'cr_date': '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now())
    }

    emptyvalue = {
        'timeseries': '{}',
        'array': '[[],[]]',
        'scalar': '0',
        'descriptor': ''
    }

    value = '{}'

    dataset = {
        'id': None,
        'name': tattr['attr']['name'],
        'unit': tattr.unit,
        'dimension': tattr.dimension,
        'type': tattr.data_type,
        'value': value,
        'metadata': json.dumps(metadata, ensure_ascii=True)
    }

    resourcescenario = {
        'resource_attr_id': res_attr_id,
        'value': dataset,
    }

    return resourcescenario, error


class WEAPArea:
    def __init__(self, filename, dir):
        self.Areadata = {}
        self.Objects = {}
        self.Scenarios = []
        self.Data = {}
        self.key_assumptions = []
        self.key_data = {}
        self.dir = dir
        self.load(filename)

    def load(self, filename):

        def row_to_dict(row, keys):
            return {key: row.__getattr__(key) for key in keys if row.__getattr__(key)}

        # Areadata
        with self.db('Areadata') as table:
            row = table[0]
            for key in ['BaseYear', 'EndYear', 'NumTS', 'FirstTS', 'TSTitle', 'NumDaysInYear',
                        'TimeStepsPerYear']:
                self.Areadata[key] = row[key]

            self.Areadata['Notes'] = striprtf(row.Notes) if row.Notes else ''

        # Master structure
        self.BranchLookup = {}
        with self.db('MasterStructure') as table:
            for row in table:
                if hasattr(row, 'ObjID'):
                    self.BranchLookup[row.ObjID] = row_to_dict(row, ['BranchID', 'ParentBranchID'])

            self.key_assumptions = [row_to_dict(row, ['BranchID', 'ParentBranchID', 'Name']) for row in table if
                                    row.BranchType == 2]

        with self.db('UserVariables') as table:
            fields = list(table.fields)
            self.UserVariables = {}
            for row in table:
                if row.TypeID not in self.UserVariables:
                    self.UserVariables[row.TypeID] = []
                self.UserVariables[row.TypeID].append(row_to_dict(row, fields))

        with self.db('UserResultVariables') as table:
            fields = list(table.fields)
            self.UserResultVariables = [row_to_dict(row, fields) for row in table]

        with self.db('Units') as table:
            fields = list(table.fields)
            self.Units = [row_to_dict(row, fields) for row in table]

        # Objects
        objkeys = ['Name', 'RiverID', 'RiverOrder']
        self.LinkChildren = {}
        with self.db('Objects') as table:
            for row in table:
                row_dict = row_to_dict(row, objkeys)
                self.Objects[row.ObjID] = row_dict

                # add a lookup dictionary for reach-like links
                if row.__getattr__('RiverID') and row.__getattr__('RiverOrder') is not None:
                    self.LinkChildren[(row.RiverID, row.RiverOrder)] = row.ObjID

        # Scenarios
        with self.db('Scenarios') as scenarios:
            for row in scenarios:
                scenario = {key: row[key] for key in ['ScenarioID', 'MainParentScenarioID', 'Name']}
                scenario['Description'] = striprtf(row.Description) if row.Description else ''
                self.Scenarios.append(scenario)

        # Data
        with self.db('Data') as datarows:
            self.key_data = {}
            for row in datarows:
                expression = row.Expression
                if type(expression) == bytes:
                    expression = expression.decode()
                self.Data[(row.VariableID, row.BranchID, row.ScenarioID)] = expression

                # this will be for guessing the key assumption data type (there's no "auto" data type, though "descriptor" could be a catchall)
                if row.VariableID == 27 and row.BranchID not in self.key_data:
                    self.key_data[row.BranchID] = expression

    def db(self, name):
        filelist = os.listdir(self.dir)
        dbname = '{}.db'.format(name)
        dbname = dbname if dbname in filelist else dbname.replace('.db', '.DB')
        if dbname not in filelist:
            return
        mbname = dbname.replace('.db', '.MB').replace('.DB', '.MB')
        if mbname not in filelist:
            mbname = None
        file_path = os.path.join(self.dir, dbname)
        blob_file_path = os.path.join(self.dir, mbname) if mbname else None
        return Table(file_path=file_path, blob_file_path=blob_file_path)


def hash(string, quote=False):
    if quote:
        return '"{}"'.format(md5(string.encode()).hexdigest())
    else:
        return md5(string.encode()).hexdigest()


class WEAPConversion:

    def __init__(self, network_id=None, resource_code_lookup=None, attr_lookup=None, network_typeattr_lookup=None):

        self.lookup = {}
        self.hashes = {}
        self.errors = {}
        self.network_id = network_id
        self.resource_code_lookup = resource_code_lookup
        self.attr_lookup = attr_lookup
        self.network_typeattr_lookup = network_typeattr_lookup
        self.key_attrs = {ta['attr']['name']: ta for ta in network_typeattr_lookup.values()}

    def convert(self, expression):

        # lookup expression and don't continue if it has already been converted
        if hasattr(self.lookup, expression):
            return self.lookup[expression], self.errors[expression]

        try:
            # can the expression be evaluated as is?
            # note: we only want to do this on main entry, not sub-conversions
            # otherwise, we'd need to do this too many times
            literal_eval(expression)
            self.lookup[expression] = expression
            self.errors[expression] = None
            return expression, None

        except:
            self.locals = {}
            self.hashes = {}
            return self._convert(expression)

    def _convert(self, expr, uncomment=False, main=True):

        if hasattr(self.lookup, expr):
            return self.lookup[expr], self.errors[expr]

        comment = False
        is_simple = False
        converted = expr
        error = None
        full_functions = self.get_functions(expr)
        for i, (fname, originalfn) in enumerate(full_functions):
            fn = originalfn
            if '@' in fn[1:]:
                sub_functions = self.get_functions(fn[1:])
                for (sub_fname, sub_fn) in sub_functions:
                    replacement = self.locals.get(sub_fn)
                    if not replacement:
                        replacement, error = self._convert(sub_fn, uncomment=True, main=False)
                    self.locals[sub_fn] = replacement
                    hashed = hash(replacement, quote=True)
                    self.hashes[hashed] = replacement
                    fn = fn.replace(sub_fn, hashed)
            if hasattr(self, fname):
                try:
                    if hasattr(self.lookup, originalfn):
                        replacement = self.lookup[originalfn]
                        error = self.errors[originalfn]
                    else:
                        self.main = main
                        call = 'self.{}'.format(fn.replace('@', '')) \
                            .replace('\\', '/') \
                            .replace('=', ' == ') \
                            .replace('> ==', ' >=') \
                            .replace('< ==', ' <=') \
                            .replace(';', '#')
                        replacement, error = eval(call)
                        self.lookup[originalfn] = replacement
                        self.locals[originalfn] = replacement
                        # hashed = hash(replacement, quote=True)
                        # self.hashes[hashed] = replacement
                        self.errors[originalfn] = error
                    converted = converted.replace(originalfn, replacement)

                except:
                    comment = True
                    if fname in SIMPLE_EXPRESSIONS:
                        is_simple = True
                        converted = fn.replace('@{}'.format(fname), SIMPLE_EXPRESSIONS[fname])
                    else:
                        error = 'Failed to convert expression: {}'.format(originalfn)


            else:
                comment = True
                error = 'No converter for WEAP expression: @{}'.format(fname)

            if comment and i == 0 and not uncomment and not is_simple:
                converted = '# {}'.format(converted)

        if not full_functions:
            converted = expr.replace('\\', '/')
            if len(converted) and converted[0] == ';':
                converted = '\"{}\"'.format(converted.replace(';', ''))  # assume this is a descriptor
        elif main:
            # converted = expr
            # for localfn, replacement in self.locals.items():
            finished = False
            while not finished:
                finished = True
                for hashed, replacement in self.hashes.items():
                    hashed_stripped = hashed.replace('"', '')
                    if hashed in converted or hashed_stripped in converted:
                        converted = converted.replace(hashed, replacement)
                        converted = converted.replace(hashed_stripped, replacement)
                        finished = False

        if comment and is_simple and not error:
            error = 'Failed to verify converted expression: {}'.format(converted)

        self.lookup[expr] = converted
        self.errors[expr] = error

        return converted, error

    def get_functions(self, expr):
        fnames = re.findall(r"(?<=@)\w+", expr)
        full_functions = []
        x = 0
        for fn in fnames:
            for i, ch1 in enumerate(expr[x:]):
                if ch1 == '@':
                    rest = expr[x + i + len(fn) + 2:]
                    balance = 1
                    for j, ch2 in enumerate(rest):
                        balance += 1 if ch2 == '(' else -1 if ch2 == ')' else 0
                        if balance == 0:
                            y = x + i + len(fn) + 2 + j
                            full_functions.append((fn, expr[x + i:y + 1]))
                            x = y
                            break
                    break
        return full_functions

    def V(self, *args, **kwargs):
        error = ''
        # another variable
        offset = kwargs.get('offset')
        branch_id, variable_id, a, b = args
        if variable_id == 27:
            resource_code = 'network/{}'.format(self.network_id)
            attr_id = self.network_typeattr_lookup.get(branch_id, {}).get('attr_id')
        else:
            resource_code = self.resource_code_lookup.get(branch_id)
            attr_id = self.attr_lookup.get(variable_id, {}).get('attr_id')
        if not resource_code:
            error += 'Could not find resource for WEAP BranchID {} '.format(branch_id)
        if not variable_id:
            error += 'Could not find attribute for WEAP VariableID {} '.format(variable_id)

        if offset:
            result = 'self.GET("{}/{}", offset={}, **kwargs)'.format(resource_code, attr_id, offset)
        else:
            result = 'self.GET("{}/{}", **kwargs)'.format(resource_code, attr_id)
        return result, error

    def F026(self, *args):
        error = None
        result = 'max({})'.format(', '.join([str(a) for a in args]))
        return result, error

    def F040(self, argstring):
        error = None
        # WeeklyValues
        vals = argstring.strip().split(',')
        lookup = {}
        for i in range(0, len(vals), 2):
            val = float(vals[i + 1])
            lookup[int(vals[i])] = val
        result = "{}.get(period, 0)".format(lookup)
        return result, error

    def F048(self, argstring):
        error = None
        # Lookup
        vals = argstring.strip().split(',')
        result = str([(float(vals[i]), float(vals[i + 1])) for i in range(0, len(vals), 2)]).replace('), (', '),\n(')
        return result, error

    def F050(self, string):
        # ReadFromFile
        argnames = ['path', 'col']
        args = {argnames[i]: arg for i, arg in enumerate(string.split(',')[:len(argnames)])}
        path = args['path']  # required
        col = args.get('col', 1)
        parts = path.split('?')
        error = ''
        if len(parts):
            path = '\"{}\".format('.format(path)
            for i, part in enumerate(parts):
                if i % 2 == 1:
                    path = path.replace('?{}?'.format(part), '{{{}}}'.format(part))
                    res_attr_id = self.key_attrs.get(part, {}).get('attr_id')
                    if not res_attr_id:
                        error += 'Could not find resource for path part {}. '.format(path)
                    path += "{part}=self.GET(\"{path}\", **kwargs), " \
                        .format(part=part, path='network/{}/{}'.format(self.network_id, res_attr_id))
            path = path[:-2] + ')'

        result = "path={}".format(path) + \
                 "\ndata = self.read_csv(path, usecols=[0,1,{col}], comment=';', header=None, **kwargs)" \
                     .format(col=str(int(col) + 1)) + \
                 "\nreturn data.iloc[timestep][{col}]".format(col=int(col) + 1)

        return result, error

    def F056(self, *args):
        error = None
        result = 'log({})'.format(args[0])
        return result, error

    def F060(self, *args):
        error = None
        result = 'int({})'.format(args[0])
        return result, error

    def F072(self, *args):
        error = None
        result, error = self.V(*args, offset=-1)
        return result, error

    def F079(self, *args):
        error = None
        script, fn = args[0].split('!')
        callargs = '\"{}\", \"{}\"'.format(script.strip(), fn.strip())
        callargs += (', ' + ', '.join(x.strip() for x in args[1:])) if args[1:] else ''

        result = 'self.call({}, **kwargs)'.format(callargs)
        return result, error

    def F087(self, argstring):
        error = None
        splits = [s.strip() for s in argstring.split(',')]
        c = 0
        x = 0
        parts = []
        for i, split in enumerate(splits):
            expr = ', '.join(splits[x:i + 1])
            try:
                literal_eval(expr)
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                if exc_type != SyntaxError:
                    parts.append(expr)
                    x = i + 1

        result = 'x = 0'
        if self.main:
            for i, expr in enumerate(parts):
                if i == 0:
                    result += '\nif {}:'.format(expr)
                elif i % 2 == 0:
                    result += '\nelif {}:'.format(expr)
                else:
                    result += '\n    x = {}'.format(expr)
            result += '\nreturn x'

        # else:
        #     for i in range(0, len(parts), 3):
        #         if i == 0:
        #             a, b, c = parts[i:i+3]
        #             result = '{} if {} else {}'.format(a, b, c)

        return result, error
