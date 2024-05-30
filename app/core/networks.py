from os import getenv
import copy
import json
import requests
from datetime import datetime
import svgwrite
import numpy
from boltons.iterutils import remap
import io
import xlsxwriter
from itertools import chain
from xml.dom import minidom

import pendulum

from app.core.network_editor import repair_network_references, update_links2, make_feature_collection
from app.core.templates import clean_template, clean_template2, add_template
from app.core.files import add_storage, upload_network_data, duplicate_folder
from app.core.templates import change_active_template

from app.models import UserNetworkSettings

INVALID_CLASS_CHARACTERS = ['~', '!', '@', '$', '%', '^', '&', '*', '(', ')', '+', '=', ',', '.', '/', '\'', ';', ':',
                            '"', '?', '>', '<', '[', ']', '\\', '{', '}', '|', '`', '#', ' ']


def check_url(url):
    try:
        requests.head(url, timeout=100)
        return True
    except:
        return False


def clean_network(net, old_template=None, new_template=None, purpose='download'):
    if old_template and new_template:
        old_ttypes = {tt['id']: tt['name'] for tt in old_template.templatetypes}
        new_ttypes = {tt['name']: tt['id'] for tt in new_template.templatetypes}

    # functions

    # general
    def visit(path, key, value):
        if key in {'cr_date', 'created_by', 'owners'}:
            return False
        else:
            return key, value

    # types
    def clean_types(types):
        if old_template and new_template:
            new_types = []
            for rt in types:
                if abs(rt['template_id']) == old_template['id']:
                    new_types.append({'template_id': new_template['id'], 'id': new_ttypes[old_ttypes[abs(rt['id'])]]})
            return new_types
        else:
            return types

    # attributes
    def clean_attrs(resource_attributes):
        new_res_attrs = []
        for ra in resource_attributes:
            new_res_attrs.append({
                'id': -ra['id'],
                'attr_id': ra['attr_id'] if purpose == 'clone' else -ra['attr_id'],
            })
        return new_res_attrs

    # network

    cleaned = remap(dict(net), visit=visit)
    cleaned['layout']['old_id'] = cleaned.pop('id')
    cleaned['types'] = clean_types(cleaned['types'])
    cleaned['attributes'] = clean_attrs(cleaned['attributes'])

    # all resources

    names = []
    for rt in ['nodes', 'links']:
        for i, resource in enumerate(cleaned[rt]):
            resource['layout']['old_id'] = resource['id']
            resource['id'] = -resource['id']
            resource['types'] = clean_types(resource['types'])
            resource['attributes'] = clean_attrs(resource['attributes'])
            if len(resource['name']) > 100 or resource['name'] in names:
                resource['name'] = '{}_{}{:05}'.format(resource['name'][:53], rt[0], i + 1)
            names.append(resource['name'])

            if rt == 'links':
                resource['node_1_id'] = -resource['node_1_id']
                resource['node_2_id'] = -resource['node_2_id']

    # groups

    for group in cleaned['resourcegroups']:
        group['id'] = -group['id']
        del group['network_id']
        group['types'] = clean_types(group['types'])
        group['attributes'] = clean_attrs(group['attributes'])

    new_scenarios = []
    for scen in cleaned['scenarios']:
        del scen['network_id']
        del scen['id']
        parent = scen['layout'].get('parent')
        if parent:
            scen['layout']['parent'] = -scen['layout']['parent']
        children = scen['layout'].get('children')
        if children:
            scen['layout']['children'] = [-c for c in set(scen['layout']['children'])]

        for rs in scen['resourcescenarios']:
            del rs['dataset_id']
            del rs['value']['id']
            del rs['attr_id']
            rs['resource_attr_id'] = -rs['resource_attr_id']

        for item in scen['resourcegroupitems']:
            for key in ['id', 'group_id', 'ref_id']:
                item[key] = -item[key]

        # new_scenarios.append(scen)

    # cleaned['scenarios'] = new_scenarios

    return cleaned


def add_network(hydra, net, location=None, template_id=None, add_baseline=True, start_time=None, end_time=None,
                time_step=None, return_summary=True):
    # net = clean_network(net)
    if 'layout' not in net:
        net['layout'] = {}
    if template_id:
        net['layout']['active_template_id'] = template_id

    if location:
        net = add_storage(net, location)

    network = None
    network_name = net['name']
    i = 0
    while True:
        result = hydra.call('add_network', net, return_summary=return_summary, timeout=10)
        if result and result.get('name'):
            network = result
            break
        elif result is None:
            break
        elif type(result.get('faultstring')) == str and 'already in' in result['faultstring'] and i < 10:
            i += 1
            net['name'] = '{} ({})'.format(network_name, i)
        else:
            network = None
            break

    # add a default scenario (similar to Hydra Modeller)
    if network and add_baseline:
        baseline = [s for s in network.get('scenarios', []) if s['layout'].get('class') == 'baseline']
        if not baseline:
            baseline = add_default_scenario(hydra, network['id'], start_time=start_time, end_time=end_time,
                                            time_step=time_step)
            network['scenarios'].append(baseline)

    return network


def get_network(hydra, source_id, network_id, simple=False, summary=True, include_resources=True, repair=False,
                repair_options=None):
    if simple:
        network = hydra.call('get_network', network_id, include_resources=include_resources, summary=summary,
                             include_data=False)
        return network

    network = hydra.call('get_network', network_id, include_resources=True, include_data=False)

    if network is None or 'error' in network:
        return network

    # add baseline scenario if it's missing
    baseline = [s for s in network['scenarios'] if
                'layout' in s and s['layout'] and s['layout'].get('class') == 'baseline']
    update_baseline = False
    if not baseline:
        update_baseline = True
        if network['scenarios']:
            baseline = network['scenarios'][0]
            baseline['layout'] = baseline.get('layout', {})
            baseline['layout']['class'] = 'baseline'
            baseline = hydra.call('update_scenario', baseline)
        else:
            baseline = {
                'name': 'baseline',
                'layout': {'class': 'baseline'},
            }
            baseline = hydra.call('add_scenario', network_id, baseline)
    else:
        baseline = baseline[0]

    # this just fixes some legacy issue and should be removed eventually
    # TODO: remove this at some point?
    settings = network['layout'].get('settings')
    if settings and not baseline.get('start_time') and not g.is_public_user:
        start = settings.get('start')
        end = settings.get('end')
        timestep = settings.get('timestep')
        fmt = '%Y-%m-%d %H:%M:%S'
        baseline.update(
            start_time=pendulum.parse(start).format(fmt) if start else None,
            end_time=pendulum.parse(end).format(fmt) if end else None,
            time_step=timestep
        )
        update_baseline = start and end and timestep
        if start or end:
            if start:
                settings.pop('start')
            if end:
                settings.pop('end')
            network['layout']['settings'] = settings
            nodes = network.pop('nodes')  # remove temporarily to save time/bandwidth
            links = network.pop('links')
            hydra.call('update_network', network)
            network.update(nodes=nodes, links=links)

    if update_baseline:
        baseline = hydra.call('update_scenario', baseline)
        network['scenarios'] = [baseline if baseline['id'] == s['id'] else s for s in network.scenarios]

    owner = [o for o in network.owners if o.user_id == hydra.user_id][0]
    network['editable'] = owner.edit == 'Y'

    # add storage if it's missing
    if not network['layout'].get('storage'):
        location = getenv('NETWORK_FILES_STORAGE_LOCATION')
        network = add_storage(network, location)

    if repair:
        network = repair_network(hydra, source_id, network=network, options=repair_options)

    return network


def autoname(ttype, network):
    base = '{}-{}-{}'.format(network['name'].replace(' ', '')[:5].upper(), ttype['resource_type'][0],
                             ttype['name'].replace(' ', '')[:3].upper())

    existing_names = [r['name'] for r in network[ttype['resource_type'].lower() + 's']]

    i = 1
    while True:
        name = base + str(i)
        if name not in existing_names:
            return name
        i += 1


def make_junction(x, y, junction_type, network, template_id):
    return {
        'x': x,
        'y': y,
        'name': autoname(junction_type, network),
        'description': 'Created automatically by OpenAgua',
        'types': [{'id': junction_type['id'], 'template_id': template_id}],
        'layout': {
            'geojson': {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [float(x), float(y)]
                },
                'properties': {}
            }
        }
    }


def repair_network(hydra, source_id, network_id=None, network=None, options=None):
    network = network or hydra.call('get_network', network_id, include_resources=True, include_data=False)

    if not options:
        return network

    network_id = network_id or network['id']

    update_resources = False

    # repair missing types
    if 'types-attrs' in options:
        network, template = change_active_template(hydra, source_id, network=network)
        template_id = template['id']

        junction_types = [tt for tt in template.templatetypes if 'junction' in tt['name'].lower()]
        junction_type = junction_types[0] if junction_types else None

        # TODO: get default junction from type layout rather than name
        if junction_type:

            update_resources = True

            # repair network topology
            positions = {}
            for node in network['nodes']:
                xy = (node['x'], node['y'])
                positions[xy] = positions.get(xy, []) + [node]
            for (x, y), old_nodes in positions.items():
                if len(old_nodes) > 1:
                    junction = make_junction(x, y, junction_type, network, template_id)
                    new_node = hydra.add_node(network_id, junction)

                    old_node_ids = [n['id'] for n in old_nodes]
                    old_link_ids = [l['id'] for l in network['links'] if
                                    l['node_1_id'] in old_node_ids or l['node_2_id'] in old_node_ids]

                    all_new_links = []
                    for old_node in old_nodes:
                        # update existing links and delete old node
                        new_links = update_links2(hydra, old_node_id=old_node['id'], new_node_id=new_node['id'],
                                                  old_link_ids=old_link_ids)
                        all_new_links.extend(new_links)
                        hydra.call('delete_node', old_node['id'], False)

                    network['nodes'] = [n for n in network['nodes'] if n['id'] not in old_node_ids] + [new_node]
                    network['links'] = [l for l in network['links'] if l['id'] not in old_link_ids] + all_new_links

    else:
        template_id = network['layout'].get('active_template_id')
        template = hydra.call('get_template', template_id)

    # repair missing attributes
    if {'types-attrs', 'topology'} & set(options):

        update_resources = True

        if 'types-attrs' in options:
            ttypes = {tt['id']: tt for tt in template['templatetypes']}

        if 'topology' in options:
            node_coords = {}

        def repair_resource(resource, resource_type):

            if 'types-attrs' in options:
                resource['attributes'] = resource.get('attributes', [])

                # repair resource types - add missing attributes
                updated_resource_types = []
                for rt in resource['types']:
                    if rt['template_id'] != template['id']:
                        continue

                    tt = ttypes[rt['id']]
                    rt['id'] = tt['id']
                    updated_resource_types.append(rt)
                    rattrs = set([ra['attr_id'] for ra in resource['attributes']])
                    tattrs = set([ta['attr_id'] for ta in tt['typeattrs']])
                    missing_attrs = tattrs - rattrs
                    new_attrs = []
                    for ta in tt['typeattrs']:
                        if ta['attr_id'] in missing_attrs:
                            new_attrs.append({
                                'ref_key': resource_type.upper(),
                                'attr_id': ta['attr_id'],
                                'attr_is_var': ta['attr_is_var']
                            })
                    resource['attributes'].extend(new_attrs)

                resource['types'] = updated_resource_types

            # repair geojson
            if 'topology' in options:
                if resource_type == 'node':
                    geojson = resource['layout'].get('geojson')
                    if geojson is None:
                        x, y = float(resource['x']), float(resource['y'])
                        resource['layout']['geojson'] = {
                            'type': 'Feature',
                            'geometry': {
                                'type': 'Point',
                                'coordinates': [x, y],
                            },
                            'properties': {}
                        }
                    else:
                        x, y = geojson['geometry']['coordinates']
                        if type(x) != float:
                            x = float(resource['x'])
                        if type(y) != float:
                            y = float(resource['y'])
                        resource['layout']['geojson']['geometry']['coordinates'] = [x, y]
                        resource['x'] = str(x)
                        resource['y'] = str(y)
                    node_coords[resource['id']] = [x, y]

                elif resource_type == 'link':
                    up_coords = node_coords.get(resource['node_1_id'])
                    down_coords = node_coords.get(resource['node_2_id'])
                    geojson = resource['layout'].get('geojson')
                    if geojson is None:
                        resource['layout']['geojson'] = {
                            'type': 'Feature',
                            'geometry': {
                                'type': 'LineString',
                                'coordinates': [],
                            },
                            'properties': {}
                        }

                    link_coords = resource['layout']['geojson']['geometry']['coordinates']
                    coords_len = len(link_coords)
                    if coords_len == 0:
                        resource['layout']['geojson']['geometry']['coordinates'] = [up_coords, down_coords]
                    else:
                        resource['layout']['geojson']['geometry']['coordinates'][0] = up_coords
                        if coords_len == 1:
                            resource['layout']['geojson']['geometry']['coordinates'] += down_coords
                        else:
                            resource['layout']['geojson']['geometry']['coordinates'][-1] = down_coords
                    resource['layout']['geojson']['geometry']['coordinates'] \
                        = [[round(x * 1e8) / 1e8, round(y * 1e8) / 1e8] for x, y in
                           resource['layout']['geojson']['geometry']['coordinates']]

            return resource

        network = repair_resource(network, 'network')
        network['nodes'] = [repair_resource(n, 'node') for n in network['nodes']]
        network['links'] = [repair_resource(l, 'link') for l in network['links']]

    # repair missing baseline scenario
    repair_scenarios = False
    if 'scenarios' in options:
        repair_scenarios = True
        baseline = [s for s in network.scenarios if s['layout'].get('class') == 'baseline']
        if not baseline:
            if network.scenarios:
                baseline = network['scenarios'][0]
                baseline.update({'class': 'baseline'})
            else:
                baseline = add_default_scenario(hydra, network['id'])
        else:
            baseline = baseline[0]

        # repair parent-less options & scenarios
        scenario_lookup = {s['id']: s for s in network['scenarios']}
        for scenario in network['scenarios']:
            scenario_class = scenario['layout'].get('class')
            parent_id = original_parent_id = scenario['layout'].get('parent')
            if scenario_class in ['option', 'scenario']:
                ancestor_id = None
                if not parent_id:
                    ancestor_id = baseline['id']
                while not ancestor_id:
                    parent = scenario_lookup.get(parent_id)
                    if parent:
                        parent_id = parent['id']
                        if parent_id == baseline['id']:
                            ancestor_id = parent_id
                        elif parent_id != scenario['id']:
                            ancestor_id = parent_id
                        else:
                            parent_id = parent['layout'].get('parent')
                    else:
                        ancestor_id = baseline['id']

                if original_parent_id != ancestor_id:
                    scenario['layout']['parent'] = ancestor_id
                    hydra.call('update_scenario', scen=scenario)

    # remove loop links
    if 'topology' in options:
        for link in network['links']:
            if link['node_1_id'] == link['node_2_id']:
                hydra.call('delete_link', link['id'], False)
        network['links'] = [link for link in network['links'] if link['node_1_id'] != link['node_2_id']]

    # repair reference layers
    if 'layers' in options:
        network = repair_network_references(network)

    if not update_resources:
        network.pop('nodes')
        network.pop('links')
        network.pop('attributes')
    if not repair_scenarios:
        network.pop('scenarios')

    repaired = hydra.call('update_network', network)

    return repaired


def add_default_scenario(hydra, network_id, start_time=None, end_time=None, time_step=None):
    scenario = {
        'name': getenv('DEFAULT_SCENARIO_NAME'),
        'description': getenv('DEFAULT_SCENARIO_DESCRIPTION'),
        'layout': {'class': 'baseline'},
        'start_time': start_time,
        'end_time': end_time,
        'time_step': time_step,
    }
    result = hydra.call('add_scenario', network_id, scenario)
    return result


def has_template(resource, template_id):
    return next((t for t in resource['types'] if t['template_id'] == template_id), None)


def get_network_extents(network, template_id):
    coords = [((float(node['x']), float(node['y']))) for node in network['nodes'] if
              node['layout'].get('exists', True) and has_template(node, template_id)]
    link_coords = []
    coords += list(chain.from_iterable(
        [link['layout']['geojson']['geometry']['coordinates'] for link in network['links'] if
         link['layout'].get('exists', True) and link['layout'].get('geojson') and has_template(link, template_id)]
    ))
    if coords:
        left = min([coord[0] for coord in coords])
        bottom = min([coord[1] for coord in coords])
        right = max([coord[0] for coord in coords])
        top = max([coord[1] for coord in coords])
        extents = (left, bottom, right, top)
    else:
        extents = (0, 0, 0, 0)
    return extents


def make_network_thumbnail(network, template):
    if template is None:
        return None
    left, bottom, right, top = get_network_extents(network, template['id'])
    if left == right:
        left -= 0.5
        right += 0.5
    else:
        left -= (right - left) * 0.025
        right += (right - left) * 0.025

    if bottom == top:
        bottom -= 0.5
        top += 0.5
    else:
        bottom -= (top - bottom) * 0.025
        top += (top - bottom) * 0.025

    linescale = 100
    pointscale = 100

    w = right - left
    h = top - bottom
    default_radius = max([h / pointscale, w / pointscale])
    linewidth = max([h / linescale, w / linescale])

    # output = io.StringIO()
    dwg = svgwrite.Drawing(profile='tiny')
    links = dwg.add(dwg.g())
    nodes = dwg.add(dwg.g())

    ttypes = {}
    colors = {}
    styles = {}
    if template:
        for tt in template.templatetypes:
            ttypes[tt['id']] = tt
            layout = tt.get('layout', {}) or {}
            color = '#000'
            styles[tt['id']] = {'stroke': '#000'}
            if 'resource_type' not in tt:
                continue
            if tt['resource_type'] == 'NODE':
                svg = layout.get('svg')
                if svg:
                    svg = minidom.parseString(svg)
                    path = svg.getElementsByTagName('path')
                    if path:
                        path = path[0]
                        color = path.getAttribute('fill')
            elif tt.resource_type == 'LINK':
                linestyle = layout.get('linestyle', {})
                if type(linestyle) == str:
                    linestyle = json.loads(linestyle)
                color = linestyle.get('color', color)

            colors[tt['id']] = color

    coord_lookup = {node['id']: [node['x'], node['y']] for node in network['nodes']}

    for res_type in ['nodes', 'links']:
        for resource in network[res_type]:
            if not resource['layout'].get('exists', True):
                continue
            gj = resource['layout'].get('geojson', {})
            extras = {}
            rt = None
            resource_class = 'undefined'
            color = '#000'

            if template:
                rts = [t for t in resource['types'] if t['template_id'] == template['id']]
                if rts:
                    rt = rts[-1]
                    tt = ttypes[rt['id']]
                    resource_class = tt['name'].lower()
                    for s in INVALID_CLASS_CHARACTERS:
                        resource_class = resource_class.replace(s, '-')
                    if 'junction' in resource_class.lower():
                        continue
                    color = colors[tt['id']]

            if gj:
                coords = gj['geometry']['coordinates']
            else:
                coords = [resource['x'], resource['y']] if res_type == 'nodes' else [coord_lookup[resource['node_1_id']],
                                                                               coord_lookup[resource['node_2_id']]]

            if res_type == 'nodes':
                radius = default_radius

                if rt:
                    radius *= 1.5

                extras.update({
                    'class': resource_class,
                    'stroke': color,
                    'fill': color,
                    'stroke-width': radius / 5
                })

                circle = dwg.circle(center=coords, r=radius, **extras)
                nodes.add(circle)
            else:
                polyline = dwg.polyline(points=coords)
                polyline.stroke(color=color, width=linewidth).fill(opacity=0)
                if resource_class:
                    polyline['class'] = resource_class
                links.add(polyline)

    # these are bandaid fixes, but they work. The top+bottom bit is confusing, but again this works.
    links.translate(tx=0, ty=top + bottom)
    links.scale(sx=1, sy=-1)
    nodes.translate(tx=0, ty=top + bottom)
    nodes.scale(sx=1, sy=-1)
    dwg.viewbox(minx=left, miny=bottom, width=w * 1.05, height=h * 1.05)

    return dwg.tostring()


def add_links_from_geojson(hydra, network, template, gj, existings):
    '''Create new a new link or set of links from a newly created geojson polyline.
    This also splits intersected existing links as needed.
    '''

    ttypes = {ttype['name']: ttype for ttype in template.templatetypes}

    link_type = ttypes[gj['properties']['template_type_name']]

    link_name = gj['properties']['name']  # link name

    new_links = []
    is_new = True
    coords = gj['geometry']['coordinates']
    new_coords = [(coords[0])]  # initialize
    new_cnt = 0

    for i, [x, y] in enumerate(coords):
        new_gj = {}
        if is_new:
            new_gj = copy.deepcopy(gj)
            is_new = False
        if i:
            new_coords.append((x, y))

        existing = existings[str(i)]

        node_type = None
        node_name = ''
        if existing['nodeId']:
            is_new = True
        elif i == 0:
            node_type = 'Inflow Node'
            node_name = 'Inflow to {}'.format(link_name)
        elif i == len(coords) - 1:
            node_type = 'Outflow Node'
            node_name = 'Outflow from {}'.format(link_name)
            is_new = True
        else:
            continue

        # need to do this to get the node id for adding the new link
        if node_type and not existing['nodeId']:
            ttype = ttypes[node_type]
            node_description = ''
            node = make_node(template['id'], ttype, node_name, node_description, x, y)
            node = hydra.add_node(network['id'], node)
            network['nodes'].append(node)  # just do this in memory - no need for a Hydra Platform call

        else:
            node = get_node(network, existing['nodeId'])

        if i == 0:
            node1 = node

        if i and is_new:
            new_cnt += 1
            node2 = node
            name, description = link_name_description(network, link_name, link_type['name'], node1, node2,
                                                      segment=new_cnt)
            # new_gj = update_link_gj(new_gj, name, description, node1, node2, new_coords, link_type)
            new_gj['geometry']['coordinates'] = coords
            new_gj['properties'] = {}
            new_link = make_link(network, template['id'], link_type, name, description, node1, node2, gj=new_gj,
                                 segment=new_cnt)
            new_links.append(new_link)
            network['links'].append(new_link)

            # prepare next link
            node1 = node  # first node of next link
            new_coords = [(x, y)]  # first coords of next link

    new_links = hydra.call('add_links', network['id'], new_links)
    # network['links'].extend(new_links)
    return network


def update_link_gj(gj, name, description, node1, node2, coords, ttype):
    gj['geometry']['coordinates'] = coords
    gj['properties'].update({
        'name': name,
        'description': description,
        'node_1_id': node1['id'],
        'node_2_id': node2['id'],
        'template_type_id': ttype['id'],
        'template_type_name': ttype['name'],
        'svg': ttype['layout']['svg']
    })
    gj['properties'].update(json.loads(ttype['layout']['linestyle']))

    return gj


def make_link(network, template_id, ttype, name, description, node1, node2, gj=None, segment=None):
    typesummary = {
        'id': ttype['id'],
        'template_id': template_id
    }
    link = {
        'id': None,
        'name': name,
        'description': description,
        'node_1_id': node1['id'],
        'node_2_id': node2['id'],
        'types': [typesummary],
    }
    if gj:
        link.update({'layout': {'geojson': gj}})

    return link


def link_name_description(network, name, type_name, node1, node2, segment=None):
    name = name.strip()
    if not name or name.isdigit() or name.replace('(', '').replace(')', '').strip().isdigit():
        name = '{} from {} to {}'.format(type_name, node1['name'], node2['name'])
    else:
        # if segment:
        # name += ' ({})'.format(segment)
        # else:
        name = '{} from {} to {}'.format(name, node1['name'], node2['name'])
    description = 'Imported from WEAP Area by OpenAgua'

    existing_names = [link['name'] for link in network['links']]
    old_name = name
    i = 1
    while name in existing_names:
        name = '{} ({})'.format(old_name, i)
        i += 1

    return name, description


def get_node(network, node_id):
    return [node for node in network['nodes'] if node['id'] == node_id][0]


def move_network(hydra, source, destination, project_id, network_id):
    # _load_datauser(url=source)
    # _make_connection()

    if destination == source:
        network = hydra.call('get_network', network_id, summary=True, include_resources=False)
        network['project_id'] = project_id
        hydra.call('update_network', network)

    else:
        network = hydra.call('get_network', network_id, summary=False, include_resources=True,
                             include_data=False)
        network['project_id'] = project_id
        template_id = network['layout'].get('active_template_id')
        cleaned_template = None
        old_template = None
        if template_id:
            old_template = hydra.call('get_template', template_id)
            cleaned_template = clean_template(template=old_template)

        # _load_datauser(url=destination)
        # _make_connection()
        new_template = None
        if template_id and cleaned_template:
            new_template = add_template(template=cleaned_template, is_public=False)
        cleaned_network = clean_network(net=network, old_template=old_template, new_template=new_template)
        new_template_id = new_template['id'] if new_template else None
        new_network = add_network(hydra=hydra, net=cleaned_network, template_id=new_template_id)

    return


def normalize_network(network, network_id=1):
    network['id'] = network_id
    node_map = {}
    link_map = {}
    for i, node in enumerate(network['nodes']):
        idx = i + 1
        node_map[node['id']] = idx
        node['id'] = idx

    for i, link in enumerate(network['links']):
        idx = i + 1
        link_map[link['id']] = idx
        link['node_1_id'] = node_map[link['node_1_id']]
        link['node_2_id'] = node_map[link['node_2_id']]
        link['id'] = idx

    return network


def clone_network(hydra, network_id, **kwargs):
    duplicate_template = kwargs.get('duplicate_template', False)
    new_name = kwargs.get('name')
    include_data = kwargs.get('include_data')
    include_input = kwargs.get('include_input')

    net = get_filtered_network(hydra, network_id, **kwargs)

    old_network_folder = net['layout']['storage']['folder']

    template_id = net['layout'].get('active_template_id')
    old_template = new_template = None
    if duplicate_template:
        if template_id:
            old_template = hydra.call('get_template', template_id)

    net = clean_network(net, old_template=old_template, new_template=new_template, purpose='clone')

    s3_bucket = current_app.config['AWS_S3_BUCKET']
    net = add_storage(net, current_app.config.get('NETWORK_FILES_STORAGE_LOCATION'), force=True)

    net['name'] = new_name

    if not include_input:
        for scen in net['scenarios']:
            scen['resourcescenarios'] = []

    # add the network
    new_network = add_network(hydra, net)

    new_network = hydra.call('get_network', new_network['id'], include_data=True)

    # copy reference layers
    new_network_folder = new_network['layout']['storage']['folder']

    for folder in ['.layers', '.thumbnail']:
        duplicate_folder(s3_bucket, s3_bucket, old_network_folder, new_network_folder, prefix=folder + '/')

    # UPDATE FUNCTIONS WITH NEW RESOURCE IDS

    # get dictionary mapping old resource ID to new resource ID
    new_resource_lookup = {}

    def update_resource_lookup(resource_type, resource):
        new_resource_lookup[(resource_type, resource['layout']['old_id'])] = resource['id']

    update_resource_lookup('network', new_network)
    for node in new_network['nodes']:
        update_resource_lookup('node', node)
    for link in new_network['links']:
        update_resource_lookup('link', link)

    def update_function(func):
        # resultsA = re.findall(r'\'(.+)\'', func)
        # resultsB = re.findall(r'\"(.+)\"', func)
        resultsA = func.split("'")
        resultsB = func.split('"')
        for old_key in resultsA + resultsB:
            parts = old_key.split('/')
            if len(parts) == 3:
                resource_type, resource_id, attr_id = parts
                try:
                    resource_id = int(resource_id)
                    attr_id = int(attr_id)
                except:
                    continue
                new_resource_id = new_resource_lookup.get((resource_type, resource_id))
                if new_resource_id:
                    new_key = '"{}/{}/{}"'.format(resource_type, new_resource_id, attr_id)
                    old_key_extended = "{quote}{key}{quote}".format(
                        key=old_key,
                        quote="'" if old_key in resultsA else '"'
                    )
                    func = func.replace(old_key_extended, new_key)

        return func

    for scen in new_network['scenarios']:
        # parent = scen['layout'].get('parent')
        # if parent:
        #     scen['layout']['parent'] = -scen['layout']['parent']
        # children = scen['layout'].get('children')
        # if children:
        #     scen['layout']['children'] = [-c for c in set(scen['layout']['children'])]

        for rs in scen['resourcescenarios']:
            metadata = json.loads(rs.get('value', {}).get('metadata', ''))
            if metadata and 'function' in metadata:
                metadata['function'] = update_function(metadata.get('function'))
                rs['value']['metadata'] = json.dumps(metadata)

    # reduce network size
    new_network['nodes'] = []
    new_network['links'] = []

    newer_network = hydra.call('update_network', new_network)

    return newer_network


def prepare_network_for_export(network):
    return clean_network(network)


def prepare_template_for_export(template):
    def visit(path, key, value):
        if key in {'cr_date', 'created_by', 'owners', 'image'}:
            return False
        elif key in {'template_id', 'type_id', 'id', 'attr_id'}:
            return key, -value
        # elif key in {'attr_id'}:
        #     return key, -1
        return key, value

    return remap(dict(template), visit=visit)


def get_filtered_network(hydra, network_id, **kwargs):
    include_data = kwargs.get('include_data', True)
    include_input = kwargs.get('include_input', True)
    include_results = kwargs.get('include_results', False)

    network = None
    if include_data and include_input and include_results:
        network = hydra.call('get_network', network_id, include_data=include_data,
                             include_resources=True)
    elif include_data:
        scenario_ids = []
        network = hydra.call('get_network', network_id, include_data=False, include_resources=False,
                             summary=False)
        if include_input:
            scenario_ids = [s['id'] for s in network.scenarios if
                            s['layout'].get('class') in [None, 'baseline', 'option', 'scenario']]
        elif include_results:
            scenario_ids = [s['id'] for s in network.scenarios if s['layout'].get('class') == 'results']
        else:
            include_data = False
        network = hydra.call('get_network', network_id, include_data=include_data, include_resources=True,
                             summary=False, scenario_ids=scenario_ids)

    return network


def nodes_to_array(network, template):
    # Note: Hydra saves coordinates to the nearest 0.0001 degree. However, OpenAgua also stores GeoJSON with the nodes, where higher resolution coordinates can be stored.
    # Here is the OpenAgua GeoJSON approach to getting the coordinates:
    nodes = [['ID', 'Name', 'Type', 'X', 'Y', 'Description']]
    node_lookup = {}
    template_id = template['id']
    for node in network['nodes']:
        coords = node['layout']['geojson']['geometry']['coordinates'] if node['layout'].get('geojson') else [node['x'],
                                                                                                             node['y']]
        resource_types = [rt for rt in node['types'] if rt['template_id'] == template_id]
        nodes.append([
            abs(node['id']),
            node['name'],
            resource_types[0]['name'] if resource_types else 'unknown',
            round(float(coords[0]), 6),
            round(float(coords[1]), 6),
            node['description']
        ])
        node_lookup[node['id']] = node
    return nodes, node_lookup


def links_to_array(network, template, node_lookup):
    links = [['ID', 'Name', 'Type', 'Node_1_ID', 'Node_2_ID', 'Description']]
    for link in network['links']:
        resource_types = [rt for rt in link['types'] if rt['template_id'] == template['id']]
        links.append([
            abs(link['id']),
            link['name'],
            resource_types[0]['name'] if resource_types else 'unknown',
            link['node_1_id'],
            link['node_2_id'],
            link['description']
        ])
    return links


def make_zipped_csv(network, template):
    import csv
    import zipfile

    file_buffer = io.BytesIO()

    with zipfile.ZipFile(file_buffer, "a", zipfile.ZIP_DEFLATED) as zip_file:
        # nodes
        nodes_csv = io.StringIO()
        nodes_writer = csv.writer(nodes_csv)
        nodes, node_lookup = nodes_to_array(network, template)
        nodes_writer.writerows(nodes)
        zip_file.writestr('nodes.csv', nodes_csv.getvalue())

        # links
        links_csv = io.StringIO()
        links_writer = csv.writer(links_csv)
        links = links_to_array(network, template, node_lookup)
        links_writer.writerows(links)
        zip_file.writestr('links.csv', links_csv.getvalue())

    file_buffer.seek(0)
    return file_buffer


def make_xlsx(network, template):
    file_buffer = io.BytesIO()

    with xlsxwriter.Workbook(file_buffer) as workbook:

        bold = workbook.add_format({'bold': True})

        def write_rows(worksheet_name, data, col_widths=None):
            worksheet = workbook.add_worksheet(worksheet_name)
            if col_widths:
                for c, width in enumerate(col_widths):
                    worksheet.set_column(c, c, width)
            for r, row in enumerate(data):
                for c, val in enumerate(row):
                    if r == 0:
                        worksheet.write(r, c, val, bold)
                    else:
                        worksheet.write(r, c, val)

        # nodes
        nodes, node_lookup = nodes_to_array(network, template)
        write_rows('nodes', nodes, col_widths=[5, 30, 20, 10, 10, 50])

        # links
        links = links_to_array(network, template, node_lookup)
        write_rows('links', links, col_widths=[5, 30, 20, 10, 10, 50])

    file_buffer.seek(0)

    return file_buffer


def make_network_shapefiles(network, template, **kwargs):
    import zipfile
    import shapefile

    template_id = template['id']

    def add_shapes(zf, resource_class, resources, resource_type=None):

        if resource_type:
            ttype_id = resource_type['id']
            resources = [r for r in resources if ttype_id in set(map(lambda x: x['id'], r['types']))]
        else:
            resources = [r for r in resources if template_id in set(map(lambda x: x['template_id'], r['types']))]
        if not resources:
            return
        shp = io.BytesIO()
        shx = io.BytesIO()
        dbf = io.BytesIO()
        with shapefile.Writer(shp=shp, shx=shx, dbf=dbf) as shape:
            shape.field('name', 'C')
            shape.field('disp_name', 'C')
            shape.field('desc', 'C')
            shape.field('res_type', 'C')
            for resource in resources:
                layout = resource['layout']
                data = [
                    resource['name'],
                    layout.get('display_name', ''),
                    resource.get('description', ''),
                    list(filter(lambda x: x['template_id'] == template_id, resource['types']))[0]['name']
                ]
                if resource_class == 'node':
                    shape.point(float(resource['x']), float(resource['y']))
                else:
                    geojson = resource['layout'].get('geojson')
                    coords = geojson['geometry']['coordinates']
                    shape.line([coords])
                shape.record(*tuple(data))
        for ext in ['shp', 'shx', 'dbf']:
            if resource_type:
                filename = '{}.{}'.format(resource_type['name'].replace('/', ' - '), ext)
            else:
                filename = '{}.{}'.format(resource_class + 's', ext)
            file = zipfile.ZipInfo(filename)
            fileobj = getattr(shape, ext)
            zf.writestr(file, fileobj.getvalue())

    file_buffer = io.BytesIO()
    with zipfile.ZipFile(file_buffer, "w") as zip_file:
        if kwargs.get('group_by_type'):
            link_types = [tt for tt in template['templatetypes'] if tt['resource_type'] == 'LINK']
            node_types = [tt for tt in template['templatetypes'] if tt['resource_type'] == 'NODE']
            for node_type in node_types:
                add_shapes(zip_file, 'node', network['nodes'], node_type)
            for link_type in link_types:
                add_shapes(zip_file, 'link', network['links'], link_type)

        else:
            add_shapes(zip_file, 'node', network['nodes'])
            add_shapes(zip_file, 'link', network['links'])

    file_buffer.seek(0)

    return file_buffer


def get_network_for_export(hydra, network_id, options, file_format):
    # QC the template

    network = None
    content = None
    ext = file_format

    normalize = options.get('normalize')

    if file_format == 'adjacency':
        ext = 'csv'
        network = hydra.call('get_network', network_id, include_data=False, include_resources=True,
                             summary=True)

        content = make_adjacency(network, flavor='csv')

    elif file_format == 'zip':
        network = hydra.call('get_network', network_id, include_data=False, include_resources=True,
                             summary=True)
        template_id = network['layout'].get('active_template_id')
        template = hydra.call('get_template', template_id)
        if normalize:
            network = normalize_network(network)
        content = make_zipped_csv(network, template)

    elif file_format == 'xlsx':
        network = hydra.call('get_network', network_id, include_data=False, include_resources=True,
                             summary=True)
        template_id = network['layout'].get('active_template_id')
        template = hydra.call('get_template', template_id)
        if normalize:
            network = normalize_network(network)
        content = make_xlsx(network, template)

    elif file_format == 'json':
        include_template = options.get('include_template', True)
        preserve = options.get('preserve', False)
        pretty = options.get('pretty', True)

        network = get_filtered_network(hydra, network_id, **options)

        if preserve:
            cleaned_network = network
        else:
            cleaned_network = clean_network(network, purpose='download')

        if include_template:
            template_id = network['layout'].get('active_template_id')
            template = hydra.call('get_template', template_id)

            if preserve:
                cleaned_template = template
            else:
                cleaned_template = prepare_template_for_export(template)

            content = {
                'network': cleaned_network,
                'template': cleaned_template,
                # 'template_attributes': template_attributes,
            }

        else:
            content = network

        if pretty:
            content = json.dumps(content, sort_keys=True, indent=4, separators=(',', ': '))
        else:
            content = json.dumps(content)

    elif file_format in ['geojson', 'shapefile']:
        network = hydra.call('get_network', network_id, include_data=False, include_resources=True,
                             summary=True)
        network = normalize_network(network)
        template_id = network['layout'].get('active_template_id')
        template = hydra.call('get_template', template_id)
        if file_format == 'geojson':
            geojson = make_feature_collection(network, template, icon=False)
            pretty = options.get('pretty', False)
            if pretty:
                content = json.dumps(geojson, indent=2, sort_keys=False)
            else:
                content = json.dumps(geojson)
        if file_format == 'shapefile':
            ext = 'zip'
            content = make_network_shapefiles(network, template, **options)

    filename = '{}.{}'.format(network['name'], ext) if network and ext else None

    return filename, content


def save_network_preview(hydra, network, filename, contents, location, s3=None, bucket_name=None):
    # add storage if needed
    network = add_storage(network, location)

    # save network preview
    url = upload_network_data(
        network=network,
        bucket_name=bucket_name,
        filename=filename,
        text=contents,
        s3=s3
    )
    if url:
        url += '?{}'.format(datetime.now().timestamp())

        network['layout']['preview'] = {'stale': False, 'url': url}
        hydra.call('update_network', network)

    return url


def import_from_json(hydra, file, project_id):
    content = file.stream.read()
    obj = json.loads(content.decode("utf-8-sig"))
    network = None
    template_id = None
    if 'nodes' in obj:
        net = obj
        tmpl = None
        # template_id = net['layout'].get('active_template_id')
    else:
        net = obj.get('network')
        tmpl = obj.get('template')

    template = None
    if tmpl:
        # old_ttype_lookup = {tt['id']: tt for tt in tmpl['types']}
        tmpl_cleaned = clean_template2(tmpl)
        tmpl_cleaned['layout']['project_id'] = project_id
        template = add_template(template=tmpl_cleaned, is_public=False)
        # new_ttypes = {tt['name']: tt for tt in template['types']}
    # elif template_id:
    #     template = hydra.call('get_template', template_id)

    if net:
        net['project_id'] = project_id
        template_id = template['id'] if tmpl and template else None

        net = prepare_network_for_import(network=net, template=template)

        network = add_network(hydra, net=net, template_id=template_id)

    return network, template


def prepare_network_for_import(network, template=None):
    if template:
        ttypes = {(tt['resource_type'], tt['name']): tt for tt in template['templatetypes']}

    template_id = template['id'] if template else None

    def update_resource(resource, resource_type):
        if resource_type == 'NETWORK':
            resource['id'] = None
            if template_id:
                resource['layout'] = {'active_template_id': template_id}
        else:
            resource['id'] = -abs(resource['id'])
        if resource_type == 'LINK':
            resource['node_1_id'] = -abs(resource['node_1_id'])
            resource['node_2_id'] = -abs(resource['node_2_id'])
        for key in {'cr_date', 'created_by', 'owners'}:
            resource.pop(key, None)
        if template_id:
            resource['types'] = [{'id': ttypes.get((resource_type, rt['name']))['id'], 'template_id': template_id} for
                                 rt in resource.get('types', []) if ttypes.get((resource_type, rt['name']))]
        else:
            resource['types'] = []

    update_resource(network, 'NETWORK')
    for node in network['nodes']:
        update_resource(node, 'NODE')
    for link in network['links']:
        update_resource(link, 'LINK')

    return network


def prepare_network_for_move(network, new_template=None):
    def visit(path, key, value):
        if key in {'cr_date', 'created_by'}:
            return False
        elif key in {'id', 'type_id', 'attr_id', 'template_id', 'template_name'}:
            return key, None
        # elif key in {'template_id'}:
        #     return key, new_template['id']
        # elif key in {'template_name'}:
        #     return key, new_template['name']
        return key, value

    return remap(dict(network), visit=visit)


def make_adjacency(network, flavor='array'):
    n = len(network['nodes'])
    adj = numpy.zeros([n + 1, n + 1], dtype=object)
    adj[0, 0] = ''
    lookup = {}
    for i, node in enumerate(network['nodes']):
        i += 1
        lookup[node['id']] = i
        adj[0, i] = '"{}"'.format(node['name'])
        adj[i, 0] = '"{}"'.format(node['name'])

    for link in network['links']:
        adj[lookup[link['node_1_id']], lookup[link['node_2_id']]] = 1

    if flavor == 'array':
        adj = adj.tolist()

    # resp.data.adjacency.map(d= > JSON.stringify(d)).join('\n').replace( / ( ^\[) | (\]$) / mg, '');
    elif flavor == 'csv':
        adj = "\n".join([",".join(map(str, row)) for row in adj])

    return adj


def update_network_on_mapbox(network, template, endpoint_url, dataset_id, mapbox_creation_token, is_public):
    kwargs = {
        'endpoint': endpoint_url,
        'access_token': mapbox_creation_token,  # "access_token" is mapbox terminology, so let's pass that
        'dataset_id': dataset_id
    }

    if is_public:
        update_features, delete_feature_ids = make_mapbox_features(network, template)
    else:
        update_features = []
        delete_feature_ids = get_feature_ids(network)

    url = '{}/update_features'.format(kwargs.get('endpoint'))
    data = {
        'access_token': kwargs.get('access_token'),
        'dataset_id': kwargs.get('dataset_id'),
        'features': update_features + delete_feature_ids
    }
    requests.post(url, json=data)


def make_mapbox_features(network, template):
    ttypes = {tt['id']: tt for tt in template.templatetypes}
    node_lookup = {node['id']: node for node in network['nodes']}

    update_features = []
    delete_features = []
    for resource_type in ['NODE', 'LINK']:
        for resource in network[resource_type.lower() + 's']:

            ttype_ids = [t['id'] for t in resource['types'] if t['template_id'] == template['id']]
            if not ttype_ids:
                continue

            feature_id = '{}{}'.format(resource_type, resource['id'])

            if not resource.layout.get('exists', True):
                delete_features.append(feature_id)
                continue

            ttype_id = ttype_ids[0]
            ttype = ttypes[ttype_id]

            if resource_type == 'NODE':
                coordinates = [float(resource['x']), float(resource['y'])]
                geo_type = 'Point'

            else:
                coordinates = resource.layout.get('geojson', {}).get('geometry', {}).get('coordinates')
                if not coordinates:
                    node1 = node_lookup[resource['node_1_id']]
                    node2 = node_lookup[resource['node_2_id']]
                    coordinates = [(node1['x'], node1['y']), (node2['x'], node2['y'])]
                geo_type = 'LineString'

            feature = {
                'id': feature_id,
                'type': 'Feature',
                'geometry': {'type': geo_type, 'coordinates': coordinates},
                'properties': {
                    'class': resource_type,
                    'type': ttype['name'],
                    'name': resource['name'],
                    'display_name': resource.layout.get('display_name', resource['name']),
                    'description': resource.description,
                }
            }

            update_features.append(feature)

    return update_features, delete_features


def get_feature_ids(network):
    return ['NODE{}'.format(n['id']) for n in network['nodes']] + ['LINK{}'.format(l['id']) for l in network['links']]


def update_types(hydra, resource, resource_type):
    tpl_ids = []
    updated_types = []
    for type in resource['types'][::-1]:
        if type['template_id'] in tpl_ids:
            hydra.call('remove_type_from_resource', type_id=type['id'], resource_type=resource_type,
                       resource_id=resource['id'])
        else:
            tpl_ids.append(type['template_id'])
            updated_types.append(type)

    return updated_types


def make_node(template_id, ttype, node_name, node_description, x, y, gj=None, id=None):
    node = {
        'id': id,
        'name': node_name,
        'description': 'Added automatically',
        'x': x if type(x) is str else str(x),
        'y': y if type(y) is str else str(y),
        'types': [{
            'id': ttype['id'],
            'template_id': template_id
        }]
    }
    if not gj:
        gj = make_node_gj(x, y, node_name=node_name, node_description=node_description, ttype=ttype)
    node.update({'layout': {'geojson': gj}})

    return node


def make_node_gj(x, y, node_name=None, node_description=None, ttype=None):
    gj = {'type': 'Feature',
          'geometry': {'type': 'Point',
                       'coordinates': (x, y)},
          }
    properties = {}
    if node_name:
        properties.update(name=node_name)
    if node_description:
        properties.update(description=node_description)
    if ttype:
        properties.update(template_type_id=ttype['id'], template_type_name=ttype['name'], svg=ttype.layout.get('svg'))
    gj['properties'] = properties

    return gj


def get_network_settings(db, user_id, source_id, network_id):
    network_settings = db.query(UserNetworkSettings).filter_by(
        user_id=user_id, dataurl_id=source_id, network_id=network_id).first()
    return network_settings and network_settings.settings or {}


def add_update_network_settings(db, user_id, source_id, network_id, settings):
    network_settings = UserNetworkSettings.query.filter_by(
        user_id=user_id, dataurl_id=source_id, network_id=network_id).first()

    if not network_settings:
        network_settings = UserNetworkSettings(
            user_id=user_id,
            dataurl_id=source_id,
            network_id=network_id,
            settings=settings
        )
        db.add(network_settings)
        db.commit()
    else:
        updated_settings = network_settings.settings
        updated_settings.update(settings)
        network_settings.settings = updated_settings
        db.commit()


def delete_network_settings(db, user_id, source_id, network_id):
    network_settings = get_network_settings(db, user_id, source_id, network_id)
    if network_settings:
        db.delete(network_settings)
        db.commit()
