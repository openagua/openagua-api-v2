import json
from copy import deepcopy

from app import config
from app.core.files import upload_network_data, add_storage, s3_bucket, object_url, s3_object_summary
from app.core.templates import get_default_types

from hydra_base import JSONObject

import geojson


def coords_are_equal(p1, p2, precision):
    return round(p1[0], precision) == round(p2[0], precision) and round(p1[1], precision) == round(p2[1], precision)


# convert geoJson node to Hydra node
def geojson_to_node(gj, template_id, i):
    x, y = gj['geometry']['coordinates']

    typesummary = {
        'id': int(gj['properties']['template_type_id']),
        'template_id': template_id
    }
    node = {
        'id': None,
        'name': gj['properties']['name'],
        'description': gj['properties']['description'],
        'x': str(x),
        'y': str(y),
        'types': [typesummary],
        'layout': {'geojson': dict(gj)}
    }

    return node


def correct_network_geojson(network, template):
    corrected_nodes = []
    try:
        for node in network['nodes']:
            node['layout']['geojson'] = make_geojson_from_node(node, template)
            corrected_nodes.append(node)
        network['nodes'] = corrected_nodes
    except:
        pass

    corrected_links = []
    for link in network['links']:
        link['layout']['geojson'] = make_geojson_from_link(network, link, template)
        corrected_links.append(link)
    network['links'] = corrected_links

    return network


# make geojson features
def make_geojson_from_nodes(nodes, template, template_type=None, icon=True):
    geojsons = []
    for node in nodes:

        if template_type:
            resource_types = [rt for rt in node['types'] if rt['id'] == template_type['id']]
            if not resource_types:
                continue

        try:
            geojson = make_geojson_from_node(node, template, template_type, icon=icon)
        except:
            continue
        if geojson:
            geojsons.append(geojson)
        else:
            continue
    return geojsons


def make_geojson_from_node(node, template, template_type=None, icon=True):
    if not template_type:
        type_id = [t['id'] for t in node['types'] if t['template_id'] == template['id']][0]
        ttype = [ttype for ttype in template.templatetypes if ttype['id'] == type_id][0]
    else:
        ttype = template_type

    gj = node['layout'].get('geojson')
    if not gj:
        gj = {'type': 'Feature', 'geometry': {'type': 'Point', 'coordinates': [float(node['x']), float(node['y'])]}}

    # make sure properties are up-to-date
    x, y = gj['geometry']['coordinates']
    gj['geometry']['coordinates'] = [round(float(x), 10), round(float(y), 10)]

    properties = {
        'id': node['id'],
        'name': node['name'],
        'displayname': node['layout'].get('displayname', node['name']),
        'description': node['description'],
        'template_type_name': ttype['name'],
        'template_type_id': ttype['id'],
        'template_name': template['name'],
        'template_id': template['id']
    }
    if 'properties' in gj:
        gj['properties'].update(properties)
    else:
        gj['properties'] = properties
    if icon:
        if 'image' in ttype['layout']:
            gj['properties']['image'] = ttype['layout'].get('image')
        gj['properties']['svg'] = ttype['layout'].get('svg', "")
        gj['properties']['img'] = ttype['layout'].get('img', "")
    else:
        if 'svg' in gj['properties']:
            del gj['properties']['svg']
        if 'image' in gj['properties']:
            del gj['properties']['image']
    return gj


def make_feature_collection(network, template, icon=True):
    features = []
    link_types = [tt for tt in template['templatetypes'] if tt['resource_type'] == 'LINK']
    node_types = [tt for tt in template['templatetypes'] if tt['resource_type'] == 'NODE']
    for link_type in link_types:
        link_features = make_geojson_from_links(network, network['links'], template, template_type=link_type, icon=icon)
        features.extend(link_features)
    for node_type in node_types:
        node_features = make_geojson_from_nodes(network['nodes'], template, template_type=node_type, icon=icon)
        features.extend(node_features)

    features = {'type': 'FeatureCollection',
                'features': features}
    return features


# make geojson features
def make_geojson_from_links(network, links, template, template_type=None, icon=True):
    features = []
    for link in links:

        if template_type:
            resource_types = [rt for rt in link['types'] if rt['id'] == template_type['id']]
            if not resource_types:
                continue

        feature = make_geojson_from_link(network, link, template, template_type=template_type, icon=icon)
        features.append(feature)

    return features


def make_geojson_from_link(network, link, template, template_type=None, icon=True):
    if 'geojson' in link['layout']:
        gj = link['layout'].get('geojson', '{}')
    else:
        coords = get_coords(network['nodes'])

        node_1_id = link['node_1_id']
        node_2_id = link['node_2_id']

        if node_1_id in coords and node_2_id in coords:
            gj = {
                'type': 'Feature',
                'geometry': {
                    'type': 'LineString',
                    'coordinates': [coords[node_1_id], coords[node_2_id]]
                },
                'properties': {}
            }
        else:
            gj = None

    if gj is not None:

        if not template_type:
            type_id = [t['id'] for t in link['types'] if t['template_id'] == template['id']][0]
            ttype = [ttype for ttype in template.templatetypes if ttype['id'] == type_id][0]
        else:
            ttype = template_type

        gj['properties'].update({
            'id': link['id'],
            'name': link['name'],
            'displayname': link['layout'].get('displayname', link['name']),
            'description': link['description'],
            'node_1_id': link['node_1_id'],
            'node_2_id': link['node_2_id'],
            'template_type_name': ttype['name'],
            'template_type_id': ttype['id'],
            'template_name': template['name'],
            'template_id': template['id']
        })

        linestyle = get_linestyle(ttype)
        gj['properties'].update(linestyle)

        if icon:
            if 'svg' in ttype['layout']:
                gj['properties']['svg'] = ttype['layout']['svg']
        else:
            if 'svg' in gj['properties']:
                del gj['properties']['svg']

    return gj


def get_linestyle(ttype):
    linestyle = ttype['layout'].get('linestyle')
    if linestyle:
        if type(linestyle) == str:
            linestyle = json.loads(linestyle)
    else:
        linestyle = {}
        if 'line_weight' in ttype['layout']:
            linestyle['weight'] = ttype['layout']['line_weight']
        else:
            linestyle['weight'] = 3
        if 'colour' in ttype['layout']:
            linestyle['color'] = ttype['layout']['colour']
        else:
            linestyle['color'] = 'black'
        if 'symbol' in ttype['layout']:
            linestyle['dashArray'] = {'solid': '1,0', 'dashed': '5,5'}[ttype['layout']['symbol']]
        else:
            linestyle['dashArray'] = '1,0'
        linestyle['opacity'] = 0.7
        linestyle['lineJoin'] = 'round'
        # linestyle = {'color': 'black',
        # 'dashArray': '1,0',
        # 'weight': 4,
        # 'opacity': 0.7,
        # 'lineJoin': 'round'}
    return linestyle


def make_node_from_geojson(gj, template_id, type_id):
    x, y = gj['geometry']['coordinates']

    typesummary = {
        'id': type_id,
        'template_id': template_id
    }
    node = {
        'id': -1,
        'name': gj['properties']['name'],
        'description': gj['properties']['description'],
        'x': str(x),
        'y': str(y),
        'types': [typesummary],
        'layout': {'geojson': dict(gj)}
    }

    return node


def make_links_from_geojson(hydra, network, template, gj, existings, split_locs):
    '''Create new a new link or set of links from a newly created geojson polyline.
    This also splits intersected existing links as needed.
    '''

    ttypes = {ttype['name']: ttype for ttype in template.templatetypes}
    ttype = ttypes[gj['properties']['template_type_name']]

    default_types = get_default_types(template)
    inflow_type = default_types['inflow']
    outflow_type = default_types['outflow']
    junction_type = default_types['junction']

    linestyle = get_linestyle(ttype)
    gj['properties'].update(linestyle)

    lname = gj['properties']['name']  # link name

    # make nodes
    new_nodes = {}
    new_gjs = []
    del_nodes = []
    del_links = []
    is_new = True
    coords = gj['geometry']['coordinates']
    new_coords = [(coords[0])]  # initialize
    new_cnt = 0
    for i, [x, y] in enumerate(coords):

        if is_new:
            new_gj = deepcopy(gj)
            is_new = False
        if i:
            new_coords.append((x, y))

        existing = existings[str(i)]
        # split_existing = False

        node_type = None
        if existing['linkId']:
            # split_existing = True
            if i:
                is_new = True
        elif existing['nodeId']:
            is_new = True
        elif i == 0:
            node_type = inflow_type['name']
            node_name = '{} {}'.format(lname, node_type)
        elif i == len(coords) - 1:
            node_type = outflow_type['name']
            node_name = '{} {}'.format(lname, node_type)
            is_new = True
        else:
            continue

        make_junction = False
        if existing['linkId'] or existing['nodeId'] and existing['nodeType'] in [inflow_type['name'], outflow_type['name']]:
            make_junction = True
            node_type = junction_type['name']
            xname = round(x, 3)
            yname = round(y, 3)
            if len(lname):
                node_name = '{} {} ({},{})'.format(lname, node_type, xname, yname)
            else:
                node_name = '{} ({},{})'.format(node_type, xname, yname)

        if node_type and not existing['nodeId'] or make_junction:
            ttype = ttypes[node_type]
            node = make_generic_node(template['id'], ttype['id'], node_name, x, y)
            node = hydra.add_node(network['id'], node)
            node['layout']['geojson'] = make_geojson_from_node(node, template)
            node = hydra.call('update_node', node)
            node = hydra.get_node(node['id'])

            new_gjs.append(node['layout']['geojson'])

            if make_junction:
                new_links = update_links(hydra, network, existing['nodeId'], node['id'])
                hydra.call('delete_node', existing['nodeId'], True)
                new_gjs.extend([l['layout']['geojson'] for l in new_links])
                del_nodes.append(existing['nodeId'])
                del_links.extend([l['id'] for l in new_links])

        else:
            node = hydra.get_node(node_id=existing['nodeId'])

        if i == 0:
            node_1 = node

        if i and is_new:
            new_cnt += 1
            new_gj['geometry']['coordinates'] = new_coords
            node_2 = node
            new_link_gj = make_link(hydra, network['id'], template, new_gj, node_1, node_2, segment=new_cnt)
            new_gjs.append(new_link_gj)

            # prepare next link
            node_1 = node  # first node of next link
            new_coords = [(x, y)]  # first coords of next link

        new_nodes[i] = node

    for line_id, splits in split_locs.items():
        idx = str(splits[0]['idx'])
        link_id = existings[idx]['linkId']
        divided_gjs = split_link_at_nodes(hydra, template_id=template['id'], network_id=network['id'],
                                          link_id=int(link_id), nodes=new_nodes, splits=splits)
        new_gjs.extend(divided_gjs)

    return new_gjs, del_nodes, del_links


def make_generic_geojson_from_node(node):
    return {
        'type': 'Feature',
        'geometry': {
            'type': 'Point',
            'coordinates': [node['x'], node['y']]
        }
    }


def make_name(network, restype, type_name, base_name='', ending=None):
    names = [n['name'] for n in network[restype + 's']]
    i = 0
    name = base_name
    while True:
        if base_name:
            name = '{}.{}'.format(base_name, i) if i else base_name
        else:
            name = '{}-{}-{}{}'.format(network['name'][:5].upper(), restype[0].upper(), type_name[:3].upper(), i + 1)
        if ending:
            name += ' ' + ending
        if name not in names:
            break
        i += 1
    return name


def add_link(hydra, network, template, ttypes, incoming_link, existings, split_locs, default_types, del_nodes=[]):
    '''Create new a new link or set of links from a newly created polyline.
    This also splits intersected existing links as needed.
    '''

    inflow_type = default_types.get('inflow')
    outflow_type = default_types.get('outflow')
    junction_type = default_types.get('junction')

    ttype = ttypes[incoming_link['types'][0]['id']]

    # make nodes
    _new_nodes = {}
    new_nodes = []
    new_links = []
    is_new = True
    coords = incoming_link['layout']['geojson']['geometry']['coordinates']
    new_coords = [(coords[0])]  # initialize
    new_cnt = 0
    for i, [x, y] in enumerate(coords):

        if is_new:
            _new_link = deepcopy(incoming_link)
            if not _new_link['layout'].get('display_name'):
                _new_link['layout']['display_name'] = _new_link['name']
            is_new = False
        if i:
            new_coords.append((x, y))

        existing = existings.get(str(i), {})
        # split_existing = False

        node_type = None
        if existing.get("linkId"):
            # split_existing = True
            if i:
                is_new = True
        elif existing.get("nodeId"):
            is_new = True
        elif i == 0:
            node_type = inflow_type
            # node_name = make_name(network, 'node', node_type['name'])
        elif i == len(coords) - 1:
            node_type = outflow_type
            # node_name = make_name(network, 'node', node_type['name'])
            is_new = True
        else:
            continue

        make_junction = False
        replace_existing_node = existing.get('nodeId') and existing.get('nodeType') in [inflow_type['name'],
                                                                                        outflow_type['name']]
        if existing.get('linkId') or replace_existing_node:
            make_junction = True
            node_type = junction_type
            # node_name = make_name(network, 'node', node_type['name'])

        if node_type and not existing.get('nodeId') or make_junction:  # make new inflow, outflow or junction node

            if make_junction:
                node_name = make_name(network, 'node', node_type['name'])

            else:
                has_node = node_type['name'].split(' ')[-1].lower() == 'node'
                if has_node:
                    ending = node_type['name'].replace('Node', '').replace('node', '').strip()
                else:
                    ending = node_type['name']
                node_name = make_name(network, 'node', node_type['name'], base_name=incoming_link.get('name'),
                                      ending=ending)
            node = make_generic_node(template['id'], node_type['id'], node_name, x, y)
            node = hydra.add_node(network['id'], node)
            network['nodes'].append(node)

            new_nodes.append(node)

            if replace_existing_node:
                old_node_id = existing.get('nodeId')
                _new_links = update_links(hydra, network, old_node_id=old_node_id, new_node_id=node['id'])
                new_links.append(_new_links)
                hydra.call('delete_node', old_node_id, True)
                # network['nodes'] = [n for n in network['nodes'] if n['id'] != old_node_id]
                del_nodes.append(old_node_id)

        else:
            node = hydra.call('get_node', existing.get('nodeId'))

        if i == 0:
            node_1 = node

        if i and is_new:
            new_cnt += 1
            node_2 = node

            _new_link['layout']['geojson']['geometry']['coordinates'] = new_coords
            _new_link.update({'node_1_id': node_1['id'], 'node_2_id': node_2['id']})
            _new_link['name'] = make_name(network, 'link', ttype['name'], base_name=_new_link.get('name'))
            # _new_link['attributes'] = [{'attr_id': ta['attr_id']} for ta in ttype['typeattrs']]
            if new_links:
                _new_link['layout']['parent'] = new_links[0]['id']
            _new_link.pop('coords', None)
            new_link = hydra.add_link(network['id'], _new_link)
            new_links.append(new_link)
            network['links'].append(new_link)

            # prepare next link
            node_1 = node  # first node of next link
            new_coords = [(x, y)]  # first coords of next link

        _new_nodes[i] = node

    del_links = []
    for line_id, splits in split_locs.items():
        idx = str(splits[0]['idx'])
        link_id = existings[idx]['linkId']
        # divided_gjs = split_link_at_nodes(hydra=hydra, template_id=template['id'], network_id=network['id'],
        #                                   link_id=int(link_id), nodes=_new_nodes, splits=splits)
        _new_links = split_link_at_nodes2(hydra, network_id=network['id'], template_id=template['id'],
                                          old_link_id=link_id, nodes=_new_nodes,
                                          splits=splits)
        new_links.extend(_new_links)
        del_links.append(link_id)

    return new_nodes, new_links, del_nodes, del_links, network


def make_generic_node(template_id, type_id, node_name, x, y):
    typesummary = {
        'id': type_id,
        'template_id': template_id
    }
    node = {
        'id': None,
        'name': node_name,
        'description': 'Added automatically.',
        'x': str(x),
        'y': str(y),
        'types': [typesummary],
        'layout': {
            'geojson': {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [x, y]
                }
            }
        }
    }

    return node


def split_link_at_nodes(hydra, network_id, template_id, link_id=None, nodes=None, splits=None):
    link = hydra.call('get_link', link_id)

    new_gj = []

    # split geojson - find the new coordinates
    node_1 = hydra.get_node(node_id=link['node_1_id'])
    for i, split in enumerate(splits):
        idx = split['idx']
        # add a new link
        # new_link = link.copy()
        if idx is not None:
            node_2 = nodes[idx]
        else:
            node_2 = hydra.get_node(node_id=link['node_2_id'])
        nl = dict(
            name=link['name'] + '.{}'.format(i + 1),
            description=link['description'],
            layout=link['layout'],
            node_1_id=node_1['id'],
            node_2_id=node_2['id'],
            types=[rt for rt in link['types'] if rt['template_id'] == template_id],
        )
        new_link = hydra.add_link(network_id, nl)

        # update the coordinates
        gj = deepcopy(link['layout']['geojson'])
        old_coords = gj['geometry']['coordinates']
        first_coord = [float(node_1['x']), float(node_1['y'])]
        last_coord = [float(node_2['x']), float(node_2['y'])]
        if i == 0:  # first new segment
            first_coord = None
            last_mid_pt = max(split['prev'], 0)
            mid_coords = old_coords[0:last_mid_pt + 1]
        else:
            first_mid_pt = max(splits[i - 1]['prev'], 0) + 1
            if idx is not None:  # middle new segment (skipped if len(splits) == 2)
                last_mid_pt = split['prev'] + 1
            else:  # last new segment
                first_mid_pt = max(splits[i - 1]['prev'], 0) + 1
                last_mid_pt = None
                last_coord = None

            mid_coords = old_coords[first_mid_pt:last_mid_pt]

        if first_coord and mid_coords and coords_are_equal(first_coord, mid_coords[0], precision=7):
            first_coord = None
        if last_coord and mid_coords and coords_are_equal(last_coord, mid_coords[-1], precision=7):
            last_coord = None

        new_coords = []
        if first_coord:
            new_coords = [first_coord]
        if mid_coords:
            new_coords += mid_coords
        if last_coord:
            new_coords += [last_coord]

        # this may be the only way to resolve duplicate coords at vertices

        gj['geometry']['coordinates'] = new_coords
        gj['properties']['name'] = new_link['name']
        gj['properties']['id'] = new_link['id']
        gj['properties']['node_1_id'] = new_link['node_1_id']
        gj['properties']['node_2_id'] = new_link['node_2_id']
        new_link['layout']['geojson'] = gj
        new_link = hydra.call('update_link', new_link)
        new_link = hydra.get_link(new_link['id'])
        new_gj.append(deepcopy(gj))

        if idx is not None:
            node_1 = nodes[idx]
    hydra.call('delete_link', link['id'], True)

    return new_gj


def split_link_at_nodes2(hydra, network_id, template_id=None, old_link_id=None, nodes=None, splits=None):
    link = hydra.get_link(old_link_id)
    new_links = []

    types = []
    for rt in link.pop('types', []):
        tt = rt.templatetype
        types.append({
            'id': tt['id'],
            'template_id': template_id
        })

    # split geojson - find the new coordinates
    # node_1 = hydra.get_node(node_id=link['node_1_id'])
    node_1 = hydra.call('get_node', link['node_1_id'])
    for i, split in enumerate(splits):
        idx = split['idx']
        # add a new link
        # new_link = link.copy()
        if idx is not None:
            node_2 = nodes[idx]
        else:
            node_2 = hydra.call('get_node', link['node_2_id'])

        nl = deepcopy(link)
        nl.update(
            node_1_id=node_1['id'],
            node_2_id=node_2['id'],
            attributes=None
        )
        if i:  # new links
            nl['types'] = types
            nl['id'] = -i

        nl['layout']['parent'] = link['layout'].get('parent', link['id'])

        j = 1

        while True and j <= 100:
            nl['name'] = '{}.{}'.format(link['name'], i + j)
            if i == 0:
                new_link = hydra.call('update_link', nl)
            else:
                new_link = hydra.add_link(network_id, nl)
            if 'error' in new_link:
                if 'Duplicate entry' not in new_link['error']:
                    break
            else:
                break
            j += 1

        # update the coordinates
        gj = deepcopy(link['layout']['geojson'])
        old_coords = gj['geometry']['coordinates']
        first_coord = (float(node_1['x']), float(node_1['y']))
        last_coord = (float(node_2['x']), float(node_2['y']))
        # first_coord = (float(node_1['x']), float(node_1['y']))
        # last_coord = (float(node_2['x']), float(node_2['y']))
        if i == 0:  # first new segment
            first_coord = None
            last_mid_pt = max(split['prev'], 0)
            mid_coords = old_coords[0:last_mid_pt + 1]
        else:
            first_mid_pt = max(splits[i - 1]['prev'], 0) + 1
            if idx is not None:  # middle new segment (skipped if len(splits) == 2)
                last_mid_pt = split['prev'] + 1
            else:  # last new segment
                first_mid_pt = max(splits[i - 1]['prev'], 0) + 1
                last_mid_pt = None
                last_coord = None

            mid_coords = tuple(old_coords[first_mid_pt:last_mid_pt])

        if first_coord and mid_coords and coords_are_equal(first_coord, mid_coords[0], precision=7):
            first_coord = None
        if last_coord and mid_coords and coords_are_equal(last_coord, mid_coords[-1], precision=7):
            last_coord = None

        new_coords = []
        if first_coord:
            new_coords = [first_coord]
        if mid_coords:
            new_coords += mid_coords
        if last_coord:
            new_coords += [last_coord]

        # this may be the only way to resolve duplicate coords at vertices
        new_link['layout']['geojson']['geometry'].update({'coordinates': new_coords})
        new_link = hydra.call('update_link', new_link)
        new_link = hydra.get_link(new_link['id'])
        new_links.append(new_link)

        if idx is not None:
            node_1 = nodes[idx]
    # hydra.call('delete_link', link['id'], True)

    return new_links


def make_link(hydra, network_id, template, gj, node_1, node_2, segment=None):
    lname = gj['properties']['name']
    desc = gj['properties']['description']

    type_name = gj['properties']['template_type_name']
    type_id = int(gj['properties']['template_type_id'])

    typesummary = {
        'id': type_id,
        'template_id': template['id']
    }

    node_1_id = node_1['id']
    node_2_id = node_2['id']
    link = {'node_1_id': node_1_id, 'node_2_id': node_2_id, 'types': [typesummary]}
    if not desc:
        desc = '{} [{}]'.format(lname, type_name)
    if len(lname):
        if segment:
            lname += ' {}'.format(segment)
        link['description'] = desc
    else:
        node_1_name = hydra.get_node(node_1_id)['name']
        node_2_name = hydra.get_node(node_2_id)['name']
        lname = '{} - {} to {}'.format(type_name, node_1_name, node_2_name)
        link['description'] = '{} from {} to {}'.format(type_name, node_1_name, node_2_name)
    link['id'] = None

    network = hydra.call('get_network', network_id, include_data=False)
    existing_names = [l['name'] for l in network['links']]
    i = 0
    link['name'] = lname
    link['displayname'] = gj['properties']['displayname']
    while link['name'] in existing_names:
        i += 1
        link['name'] = '{} ({})'.format(lname, i)

    # add link to db
    new_link = hydra.add_link(network_id, link)

    # add geojson to link layout
    gj['properties']['name'] = link['name']
    gj['properties']['description'] = link['description']
    gj['properties']['node_1_id'] = node_1_id
    gj['properties']['node_2_id'] = node_2_id
    gj['properties']['id'] = new_link['id']
    new_link['layout'] = {'geojson': gj}
    hydra.call('update_link', new_link)

    return gj


def get_coords(nodes):
    coords = {}
    for node in nodes:
        coords[node['id']] = [float(node['x']), float(node['y'])]
    return coords


def purge_replace_feature(hydra, feature, network, template):
    if feature.geometry.type == 'Point':
        error, new_gj, del_links = purge_replace_point(hydra, network, feature, template)
    else:
        error, new_gj, del_links = purge_replace_line(hydra, feature)
    return error, new_gj, del_links


def purge_replace_features(hydra, features, network, template):
    error = 0
    new_gj_all = []
    del_links_all = []
    for polyline in features.polylines:
        error, new_gj, del_links = purge_replace_line(hydra, polyline)
        if error:
            break
        else:
            new_gj_all.extend(new_gj)
            del_links_all.extend(del_links)
    if not error:
        network = hydra.call('get_network', network['id'])
        for point in features.points:
            error, new_gj, del_links = purge_replace_point(hydra, network, point, template)
            if error:
                break
            else:
                new_gj_all.extend(new_gj)
                del_links_all.extend(del_links)

    return error, new_gj_all, del_links_all


def purge_replace_line(hydra, gj):
    error = 0
    new_gjs = []
    del_links = []

    link_id = gj['properties']['id']
    result = hydra.call('delete_link', link_id, True)
    if result != 'OK':
        result = hydra.call('delete_link', link_id, False)
    if result != 'OK':
        error = 1
    else:
        # load_active_study(hydra)
        del_links = [link_id]

    return error, new_gjs, del_links


def purge_replace_point(hydra, network, gj, template):
    error = 0
    new_gjs = []
    new_node, new_links, del_links = purge_replace_node(hydra, network, template, gj)
    if new_node:
        new_gj = make_geojson_from_node(new_node, template)
        new_gjs.append(new_gj)
    if new_links:
        for link in new_links:
            new_gj = make_geojson_from_link(network, link, template)
            new_gjs.append(new_gj)
    return error, new_gjs, del_links


def purge_replace_node(hydra, network, template, gj):
    ttypes = {ttype['name']: ttype for ttype in template.templatetypes}
    node_id = gj['properties']['id']
    type_name = gj['properties']['template_type_name']
    adj_links = [l for l in network['links'] if node_id in [l['node_1_id'], l['node_2_id']]]

    if type_name == 'Junction':
        new_node = None
        new_links = []
        del_links = [l['id'] for l in adj_links]

        # delete the downstream node and modify the upstream node
        if len(adj_links) == 2 \
                and (adj_links[0]['node_2_id'] == adj_links[1]['node_1_id'] \
                     or adj_links[1]['node_2_id'] == adj_links[0]['node_1_id']):
            # uplink = [l for l in adj_links if l['node_2_id'] == node_id][0]
            # downlink = [l for l in adj_links if l['node_1_id'] == node_id][0]
            # uplink['node_2_id'] = downlink['node_2_id']
            # new_link = hydra.call('update_link', uplink)
            # hydra.call('delete_link', downlink, False)
            new_links = update_links(hydra, network, node_id)
    elif type_name in ['Inflow Node', 'Outflow Node']:
        new_node = None
        new_links = []
        del_links = [l['id'] for l in adj_links]
    else:
        # update existing adjacent links
        if adj_links:
            if len(adj_links) > 1:
                replacement_type = 'Junction'
                lname = ' + '.join([l['name'] for l in adj_links])
                node_name = '{} {}'.format(lname, replacement_type)
            elif adj_links[0]['node_1_id'] == node_id:
                replacement_type = 'Inflow Node'
                lname = adj_links[0]['name']
                node_name = '{} {}'.format(lname, replacement_type)
            else:
                replacement_type = 'Outflow Node'
                lname = adj_links[0]['name']
                node_name = '{} {}'.format(lname, replacement_type)
            x, y = [float(i) for i in gj['geometry']['coordinates']]
            ttype = ttypes[replacement_type]
            node = make_generic_node(template['id'], ttype['id'], node_name, x, y)
            new_node = hydra.add_node(network['id'], node)
            if 'faultcode' in new_node and 'already in network' in new_node['faultstring']:
                new_node = [n for n in network['nodes'] if n['name'] == node_name][0]
            # if len(adj_links) == 2 and \
            #                 adj_links[0]['layout']['geojson']['properties']['template_type_name'] == \
            #                 adj_links[1]['layout']['geojson']['properties']['template_type_name']:
            #     new_links = update_links(hydra, network, node_id)
            # else:
            new_links = update_links(hydra, network, node_id, new_node['id'])
            del_links = [l['id'] for l in new_links]
        else:
            new_node = None
            new_links = []
            del_links = []

    # purge node (adjacent links are deleted if not updated)
    hydra.call('delete_node', node_id, True)

    return new_node, new_links, del_links


def update_links(hydra, network, old_node_id, new_node_id=None):
    updated_links = []

    if new_node_id is not None:
        for link in network['links']:
            if old_node_id in [link['node_1_id'], link['node_2_id']]:
                new_link = deepcopy(link)
                if link['node_1_id'] == old_node_id:
                    new_link['node_1_id'] = new_node_id
                    new_link['layout']['geojson']['properties']['node_1_id'] = new_node_id
                elif link['node_2_id'] == old_node_id:
                    new_link['node_2_id'] = new_node_id
                    new_link['layout']['geojson']['properties']['node_2_id'] = new_node_id
                updated_link = hydra.call('update_link', new_link)
                updated_links.append(updated_link)
    else:
        for link in network['links']:
            if old_node_id == link['node_2_id']:
                new_link = deepcopy(link)
            if old_node_id == link['node_1_id']:
                old_link = deepcopy(link)
        new_link['node_2_id'] = old_link['node_2_id']
        new_link['layout']['geojson']['properties']['node_2_id'] = new_link['node_2_id']
        new_link['layout']['geojson']['geometry']['coordinates'] \
            .extend(old_link['layout']['geojson']['geometry']['coordinates'][1:])
        hydra.call('delete_link', old_link['id'], False)
        updated_link = hydra.call('update_link', new_link)
        updated_links.append(updated_link)
    return updated_links


def update_links2(hydra, old_node_id, new_node_id=None, old_link_ids=[]):
    """Update existing links with new information"""

    updated_links = []

    if new_node_id is not None:
        # we are just updating the link at the beginning or end
        for lid in old_link_ids:
            link = hydra.call('get_link', lid)
            if old_node_id in [link['node_1_id'], link['node_2_id']]:
                new_link = deepcopy(link)
                if link['node_1_id'] == old_node_id:
                    new_link['node_1_id'] = new_node_id
                elif link['node_2_id'] == old_node_id:
                    new_link['node_2_id'] = new_node_id
                updated_link = hydra.call('update_link', new_link)
                updated_links.append(updated_link)
    else:
        # we are purging an existing node and joining the two links; this needs fixing
        for lid in old_link_ids:
            link = hydra.call('get_link', lid)
            if old_node_id == link['node_2_id']:
                new_link = deepcopy(link)
            if old_node_id == link['node_1_id']:
                old_link = deepcopy(link)
        new_link['node_2_id'] = old_link['node_2_id']
        new_link['layout']['geojson']['properties']['node_2_id'] = new_link['node_2_id']
        new_link['layout']['geojson']['geometry']['coordinates'] \
            .extend(old_link['layout']['geojson']['geometry']['coordinates'][1:])
        hydra.call('delete_link', old_link['id'], False)
        updated_link = hydra.call('update_link', new_link)
        updated_links.append(updated_link)
    return updated_links


def get_network_references(hydra, network, update_url=True):
    refs = []
    bucket_name = config.AWS_S3_BUCKET
    if 'refs' in network['layout']:
        if 's3' in network['layout']['storage']['location'].lower():
            folder = network['layout']['storage']['folder']
            for ref in network['layout']['refs']:
                if isinstance(ref, int):
                    ref = {'id': ref}
                if 'url' not in ref or update_url:
                    filename = '{}.json'.format(ref['id'])
                    key = '{}/{}'.format(folder, filename)
                    obj = s3_object_summary(bucket_name, key)
                    url = object_url(obj)
                    ref['url'] = url
                refs.append(ref)

    if 'references' in network['layout']:
        del network['layout']['references']
        hydra.call('update_network', network)
    if 'reference_layers' in network['layout']:
        del network['layout']['reference_layers']
        hydra.call('update_network', network)

    return refs


def repair_feature_coords(feature_coords):
    new_coords = []
    for part_coords in feature_coords:
        new_part_coords = []
        for old_coords in part_coords:
            if len(old_coords) == 2 and type(old_coords[0]) == type(old_coords[1]) == float:
                new_part_coords.append(old_coords)
        if new_part_coords:
            new_coords.append(new_part_coords)

    return new_coords


# def repair_feature(feature):
#     all_new_coords = []
#
#     if feature['type'] == 'Feature':
#         feature_coords = feature['geometry']['coordinates']
#         new_coords = repair_feature(feature_coords)
#         if new_coords:
#             all_new_coords.append(new_coords)
#
#     elif feature['type'] == 'Polygon':
#
#
#     feature['geometry']['coordinates'] = all_new_coords
#
#     return feature


# def repair_geojson(geojson):
#     gtype = geojson['type']
#     if gtype == 'FeatureCollection':
#         repaired_features = []
#         for feature in geojson['features']:
#             repaired_features = repair_feature(feature)
#         geojson['features'] = repaired_features
#     elif gtype == 'Feature':
#         geojson = repair_feature(geojson)
#     return geojson


def repair_network_references(network, update_url=True):
    refs = {}
    ref_ids = []
    actual_refs = []
    for ref in network['layout'].get('refs', []):
        url = ref.get('url')
        if url:
            path = '/'.join(url.split('/')[4:])
        else:
            path = ref.get('path')
        if path and path not in refs and path[:7] == '.layers':
            refs[path] = ref
            ref_ids = int(ref['id'])

    storage = network['layout'].get('storage', {})
    location = storage.get('location', config.NETWORK_FILES_STORAGE_LOCATION)
    network_folder = storage.get('folder')
    if network_folder and 's3' in location.lower():
        bucket_name = config.AWS_S3_BUCKET
        bucket = s3_bucket(bucket_name)
        folder = '{}/.layers/'.format(network_folder)
        cnt = 0
        for obj in bucket.objects.filter(Prefix=folder):
            key = obj.key
            filename = key.replace(folder, '')
            path = key.replace(network_folder, '')[1:]
            if path[-1] == '/':
                continue
            cnt += 1
            geojson_string = obj.get()['Body'].read().decode()
            gj = geojson.loads(geojson_string)
            if gj.is_valid:
                if path in refs:
                    actual_refs.append(path)
                    continue
            else:
                obj.delete()
                refs.pop(path, None)
                continue

            new_id = max(ref_ids) + cnt if ref_ids else cnt
            ref_ids.append(new_id)

            ref = {
                'id': new_id,
                'path': path,
                'name': filename,
            }
            refs[path] = ref
            actual_refs.append(path)

    for path in list(refs.keys()):
        if path not in actual_refs:
            refs.pop(path)

    network['layout']['refs'] = list(refs.values())

    return network


def add_network_reference(hydra, network, reference, geojson):
    location = config.NETWORK_FILES_STORAGE_LOCATION
    network = add_storage(network, location)

    '''Store a reference geojson as a file.'''
    new_id = 1
    ref_ids = []
    refs = network['layout'].get('refs', [])
    for ref in refs:
        if isinstance(ref, dict) and ref.get('id'):
            ref_ids.append(ref['id'])

    if ref_ids:
        new_id = max(ref_ids) + 1

    # add id to geojson
    geojson['id'] = new_id

    # save geojson
    filename = '.layers/{}.json'.format(new_id)
    url = upload_network_data(
        network,
        bucket_name=config.AWS_S3_BUCKET,
        filename=filename,
        text=json.dumps(geojson)
    )

    # update network layout
    reference.update({
        'id': new_id,
        'path': filename
    })

    if not network['layout'].get('refs'):
        network['layout']['refs'] = []
    network['layout']['refs'].append(reference)
    hydra.call('update_network', network)

    return reference


def update_network_reference(network, geojson, filename):
    upload_network_data(
        network,
        bucket_name=config.AWS_S3_BUCKET,
        filename=filename,
        text=json.dumps(geojson)
    )
    return


def delete_network_references(hydra, network_id, reference_ids):
    network = hydra.call('get_network', network_id, include_resources=False, include_data=False,
                         summary=True)

    keepers = []
    if 's3' in network['layout']['storage']['location'].lower():
        bucket = s3_bucket(config.AWS_S3_BUCKET)
        folder = network['layout']['storage']['folder']

        for ref in network['layout']['refs']:
            if type(ref) == int:
                ref = {'id': ref}
            if ref['id'] not in reference_ids:
                #     path = ref.get('path')
                #     if path:
                #         delete_from_s3(bucket, path)
                # else:
                keepers.append(ref)
    else:
        # placeholder
        keepers = network['layout']['refs']

    # update network
    network['layout']['refs'] = keepers
    hydra.call('update_network', network)

    return
