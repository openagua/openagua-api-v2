from os import environ as env
from os.path import splitext

from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import Response, FileResponse
from typing import List
from pydantic import HttpUrl

from app.deps import get_g
from app.services import s3
from app.schemas import Network
from app import config

from app.core.networks import get_network, update_types, get_network_for_export, \
    make_network_thumbnail, save_network_preview, clone_network, move_network, import_from_json, \
    get_network_settings, add_update_network_settings, delete_network_settings
from app.core.sharing import set_resource_permissions, share_resource
from app.core.files import delete_all_network_files
from app.core.network_editor import update_links2, split_link_at_nodes2
from app.core.modeling import update_network_model
from app.core.templates import change_active_template

# from openagua.lib.addins.weap_import import import_from_weap

# from openagua import socketio

api = APIRouter(tags=['Networks'])


@api.get('/networks')
async def _get_networks(network_ids: List[int], include_resources: bool = False, g=Depends(get_g)):
    networks = []
    for network_id in network_ids:
        network = g.hydra.call('get_network', network_id, include_resources=include_resources, summary=True,
                              include_data=False)
        networks.append(network)

    return networks


@api.post('/networks', status_code=201)
async def _add_network(request: Request, purpose: str | None = None, g=Depends(get_g)):
    data = await request.json()
    if purpose == 'import':

        file = request.files['file']

        project_id = request.form.get('project_id', type=int)
        network_name = request.form.get('network_name')
        flavor = request.form.get('flavor')
        # filename = secure_filename(file.filename)
        filename = file.filename
        ext = splitext(file.filename)[-1]

        network = None
        template = None

        if ext in ['.zip', '.weap']:
            # # TODO: examine contents of zip file to auto-detect if it is from WEAP
            # template_name = 'WaterLP v0.3'  # TODO: get from setting or user input
            # network, template = import_from_weap(hydra=g.hydra, file=file, project_id=project_id,
            #                                      template_name=template_name, network_name=network_name)
            raise HTTPException(500, 'Not yet implemented')

        elif ext == '.json':
            if flavor == 'json':
                network, template = import_from_json(g.hydra, file, project_id=project_id)
            else:
                network, template = import_from_json(g.hydra, file, project_id=project_id)

        return network

    if purpose == 'clone':
        options = data.get('options')
        network_id = data.get('network_id')
        network = clone_network(g.hydra, network_id=network_id, **options)
        return network

    if purpose == 'move':
        source = data.get('source')
        destination = data.get('destination')
        project_id = data.get('project_id')
        network_id = data.get('network_id')
        move_network(g.hydra, source=source, destination=destination, project_id=project_id, network_id=network_id)
        return Response(204)

    net = data.get('network')
    # scen = request.json.get('scenario')
    network = g.hydra.call('add_network', net)
    # if scen:
    #     scenario = g.hydra.call('add_scenario', network['id'], scen)
    #     network['scenarios'] = [scenario]
    # else:
    #     network['scenarios'] = network.get('scenarios', [])
    return network


# TODO: move repair to a completely different route?
@api.get('/networks/{network_id}')
def _get_network(network_id: int, simple: bool = False, summary: bool = True, purpose: str | None = None,
                 include_resources: bool = True,
                 repair: bool = False, repair_options: list = [], download_options: dict | None = None,
                 file_format: str = 'json', g=Depends(get_g)):
    if purpose == 'download':

        if file_format in ['zip', 'xlsx', 'shapefile']:
            filename, file_buffer = get_network_for_export(g.hydra, network_id, repair_options, file_format)
            return FileResponse(file_buffer, filename=filename, content_disposition_type="attachment")

        elif file_format:
            filename, network = get_network_for_export(g.hydra, network_id, download_options, file_format)
            return network

    network = get_network(
        g.hydra,
        g.source_id,
        network_id,
        simple=simple,
        include_resources=include_resources,
        repair=repair,
        repair_options=repair_options
    )

    if network is None:
        raise HTTPException(511, 'No network found')

    elif 'error' in network:
        raise HTTPException(403, str(network))

    network['scenarios'] = [s for s in network['scenarios'] if
                            not (s['layout'].get('class') == 'results' and s['parent_id'])]
    return network


@api.put('/networks/{network_id}')
def _update_network(network_id: int, network: Network, g=Depends(get_g)):
    return g.hydra.call('update_network', network)


@api.patch('/networks/{network_id}', status_code=204)
async def _patch_network(network_id: int, request: Request, g=Depends(get_g)):
    data = await request.json()
    layout = data.get('layout', {})

    network = g.hydra.call('get_network', network_id, include_data=False, include_resources=False)

    if layout:

        # update template ID
        active_template_id = layout.get('active_template_id')
        if active_template_id != network['layout'].get('active_template_id'):
            change_active_template(g.db, g.hydra, g.study.id, network=network, new_template_id=active_template_id)

        # update model ID
        active_model_id = layout.get('model_id')
        if active_model_id != network['layout'].get('model_id'):
            update_network_model(g.db, g.hydra.url, network_id=network_id, model_id=active_model_id)

        network['layout'].update(layout)

    else:
        network.update(data)

    resp = g.hydra.call('update_network', network)

    if 'error' in resp:
        raise HTTPException(500, resp)

    else:
        return Response(204)


@api.delete('/networks/{network_id}')
def _delete_network(network_id, g=Depends(get_g)):
    network = g.hydra.call('get_network', network_id, include_resources=False, include_data=False,
                        summary=True)
    # note that purge_data is required, but not used in the Hydra function
    resp = g.hydra.call('delete_network', network_id, True)

    if resp == 'OK':
        bucket_name = env['AWS_S3_BUCKET']
        delete_all_network_files(network, bucket_name, s3=s3)
        delete_network_settings(g.db, g.current_user.id, g.source_id, network_id)
        return Response(204)
    else:
        raise HTTPException(500, resp)


@api.get('/networks/{network_id}/settings')
def _get_network_settings(network_id: int, g=Depends(get_g)):
    settings = get_network_settings(g.db, g.current_user.id, g.datauser.dataurl_id, network_id)
    return settings


@api.post('/networks/{network_id}/settings', status_code=204)
async def _add_network_settings(request: Request, network_id: int, g=Depends(get_g)):
    settings = await request.json()
    add_update_network_settings(g.db, g.current_user.id, g.source_id, network_id, settings)


@api.put('/networks/{network_id}/settings', status_code=204)
async def _update_network_settings(request: Request, network_id: int, g=Depends(get_g)):
    settings = await request.json()
    add_update_network_settings(g.db, g.current_user.id, g.source_id, network_id, settings)


@api.get('/networks/{network_id}/attribute_scenarios')
def _get_network_attribute_scenario(network_id: int, g=Depends(get_g)):
    network = g.hydra.call('get_network', network_id, include_data=False)
    template_id = g.hydra.get_template_id_from_network(network)
    template = g.hydra.call('get_template', template_id)

    tattrs = {tt['id']: {ta['attr_id']: ta for ta in tt['typeattrs']} for tt in template['templatetypes']}

    def simplify(scenario):
        return {
            'id': scenario['id'],
            'name': scenario['name'],
            'class': scenario['layout']['class'] if 'class' in scenario['layout'] else 'baseline'
        }

    scenarios = [simplify(s) for s in network['scenarios'] if
                 s['layout'].get('class') in ['baseline', 'option', 'scenario']]
    # for i, scenario in enumerate(network.scenarios):
    #     layout = scenario['layout']
    #     if layout.get('class') in ['option', 'scenario']:
    #         scenarios.append(simplify(scenario))

    res_attr_scens = {'nodes': {}, 'links': {}}
    for res_type in ['nodes', 'links']:
        for resource in network[res_type]:
            rattrs = []
            typeids = [rt['id'] for rt in resource['types'] if rt['template_id'] == template_id]
            if typeids:
                typeid = typeids[0]
                tas = tattrs.get(typeid)
                for ra in resource['attributes']:
                    # filter by attribute scope and limit to what's in the template
                    ta = tas.get(ra['attr_id'])
                    if ta and ta['attr_is_var'] == 'N':
                        rattrs.append({
                            'id': ra['id'],
                            'name': ta['attr']['name'],
                            'scenarios': scenarios
                        })
                res_attr_scens[res_type][resource['id']] = rattrs

    return res_attr_scens


@api.get('/networks/{network_id}/preview_url')
def _get_preview_url(network_id: int, g=Depends(get_g)) -> HttpUrl:
    network = g.hydra.call('get_network', network_id, summary=True, include_resources=True)
    template_id = network['layout'].get('active_template_id')
    template = template_id and g.hydra.call('get_template', template_id)
    svg = make_network_thumbnail(network, template)
    url = save_network_preview(
        g.hydra,
        network=network,
        filename='.thumbnail/preview.svg',
        contents=svg,
        location=config.NETWORK_FILES_STORAGE_LOCATION,
        s3=s3,
        bucket_name=config.AWS_S3_BUCKET
    )
    return url


@api.get('/networks/{network_id}/preview_svg')
def _get_preview_svg(network_id: int, g=Depends(get_g)) -> str:
    network = g.hydra.call('get_network', network_id, summary=True, include_resources=True)
    template_id = network['layout'].get('active_template_id')
    template = template_id and g.hydra.call('get_template', template_id)
    svg = make_network_thumbnail(network, template)
    return svg


# @api.route('/networks/{network_id}/reference_layers')
# class ReferenceLayers(Resource):
# 
#     @api.doc('Add a reference layer to a network')
#     @api.param('realtime', 'Specify this after returning from a long-running task')
#     def post(network_id):
# 
#         realtime = request.args.get('realtime', False, bool)
# 
#         reference = request.json.get('reference')
#         geojson = request.json.get('geojson')
#         network = g.hydra.call('get_network', network_id, include_resources=False, summary=True,
#                               include_data=False)
#         reference = add_network_reference(g.hydra, network, reference, geojson)
# 
#         if realtime:
#             # add the reference to the client via socketio
#             room = current_app.config['NETWORK_ROOM_NAME'].format(source_id=g.hydra.dataurl_id, network_id=network_id)
#             event = 'add-reference-layer'
#             socketio.emit(event, reference, room=room)
# 
#             return 'success', 200
# 
#         else:
#             return jsonify(reference)
# 
#     def put(network_id):
#         references = request.json.get('references')
# 
#         network = g.hydra.call('get_network', network_id, include_data=False, include_resources=False, summary=True)
# 
#         updated = []
#         updatedIds = []
#         for reference in references:
#             # filter reference to make sure nothing really large is accidentally passed in (like geojson or paths)
#             id = reference.get('id')
#             updated.append({
#                 'id': reference.get('id'),
#                 'path': reference.get('path'),
#                 'name': reference.get('name'),
#                 'visible': reference.get('visible'),
#                 'style': reference.get('style'),
#                 'labelField': reference.get('labelField', '')
#             })
#             updatedIds.append(id)
# 
#             geojson = reference.get('geojson')
#             if geojson:
#                 update_network_reference(network, geojson, filename='{}.json'.format(reference['id']))
# 
#         refs = [ref for ref in network['layout'].get('refs', []) if
#                 type(ref) in [dict, AttrDict] and ref.get('id') not in updatedIds]
#         refs.extend(updated)
#         network['layout']['refs'] = refs
#         g.hydra.call('update_network', network)
# 
#         return '', 204
# 
#     def delete(network_id):
#         reference_ids = request.args.getlist('referenceIds[]', type=int)
#         delete_network_references(g.hydra, network_id, reference_ids)
#         return '', 204


@api.post('/networks/{network_id}/permissions', status_code=201)
async def _add_network_permissions(request: Request, network_id: int, g=Depends(get_g)):
    data = await request.json()
    emails = data['emails']
    permissions = data['permissions']
    message = data.get('message', '')
    results = share_resource(g.db, g.hydra, 'network', network_id, emails, permissions, message=message)
    return results


@api.put('/networks/{network_id}/permissions', status_code=204)
async def put(request: Request, network_id: int, g=Depends(get_g)):
    data = await request.json()
    permissions = data['permissions']
    for username, _permissions in permissions.items():
        set_resource_permissions(g.hydra, 'network', network_id, username, _permissions)


@api.route('/nodes')
async def post(request: Request, network_id: int, template_id: int, g=Depends(get_g)):
    # for single nodes
    data = await request.json()
    incoming_node = data.get('node')
    _existing = data.get('existing')
    _split_locs = data.get('splitLocs')

    # for multiple nodes
    incoming_nodes = data.get('nodes', [incoming_node])

    nodes = []
    links = []
    del_nodes = []
    del_links = []

    for incoming in incoming_nodes:

        # create the new node
        if incoming.get('id', 0) > 0:
            incoming['types'] = update_types(g.db, incoming, 'NODE')
            node = g.hydra.call('update_node', incoming)
            nodes.append(node)
        else:

            existing = incoming.pop('existing', _existing)
            split_locs = incoming.pop('splitLocs', _split_locs)
            incoming.pop('resType', None)

            node = g.hydra.add_node(network_id, incoming)
            nodes.append(node)

            if existing:
                old_node_id = existing['0'].get('nodeId')
                old_link_id = existing['0'].get('linkId')
                new_links = []
                if old_node_id:

                    # update existing links and delete old node
                    old_link_ids = existing['0'].get('linkIds', [])
                    new_links = update_links2(g.hydra, old_node_id=old_node_id, new_node_id=node['id'],
                                              old_link_ids=old_link_ids)
                    g.hydra.call('delete_node', old_node_id, False)
                    del_nodes.append(old_node_id)

                elif old_link_id:  # there should be only one, but existing includes an array
                    splits = next(iter(split_locs.values()))
                    new_links = split_link_at_nodes2(g.hydra, network_id=network_id,
                                                     template_id=template_id,
                                                     old_link_id=old_link_id,
                                                     nodes=[node],
                                                     splits=splits)
                    del_links.append(old_link_id)
                else:
                    error = 1  # we shouldn't get here, obviously

                links.extend(new_links)

            error = 0

    return dict(nodes=nodes, links=links, del_nodes=del_nodes, del_links=del_links)


@api.put('/nodes', status_code=204)
async def _update_nodes(request: Request, g=Depends(get_g)):
    data = await request.json()
    nodes = data.get('nodes', [])
    for node in nodes:
        g.hydra.call('update_node', node)


@api.delete('/nodes', status_code=204,
            description='Bulk delete multiple nodes. WARNING: This currently does not have the "merge" option as when \
            deleting a single node; adjacent links will also be deleted.')
def _delete_nodes(ids: List[int], g=Depends(get_g)):
    for id in ids:
        g.hydra.call('delete_node', id, True)

# @api.route('/nodes/<int:node_id>')
# class Node(Resource):
#
#     @api.doc('Update a single node')
#     def put(node_id):
#
#         incoming_node = request.json.get('node')
#         should_update_types = request.args.get('update_types', 'false') == 'true'
#
#         links = []
#         old_node_id = None
#         old_link_ids = []
#
#         if should_update_types:
#             incoming_node['types'] = update_types(incoming_node, 'NODE')
#
#         resp = g.hydra.call('update_node', incoming_node)
#         node = g.hydra.call('get_node', incoming_node['id'])
#
#         # This is a hack to account for Hydra Platform differences between nodes from networks and from get_node
#         resource_types = []
#         for rt in node['types']:
#             tt = rt.pop('templatetype')
#             tt.pop('typeattrs')
#             resource_types.append(tt)
#         node['types'] = resource_types
#         return jsonify(nodes=[node], links=links, del_nodes=[old_node_id], del_links=old_link_ids)
#
#     @api.doc(
#         description='Delete a single node',
#         params={
#             'method': 'How to handle adjacent links. Options are "delete" or "merge". "merge" (default) joins '
#                       'adjacent links (this only works for two adjacent links), while "delete" removes adjacent links '
#                       '(the default in Hydra Platform). "merge" merges the downstream link into the upstream link; '
#                       'data for the downstream link will be lost.'
#         }
#     )
#     @api.response(200, 'The new link, if the merge method is used')
#     @api.response(204, 'Nothing, if the delete method is used')
#     def delete(node_id):
#         node = g.hydra.call('get_node', node_id)
#         method = request.args.get('method', 'delete')  # normal delete should be default
#         if method == 'delete':
#             resp = g.hydra.call('delete_node', node_id, True)
#             return 'Success', 204
#         elif method == 'merge':
#             up_link_id = request.args.get('up_link_id', type=int)
#             down_link_id = request.args.get('down_link_id', type=int)
#             links = g.hydra.call('get_links', node['network_id'], link_ids=[up_link_id, down_link_id])
#             up_link = next((x for x in links if x['id'] == up_link_id), None)
#             down_link = next((x for x in links if x['id'] == down_link_id), None)
#
#             # merge links...
#             up_link['node_2_id'] = down_link['node_2_id']
#             if up_link['layout'].get('geojson') and down_link['layout'].get('geojson'):
#                 up_link['layout']['geojson']['geometry']['coordinates'].extend(
#                     down_link['layout']['geojson']['geometry']['coordinates'][1:]
#                 )
#             up_link.pop('types', None)
#             new_link = g.hydra.call('update_link', up_link)
#             new_link = g.hydra.get_link(new_link['id'])
#             g.hydra.call('delete_link', down_link_id, True)
#             g.hydra.call('delete_node', node_id, True)
#
#             return jsonify(new_link)
#
#
# links_fields = api.model('Links', {
#     'link': fields.String,
#     'links': fields.String,
#     'existing': fields.String,
#     'splitLocs': fields.String
# })
#
#
# @api.route('/links')
# class Links(Resource):
#
#     @api.doc(description='Add multiple links to a network')
#     @api.param('network_id', 'Network ID')
#     @api.param('template_id', 'Template ID (optional)')
#     @api.expect(links_fields)
#     def post(self):
#         network_id = request.args.get('network_id', type=int)
#         template_id = request.args.get('template_id', type=int)
#
#         # for single link
#         incoming_link = request.json.get('link')
#         _existing = request.json.get('existing')
#         _split_locs = request.json.get('splitLocs', {})
#
#         # for multiple links
#         incoming_links = request.json.get('links', [incoming_link])
#
#         nodes = []
#         links = []
#         del_nodes = []
#         del_links = []
#
#         if incoming_links:
#             network = g.hydra.call('get_network', network_id, include_resources=True, include_data=False, summary=True)
#             template_id = template_id or network['layout'].get('active_template_id')
#             template = g.hydra.call('get_template', template_id)
#             templatetypes = {tt['id']: tt for tt in template.templatetypes}
#
#             # TODO: get inflow/outflow node time from template
#             default_types = get_default_types(template)
#
#             # create the new link(s)
#             for incoming in incoming_links:
#
#                 existing = incoming.pop('existing', _existing)
#                 split_locs = incoming.pop('splitLocs', _split_locs)
#
#                 incoming.pop('resType', None)
#
#                 if incoming.get('id', -1) <= 0 or existing:
#
#                     _new_nodes, _new_links, _del_nodes, _del_links, network = add_link(
#                         hydra=g.hydra,
#                         network=network,
#                         template=template,
#                         ttypes=templatetypes,
#                         incoming_link=incoming_link,
#                         existings=existing,
#                         split_locs=split_locs,
#                         default_types=default_types,
#                         del_nodes=[]
#                     )
#
#                     nodes.extend(_new_nodes)
#                     links.extend(_new_links)
#                     del_nodes.extend(_del_nodes)
#                     del_links.extend(_del_links)
#
#                 else:
#                     incoming['types'] = update_types(incoming, 'LINK')
#                     g.hydra.call('update_link', incoming)
#                     links.append(incoming)
#
#         return jsonify(nodes=nodes, links=links, del_nodes=del_nodes, del_links=del_links)
#
#     @api.doc('Update multiple links.')
#     def put(self):
#
#         links = request.json.get('links', [])
#         for link in links:
#             g.hydra.call('update_link', link)
#
#         return '', 204
#
#     @api.doc(description='Bulk delete multiple links.')
#     def delete(self):
#         ids = request.args.getlist('ids[]')
#         for id in ids:
#             g.hydra.call('delete_link', id, True)
#
#         return '', 204
#
#
# @api.route('/links/<int:link_id>')
# class Link(Resource):
#
#     @api.doc(description='Update a link')
#     def put(link_id):
#         link = request.json.get('link')
#         link.pop('coords', None)
#         update_types = request.json.get('update_types', False)
#         if update_types:
#             link['types'] = update_types(link, 'LINK')
#         updated_link = g.hydra.call('update_link', link)
#         return jsonify(updated_link)
#
#     @api.doc(description='Delete a link')
#     def delete(link_id):
#         g.hydra.call('delete_link', link_id, True)
#         return '', 204
#
#
# @api.route('/resources')
# class Resources(Resource):
#
#     @api.doc(description='Delete multiple resources (nodes and links)')
#     @api.param('node_ids[]', 'The list of node IDs to delete')
#     @api.param('link_ids[]', 'The list of link IDs to delete')
#     def delete(self):
#         node_ids = request.args.getlist('node_ids[]', type=int)
#         link_ids = request.args.getlist('link_ids[]', type=int)
#
#         def _delete_resource(resource_type, resource_id):
#             fn = 'delete_{}'.format(resource_type)
#             try:
#                 g.hydra.call(fn, resource_id, True)
#             except:
#                 g.hydra.call(fn, resource_id, False)
#
#         for link_id in link_ids:
#             delete_resource('link', link_id)
#         for node_id in node_ids:
#             delete_resource('node', node_id)
#
#         return '', 204
#
#
# @api.route('/resource_groups')
# class ResourceGroups(Resource):
#
#     @api.doc(description='Add resource group')
#     def post(self):
#         network_id = request.args.get('network_id')
#         group = request.json.get('group')
#         rg = g.hydra.call('add_resourcegroup', group, network_id)
#         return jsonify(rg)
#
#
# @api.route('/resource_groups/<int:group_id>')
# class ResourceGroup(Resource):
#
#     @api.doc(description='Update resource group')
#     def put(group_id):
#         group = request.json.get('group')
#         rg = g.hydra.call('update_resourcegroup', group=group)
#         return jsonify(rg)
#
#
# @api.route('/resource_attributes')
# class ResourceAttributes(Resource):
#
#     # def get(self):
#     #     type_id = request.args.get('type_id', type=int)
#     #     res_id = request.args.get('id', type=int)
#     #     res_type = request.args.get('res_type').lower()
#     #     active_res_attr_id = request.args.get('active_res_attr', type=int)
#     #
#     #     res_attrs = g.hydra.call('get_{}_attributes'.format(res_type), **{
#     #         '{}_id'.format(res_type): res_id,
#     #         'type_id': type_id
#     #     })
#     #
#     #     # add templatetype attribute information to each resource attribute
#     #     tattrs = {}
#     #     if type_id:
#     #         tt = g.hydra.call('get_templatetype', type_id)
#     #         tattrs = {ta['attr_id']: ta for ta in tt['typeattrs'] if ta['attr_is_var'] == 'N'}
#     #
#     #     ret = []
#     #     for ra in res_attrs:
#     #         if ra['attr_id'] in tattrs:
#     #             ra.update({
#     #                 'tattr': tattrs[ra['attr_id']],
#     #                 'active': ra.id == active_res_attr_id
#     #             })
#     #             ret.append(ra)
#     #     return jsonify(res_attrs=ret)
#
#     @api.doc('Add resource attribute')
#     def post(self):
#         res_type = request.json['res_type']
#         res_id = request.json['res_id']
#         attr_id = request.json['attr_id']
#         is_var = request.json['is_var']
#         group = request.json.get('group')
#
#         # get/create group
#         if group and group['id'] is None:
#             group = g.hydra.call('add_group', group)
#         else:
#             group = None
#
#         # create attribute
#         is_var = 'Y' if is_var else 'N'
#         res_attr = g.hydra.call(
#             'add_resource_attribute', **{
#                 'resource_type': res_type,
#                 'resource_id': res_id,
#                 'attr_id': attr_id,
#                 'is_var': is_var
#             })
#
#         # add to group
#         if group:
#             group_attr = g.hydra.call('add_group_attribute', group_id=group.id, attr_id=res_attr.id, is_var=is_var)
#             res_attr['group_id'] = group.id
#
#         return jsonify(res_attr)
#
#
# @api.route('/resource_attributes/<int:res_attr_id>')
# class ResourceAttribute(Resource):
#
#     @api.doc(
#         description='Update a resource attribute'
#     )
#     def put(res_attr_id):
#         res_attr = request.json
#         is_var = res_attr.get('attr_is_var', 'N')
#         unit = res_attr.get('unit', '')
#         data_type = res_attr.get('data_type', '')
#         description = res_attr.get('description', '')
#         properties = res_attr.get('properties') or {}
#         resp = g.hydra.call(
#             'update_resource_attribute',
#             resource_attr_id=res_attr_id, is_var=is_var, unit=unit, data_type=data_type,
#             description=description, properties=json.dumps(properties))
#         return '', 204
#
#     @api.doc(
#         description='Delete a resource attribute'
#     )
#     def delete(res_attr_id):
#         g.hydra.call('delete_resource_attribute', res_attr_id)
#         return '', 200
#
#
# @api.route('/nodes/<int:node_id>/resource_types/<int:type_id>')
# class LinkResourceType(Resource):
#
#     @api.doc('Remove a type from a node')
#     def delete(node_id, type_id):
#         g.hydra.call('remove_type_from_resource', type_id=type_id, resource_type='NODE', resource_id=node_id)
#         return '', 200
#
#
# @api.route('/links/<int:link_id>/resource_types/<int:type_id>')
# class LinkResourceType(Resource):
#
#     @api.doc('Remove a type from a link')
#     def delete(link_id, type_id):
#         g.hydra.call('remove_type_from_resource', type_id=type_id, resource_type='LINK', resource_id=link_id)
#         return '', 200
#
# # CORS-protected routes
#
# # @api0.route('/networks/{network_id}/public_map', methods=['PUT'])
# # @login_required
# # def update_networks_map(network_id):
# #     is_public = request.args.get('is_public') == 'true'
# #
# #     endpoint_url = current_app.config['MAPBOX_UPDATE_ENDPOINT']
# #     dataset_id = current_app.config['MAPBOX_DATASET_ID']
# #     mapbox_creation_token = current_app.config['MAPBOX_CREATION_TOKEN']
# #
# #     network = g.hydra.call('get_network', network_id, include_resources=True, include_data=False,
# #                           summary=True)
# #     template_id = network['layout'].get('active_template_id')
# #     if not template_id:
# #         return '', 500
# #     template = g.hydra.call('get_template', template_id)
# #     update_network_on_mapbox(network, template, endpoint_url, dataset_id, mapbox_creation_token, is_public)
# #     return '', 200
# #
# #
# # @api0.route('/network/settings', methods=['PUT'])
# # @login_required
# # def _update_network_settings():
# #     network_id = request.json['network_id']
# #
# #     settings = request.json.get('settings')
# #     layout = request.json.get('layout')
# #     model_id = request.json.get('model_id')
# #
# #     network = g.hydra.call('get_network', network_id, summary=True, include_resources=False,
# #                           include_data=False)
# #
# #     if layout:
# #         network['layout'].update(layout)
# #
# #         active_template_id = layout.get('active_template_id')
# #         if active_template_id != network['layout'].get('active_template_id'):
# #             network, new_tpl = change_active_template(g.hydra, g.study.id, network=network,
# #                                                       new_template_id=active_template_id)
# #
# #     if settings:
# #         current_settings = network['layout'].get('settings', {})
# #         current_settings.update(settings)
# #         network['layout']['settings'] = current_settings
# #
# #     if model_id:
# #         update_network_model(g.hydra.url, network_id=network_id, model_id=model_id)
# #
# #     if layout or settings:
# #         g.hydra.call('update_network', network)
# #
# #     return '', 204
