import xml.etree.ElementTree as ET

from app.core.users import get_datauser

from boltons.iterutils import remap

unknown_svg = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 15 15" height="15" width="15"><title>circle-15.svg</title><rect fill="none" x="0" y="0" width="15" height="15"></rect><path fill="red" transform="translate(0 0)" d="M14,7.5c0,3.5899-2.9101,6.5-6.5,6.5S1,11.0899,1,7.5S3.9101,1,7.5,1S14,3.9101,14,7.5z"></path></svg>'


def make_ttypes(template):
    ttypes = {}
    for ttype in template['templatetypes']:
        ttypes[ttype['id']] = ttype
    return ttypes


def make_ttype_dict(template):
    return {tt['name']: tt for tt in template['templatetypes']}


def get_tattrs(template):
    tattrs = {}
    for t in template['templatetypes']:
        for ta in t['typeattrs']:
            tattrs[ta['attr_id']] = ta.copy()
    return tattrs


def get_res_attrs(network, template):
    '''Create a dictionary of resource and attribute information for each resource attribute. Keys are resource attribute IDs.'''

    tattrs = get_tattrs(template)
    # attrs = hydra.call('get_all_attributes')
    # attrs = {attr.id: attr for attr in attrs}
    res_attrs = {}
    for obj_type in ['Nodes', 'Links']:
        features = network[obj_type.lower()]
        for f in features:
            ttype = [t['name'] for t in f['types'] if t['template_id'] == template['id']][0]
            for ra in f['attributes']:
                if ra['attr_id'] not in tattrs:
                    continue
                res_attr = {'res_name': f['name'], 'res_type': ttype, 'obj_type': obj_type}
                res_attr.update(tattrs[ra['attr_id']])
                res_attrs[ra.id] = res_attr
    return res_attrs


def get_used_ttypes(hydra, network, template, incl_vars=False):
    '''Remove template types that do not actually exist in the model'''

    if incl_vars:
        var_types = ['Y', 'N']
    else:
        var_types = ['N']

    res_attrs = get_res_attrs(network, template)
    # TODO: update hydra with get_all_node_attributes
    node_attrs = hydra.call('get_all_node_attributes', network['id'], template['id'])
    node_types = []
    for na in node_attrs:
        res_type = res_attrs[na.id]['res_type']
        if res_type not in node_types:
            node_types.append(res_type)
    link_attrs = g.hydra.call('get_all_link_attributes', network['id'], template['id'])
    link_types = []
    for la in link_attrs:
        res_type = res_attrs[la.id]['res_type']
        if res_type not in link_types:
            link_types.append(res_type)

    ttypes = {}
    ttypes_all = make_ttypes(template)
    for tt in ttypes_all:
        ttype = ttypes_all[tt]
        if ttype.resource_type == 'NODE' and ttype['name'] in node_types:
            ttypes[tt] = ttype
        elif ttype.resource_type == 'LINK' and ttype['name'] in link_types:
            ttypes[tt] = ttype
        else:
            continue

        tattrs = [ta for ta in ttypes[tt]['typeattrs'] if ta['attr_is_var'] in var_types]
        if len(tattrs):
            ttypes[tt]['typeattrs'] = tattrs
        else:
            ttypes.pop(tt)

    return ttypes


def get_templates(hydra, user_id):
    datauser = get_datauser(url=hydra.url, user_id=user_id)
    all_templates = hydra.call('get_templates')
    userid = datauser.userid
    templates = []
    for tpl in all_templates or []:
        # is_public = tpl.layout.get('is_public', True)
        # if is_public:
        #     templates.append(tpl)
        # else:
        for owner in tpl.owners:
            if owner.user_id == userid or owner.user_id == 1:
                templates.append(tpl)
                break

    templates.sort(key=lambda x: x['name'], reverse=True)
    templates.sort(key=lambda x: x['name'], reverse=True)

    return templates


def change_active_template(db, hydra, source_id, network=None, network_id=None, new_template_id=None):
    network = network or hydra.call('get_network', network_id, include_resources=True, include_data=False)
    current_tpl = network['layout'].get('active_template_id')
    if current_tpl is not None:
        old_template_id = current_tpl if type(current_tpl) == int else current_tpl['id']
    else:
        old_template_id = None
    old_types = None

    new_template_id = new_template_id or network['layout'].get('active_template_id')

    new_tpl = hydra.call('get_template', new_template_id)

    # update network types
    existing_types = [rt['id'] for rt in network.types]
    for tt in new_tpl['templatetypes']:
        if tt.resource_type != 'NETWORK':
            continue

        if tt['id'] not in existing_types:
            rts = [rt for rt in network['types'] if rt['name'] == tt['name']]
            if rts:
                # get from existing network type
                rt = rts[0]
                rt['id'] = tt['id']
                rt['template_id'] = new_tpl.id
            else:
                # create new network type
                rt = {
                    'template_id': new_tpl.id,
                    'name': tt['name'],
                    'id': tt['id'],
                }
            network['types'].append(rt)

    # map from old template to new template
    new_types = {(tt.resource_type, tt['name'].lower()): tt for tt in new_tpl['templatetypes']}
    new_types_by_id = {tt['id']: tt for tt in new_tpl['templatetypes']}

    def update_resource_types(resource, resource_class=None):
        nonlocal old_types

        rt = None

        matching = [rt for rt in resource['types'] if rt['template_id'] == new_template_id]
        if matching:
            rt = matching[0]

        else:
            tt = None
            for old_rt in reversed(resource.types):  # search newest types first
                old_name_lower = old_rt['name'].lower()
                if (resource_class, old_name_lower) in new_types:
                    tt = new_types[(resource_class, old_name_lower)]
                    break
            rt_updated = False
            if not tt and resource.types:
                old_rt = resource.types[-1]
                if old_types is None and old_template_id is not None:
                    old_tpl = hydra.call('get_template', old_template_id)
                    old_types = {(resource_class, tt['name'].lower()): tt for tt in old_tpl['templatetypes']}
                else:
                    old_types = {}
                # add the old template type to the new template
                # this should only work if the user has permission to modify the new template
                old_name_lower = old_rt['name'].lower()
                if (resource_class, old_name_lower) in old_types.keys():
                    tt = old_types[(resource_class, old_name_lower)]
                    new_name_lower = tt['name'].lower()
                    del tt['id']
                    del tt['cr_date']
                    tt['template_id'] = new_template_id
                    for ta in tt['typeattrs']:
                        ta['attr_id'] = None
                        ta.pop('cr_date')
                    tt = hydra.call('add_templatetype', tt)
                    new_types[
                        (resource_class, new_name_lower)] = tt  # add to new template (will update new template at end)
                    new_types_by_id[tt['id']] = tt
                    rt_updated = True

                if not rt_updated:
                    # This is bad: it means the resource has no types at all. There are two options, both bad:
                    # 1) delete the resource
                    # 2) assign some arbitrary, catchall type
                    # for now, the latter is selected, with a type of UNKNOWN added to the template
                    tt = new_types.get((resource_class, 'unknown'))
                    if tt is None:
                        unknowntype = {
                            'template_id': new_template_id,
                            'name': 'UNKNOWN',
                            'resource_type': resource_class,
                            'layout': {'svg': unknown_svg}
                        }
                        tt = hydra.call('add_templatetype', unknowntype)
                        new_types_by_id[tt['id']] = tt

            if tt:
                rt = {
                    'template_id': new_template_id,
                    'name': tt['name'],
                    'id': tt['id']
                }
                # add new template type to network
                # TODO: Add multiple template capability; for now, only one template is assumed

        if rt:
            resource['types'] = [_rt for _rt in resource['types'] if _rt['id'] != rt['id']] + [rt]

            # add missing attributes
            tt = new_types_by_id[rt['id']]
            rattrs = set([ra['attr_id'] for ra in resource['attributes']])
            tattrs = set([ta['attr_id'] for ta in tt['typeattrs']])
            missing_attrs = tattrs - rattrs
            new_attrs = [
                {'attr_id': ta['attr_id'], 'attr_is_var': ta['attr_is_var']}
                for ta in tt['typeattrs'] if ta['attr_id'] in missing_attrs]
            resource['attributes'].extend(new_attrs)

        return resource

    network['nodes'] = [update_resource_types(res, 'NODE') for res in network['nodes']]
    network['links'] = [update_resource_types(res, 'LINK') for res in network['links']]

    set_active_model(db, hydra, source_id, network)

    return network, new_tpl


def prepare_template_for_import(template, internal=True):
    def visit(path, key, value):
        if key in {'cr_date', 'created_by', 'owners', 'image', 'id', 'template_id'}:
            return False
        if key in {'attr_id'}:
            return (key, abs(value)) if internal else (key, -abs(value))
        elif key in {'template_id', 'type_id'}:
            return key, -abs(value)
        return key, value

    return remap(template, visit=visit)


def clean_template(template=None):
    def visit(path, key, value):
        if key in {'created_by', 'cr_date', 'owners', 'image'}:
            return False
        elif key in {'type_id'}:
            # return key, -abs(value)
            return False
        elif key in {'id', 'attr_id', 'template_id'}:
            return key, None
        elif key == 'types':
            return 'templatetypes', value
        return key, value

    cleaned = remap(template, visit=visit)

    return cleaned


def clean_template2(template):
    if 'layout' not in template:
        template['layout'] = {}
    for key in ['id', 'created_by', 'cr_date', 'owners']:
        template.pop(key, None)
    for i, tt in enumerate(template['templatetypes']):
        for key in ['id', 'template_id', 'cr_date']:
            tt.pop(key, None)
        for ta in tt['typeattrs']:
            for key in ['type_id', 'cr_date']:
                ta.pop(key, None)
            ta['attr_id'] = None
            if 'attr' in ta:
                ta['name'] = ta['attr']['name']
                ta['attr_id'] = ta['attr']['id']

    return template


def add_template(hydra, template, is_public=False):
    i = 0
    old_name = template['name']
    while True:
        result = hydra.call('add_template', template)
        if result.get('name'):
            break
        elif 'Duplicate' in result.get('error', '') and i < 100:
            i += 1
            template['name'] = '{} ({})'.format(old_name, i)
        else:
            break

    return result


def get_default_types(template, mapping=None):
    template_default_types = template['layout'].get('default_types', {})
    mapping = mapping or dict(
        inflow="Inflow",
        outflow="Outflow",
        junction="Junction"
    )
    default_types = {}
    for key in ['inflow', 'outflow', 'junction']:
        if key in template_default_types:
            templatetypes = [tt for tt in template['templatetypes'] if tt['id'] == int(template_default_types[key])]
            if templatetypes:
                default_types[key] = templatetypes[0]
        else:
            default_name = mapping[key]
            templatetypes = [tt for tt in template['templatetypes'] if tt['name'] == default_name]
            if templatetypes:
                default_types[key] = templatetypes[0]
    return default_types


def upload_template(hydra, zf, tpl_name):
    '''Upload a template from a zipfile.'''

    template_xml_path = zf.namelist()[0]
    template_xml = zf.read(template_xml_path).decode('utf-8')

    root = ET.fromstring(template_xml)
    for name in root.iter('template_name'):
        name.text = tpl_name
    new_xml = ET.tostring(root).decode()

    template = hydra.call('upload_template_xml', new_xml)

    return template
