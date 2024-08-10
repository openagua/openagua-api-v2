from pathlib import Path
import json

from fastapi import APIRouter, Request, HTTPException, Response, Depends
from app import schemas
from app.deps import get_g
from app.core.templates import add_template, clean_template, prepare_template_for_import

ALLOWED_EXTENSIONS = ['.json']

api = APIRouter(tags=['Templates'])


@api.get('/templates')
def _get_templates(project_id: int | None = None, load_all: bool = True,
                   ownership: str | None = None, template_ids: list[int] = [],
                   exclude_user: bool = False, exclude_public: bool = False, g=Depends(get_g)):
    templates = []
    project_template_ids = []

    is_public_user = g.is_public_user

    if project_id or template_ids:
        if project_id:
            templates = g.hydra.call('get_templates', project_id=project_id)
            try:
                project_template_ids = [t['id'] for t in templates]
            except:
                print(templates)
        if template_ids:
            template_ids = [tid for tid in template_ids if tid not in project_template_ids]
            other_templates = g.hydra.call('get_templates', template_ids=template_ids)
            try:
                templates.extend(other_templates)
            except:
                template_ids_str = str(template_ids)
                raise HTTPException(f'Something went wrong processing templates IDs {template_ids_str}')

        for template in templates:
            try:
                if template['layout'].get('project_id'):
                    template['project_id'] = template['layout']['project_id']
                    del template['layout']['project_id']
                    g.hydra.call('update_template', template, update_types=False)
            except:
                template_id = template['id']
                raise HTTPException(f'Something went wrong processing template ID {template_id}')

    elif is_public_user:
        templates = g.hydra.call('get_templates', uid=g.hydra.user_id, public_only=True, load_all=load_all)
        if exclude_user:
            user_id = g.datauser.userid
            templates = [t for t in templates if not any(o for o in t['owners'] if o['user_id'] == user_id)]

    else:
        templates = g.hydra.call('get_templates', uid=g.hydra.user_id, load_all=load_all)
        if exclude_public:
            templates = [t for t in templates if not t['is_public']]
        if ownership == 'shared':
            user_id = g.datauser.userid
            templates = [t for t in templates if not t['created_by'] == user_id]

    return templates


@api.post('/templates', response_model=schemas.Template, status_code=201)
def _add_templates(request: Request, template: schemas.Template, fork: bool = False, g=Depends(get_g)):
    file = request.files.get('file')
    if file:
        if Path(file.filename).suffix in ALLOWED_EXTENSIONS:
            # filename = secure_filename(file.filename)
            content = file.stream.read()
            template = json.loads(content.decode("utf-8-sig"))
            template = clean_template(template=template)
            template = add_template(g.hydra, template)
            return template
        else:
            raise HTTPException(415, 'Unsupported file type')

    else:
        if fork:
            template.pop('id', None)
            template['layout']['base_template_id'] = template['id']
            cleaned = clean_template(template=template)
        else:
            cleaned = prepare_template_for_import(template, internal=True)
        template = add_template(g.hydra, cleaned)
        return template


@api.get('/templates/{template_id}', response_model=schemas.Template)
def _get_template(template_id: int, g=Depends(get_g)):
    template = g.hydra.call('get_template', template_id)
    return template


@api.put('/templates/{template_id}', response_model=schemas.Template)
def _update_template(template: schemas.Template, template_id: int, g=Depends(get_g)):
    updated = g.hydra.call('update_template', template)
    return updated


@api.patch('/templates/{template_id}')
def patch_template(updates: object, template_id: int, g=Depends(get_g)):
    template = g.hydra.call('get_template', template_id)
    template.update(updates)
    result = g.hydra.call('update_template', template)
    if 'faultcode' in result:
        raise HTTPException(405, 'Name already taken.')
    else:
        return Response(204)


@api.patch('/templates/{template_id}')
def _delete_template(template_id: int, delete_types: bool = False, g=Depends(get_g)):
    resp = g.hydra.call('delete_template', template_id, delete_resourcetypes=delete_types)
    if 'error' in resp:
        raise HTTPException(501, 'Could not delete types')
    else:
        return Response(204)


@api.post('/templatetypes', status_code=201)
def _add_template_type(templatetype, g=Depends(get_g)):
    ttype = g.hydra.call('add_templatetype', templatetype)
    return ttype


@api.put('/templatetypes/{template_type_id}')
def _update_template_type(templatetype, template_type_id: int, g=Depends(get_g)):
    if templatetype['resource_type'] == 'LINK':
        templatetype['layout'].pop('svg', None)
    for ta in templatetype.get('typeattrs', []):
        ta.pop('default_dataset', None)  # it's unclear why this is needed
    g.hydra.call('update_templatetype', templatetype)
    return Response(204)


@api.delete('/templatetypes/{template_type_id}')
def _delete_templatetype(template_type_id, g=Depends(get_g)):
    g.hydra.call('delete_templatetype', template_type_id)
    return Response(204)


@api.post('/typeattrs', status_code=201)
def _add_type_attribute(typeattr, g=Depends(get_g)):
    attr = dict(name=typeattr['attr_name'], dimension_id=typeattr['dimension_id'])
    attr = g.hydra.call('add_attribute', attr)
    typeattr['attr_id'] = attr['id']
    ret_typeattr = g.hydra.call('add_typeattr', typeattr)
    ret_typeattr['attr'] = attr
    return ret_typeattr


@api.put('/typeattrs/{typeattr_id}')
def _update_type_attribute(typeattr, typeattr_id: int, g=Depends(get_g)):
    if 'attr_is_var' in typeattr:
        typeattr['is_var'] = typeattr.pop('attr_is_var')

    attr = g.hydra.call('add_attribute', dict(name=typeattr['attr_name'], dimension_id=typeattr['dimension_id']))
    if typeattr['attr_id'] == attr['id']:
        # attribute hasn't changed
        typeattr.pop('dimension_id')
    ttype = g.hydra.call('get_templatetype', typeattr['type_id'])
    existing_attr_ids = [ta['attr_id'] for ta in ttype['typeattrs']]

    if attr['id'] not in existing_attr_ids:
        g.hydra.call('remove_attr_from_type', ttype['id'], typeattr['attr_id'])
        typeattr['attr_id'] = attr['id']
        # TODO: double check if the following is still needed with hydra_base
        typeattr.pop('default_dataset', None)
        ret = g.hydra.call('add_typeattr', typeattr)
    else:
        ttype['typeattrs'] = [typeattr if typeattr['attr_id'] == ta['attr_id'] else ta for ta in
                              ttype['typeattrs']]
        g.hydra.call('update_templatetype', ttype)
        ret = typeattr

    ret['attr'] = attr

    return ret


@api.put('/typeattrs/{typeattr_id}')
def _delete_typeattr(typeattr_id: int, g=Depends(get_g)):
    g.hydra.call('delete_typeattr', typeattr_id)
    return Response(204)


@api.get('/dimensions')
def _get_dimensions(full: bool = True, g=Depends(get_g)):
    dimensions = g.hydra.call('get_dimensions', full=full)
    return dimensions


@api.get('/units/{unit_id}', response_model=schemas.Unit)
def _get_unit(unit_id: int, include_dimension: bool = True, g=Depends(get_g)):
    unit = g.hydra.call('get_unit', unit_id)
    if include_dimension:
        dimension = g.hydra.call('get_dimension', unit['dimension_id'])
        dimension.pop('units', None)
        unit['dimension'] = dimension
    return unit
