from os import getenv

from app import config
import hydra_base as hb
# from hydra_base.lib import objects
import hydra_client as hydra
import logging

log = logging.getLogger(__name__)


class HydraConnection(object):
    def __init__(self, url, id=None, is_root=False, session_id=None, app_name=None, user_id=None, username=None,
                 password=None):

        self.url = url
        self.id = id
        self.is_root = is_root
        self.app_name = app_name
        self.session_id = session_id
        self.username = username
        self.user_id = user_id

        if url == 'base':
            self.hydra = hydra.JSONConnection(
                session=hb.db.DBSession,
                # session_id=session_id,
                user_id=user_id
            )
            # self.session_id = self.hydra.session_id
        else:
            self.hydra = hydra.RemoteJSONConnection(url=url)

            if self.session_id is None:
                if username and password:
                    self.login(username, password)

    def login(self, username, password):
        self.session_id = None
        self.username = username
        self.hydra.login(username, password)
        self.user_id = self.hydra.user_id
        # self.session_id = self.hydra.session_id

    def call(self, fn, *args, **kwargs):
        kwargs['user_id'] = self.user_id
        # Convert any boolean parameters to 'Y' or 'N'.
        # This is conditional - some Hydra functions take Y/N, others take True/False.
        if ('_scenario' not in fn and '_template' not in fn) or fn == 'update_template':
            for kwarg in kwargs.keys():
                if type(kwargs[kwarg]) == bool:
                    kwargs[kwarg] = 'Y' if kwargs[kwarg] else 'N'
            for item in ['project', 'network']:
                if item in kwargs and 'owners' in kwargs[item]:
                    del kwargs[item]['owners']
        try:
            # TODO: this is potentially dangerous. double check that this doesn't have unintended consequences
            self.hydra.autocommit = fn[:4] != 'get_'
            resp = self.hydra.call(fn, *args, **kwargs)
        except Exception as err:
            return {'error': str(err)}
        # hb.db.DBSession.close()
        return resp

    def update_add_data_user(self, admin_username, admin_password, username, password, role='modeller'):

        # login with admin account
        # self.login(username=admin_username, password=admin_password)
        user = self.get_user_by_name(username)
        if user:  # update it
            data_user = self.call('update_user_password', user.id, password)
        else:  # add it
            hydra_user = {'username': username, 'password': password}
            data_user = self.call('add_user', hydra_user)
            role_obj = self.call('get_role_by_code', role)
            self.call('set_user_role', data_user.id, role_obj.id)
        return data_user

    def get_user_by_name(self, username):
        user = self.call('get_user_by_name', username)
        if user:
            del user['password']
        return user

    def get_link(self, link_id):
        # This is needed because hydra returns links in a different format than when with a network
        link = self.call('get_link', link_id)
        for t in link['types']:
            t.update(t['templatetype'])
        return link

    def get_node(self, node_id):
        # This is needed because hydra returns nodes in a different format than when with a network
        node = self.call('get_node', node_id)
        for t in node['types']:
            t.update(t['templatetype'])
        for a in node['attributes']:
            a.update(a['attr'])
        return node

    def add_link(self, network_id, link):
        link = self.call('add_link', network_id, link)
        if 'error' in link:
            return link
        else:
            return self.get_link(link['id'])

    def add_node(self, network_id, node):
        node = self.call('add_node', network_id, node)
        return self.get_node(node['id'])

    def get_template_id_from_network(self, network):
        if 'active_template_id' in network['layout']:
            template_id = network['layout']['active_template_id']
        else:
            template_id = network.types[0]['template_id']
            network['layout']['active_template_id'] = template_id
            self.call('update_network', network)
        return template_id

    def get_res_attr_data(self, **kwargs):
        result = self.call(
            'get_resource_attribute_data',
            ref_key=kwargs['resource_type'].upper(),
            ref_id=int(kwargs['resource_id']),
            scenario_id=kwargs['scenario_id'],
            attr_id=kwargs.get('attr_id')
        )

        for attr_data in result:
            if 'metadata' in attr_data['dataset']:
                if 'function' in attr_data['dataset']['metadata']:
                    attr_data['dataset']['metadata']['function'] = str(attr_data['dataset']['metadata']['function'])

        return result


def root_connection(url=None):
    return HydraConnection(
        url=url or config.DATA_URL,
        username=config.DATA_ROOT_USERNAME,
        password=config.DATA_ROOT_PASSWORD,
        # key=app.config['SECRET_ENCRYPT_KEY'],
        is_root=True
    )
