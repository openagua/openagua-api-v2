from app import config

import requests

vhosts = {
    # 'model-run': {"description": "For model run queues", "tags": "production"},
    'flask-socketio': {"description": "For socket-io", "tags": "production"}
}


class API(object):
    def __init__(self, url, path, auth):
        self.auth = auth
        self.path = url + path

    def get(self, **urlparts):
        return requests.get(self.path.format(**urlparts), auth=self.auth)

    def put(self, json, **urlparts):
        return requests.put(self.path.format(**urlparts), auth=self.auth, json=json)

    def delete(self, **urlparts):
        return requests.delete(self.path.format(**urlparts), auth=self.auth)


class RabbitMQ(object):

    def __init__(self, api_url=None, host=None, username=None, password=None):
        if not api_url and host:
            api_url = 'http://{host}:15672/api'.format(host=host)
        auth = (username, password)

        # self.vhosts = ['model-run', 'flask-socketio']
        self.VHosts = API(url=api_url, path='/vhosts/{vhost}', auth=auth)
        self.Users = API(url=api_url, path='/users/{user}', auth=auth)
        self.Permissions = API(url=api_url, path='/permissions/{vhost}/{user}', auth=auth)
        self.Queues = API(url=api_url, path='/queues/{vhost}/{name}', auth=auth)

        # set up vhosts
        for vhost in vhosts:
            self.add_vhost(vhost, vhosts[vhost])

    def add_vhost(self, vhost, vhost_kwargs):
        resp = self.VHosts.put(vhost_kwargs, vhost=vhost)
        return resp

    def delete_vhost(self, vhost):
        resp = self.VHosts.delete(vhost=vhost)
        return resp

    def add_user(self, user, password='password', tags=''):
        user_kwargs = {'password': password, 'tags': tags}
        resp = self.Users.put(user_kwargs, user=user)
        return resp

    def delete_user(self, user):
        self.Users.delete(user=user)

    def update_user(self, vhost, user):
        resp = self.add_user(user)
        resp = self.Permissions.put({"configure": ".*", "write": ".*", "read": ".*"}, vhost=vhost, user=user)

        return

    def get_queue(self, vhost, queue_name):
        return self.Queues.get(vhost=vhost, name=queue_name)

    def add_queue(self, json, queue_name):
        return self.Queues.put(json, name=queue_name)


# RabbitMQ for adding model users management
api_url = 'http://{hostname}:15672/api'.format(
    hostname=config.RABBITMQ_HOST,
)
rabbitmq = RabbitMQ(
    api_url=api_url,
    username=config.get('RABBITMQ_DEFAULT_USERNAME'),
    password=config.get('RABBITMQ_DEFAULT_PASSWORD')
)
