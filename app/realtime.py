from flask import current_app
from flask_socketio import join_room


def init_socketio():
    from openagua import socketio

    @socketio.on('join-network')
    def _join_network(data):
        source_id = data.get('source_id')
        project_id = data.get('project_id')
        network_id = data.get('network_id')
        # study = get_study(project_id=project_id, dataurl_id=source_id)
        room = current_app.config['NETWORK_ROOM_NAME'].format(source_id=source_id, network_id=network_id)
        join_room(room)

# if install_type == 'development':
#     socketio = SocketIO(app, cors_allowed_origins=allowed_origin)
# else:
#     socketio_url = 'pyamqp://{username}:{password}@{hostname}/flask-socketio'.format(
#         username=app.config['RABBITMQ_DEFAULT_USERNAME'],
#         password=app.config['RABBITMQ_DEFAULT_PASSWORD'],
#         hostname=app.config.get('RABBITMQ_HOST', 'localhost'),
#         # vhost=app.config.get('RABBITMQ_VHOST'),
#     )
#     # print(' [*] Connected to {}'.format(socketio_url.replace(app.config['RABBITMQ_DEFAULT_PASSWORD'], '********')))
#     socketio = SocketIO(app, async_mode='gevent', message_queue=socketio_url, cors_allowed_origins=allowed_origin)
#
#
# # this starts the socketio listener functions
# init_socketio()
