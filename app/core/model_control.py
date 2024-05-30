from os import getenv, environ as env
import json
from datetime import datetime

from app.core.runners import run_model_rabbitmq, run_model_local #, run_model_ec2
from app.models import Ping, Run
from app.core.utils import get_utc
from app.core.modeling import get_model, get_network_model


class ProcessState:
    REQUESTED = 'requested'
    STARTED = 'started'
    RUNNING = 'running'
    PAUSED = 'paused'
    CANCELED = 'stopped'
    ERROR = 'error'
    FINISHED = 'finished'


def get_all_data_scenarios(hydra, network_id):
    data_scenarios = hydra.call('get_scenarios', {'network_id': network_id})
    all_scenarios = {
        'option': [],
        'portfolio': [],
        'scenario': [],
        'scenario_group': []
    }
    for data_scenario in data_scenarios:
        if data_scenario['name'] == env['DEFAULT_SCENARIO_NAME']:
            all_scenarios['option'].append(data_scenario)
            all_scenarios['scenario'].append(data_scenario)
        elif 'class' in data_scenario['layout'] and data_scenario['layout']['class'] in all_scenarios:
            all_scenarios[data_scenario['layout']['class']].append(data_scenario)

    return all_scenarios


def add_run(db, sid, model_id, layout=None):
    run = Run(sid=sid, model_id=model_id, layout=json.dumps(layout))
    db.add(run)
    db.commit()
    return


def add_ping(db, sid, status, **data):
    canceled_ping = get_ping(db, sid, status=ProcessState.CANCELED)
    if canceled_ping:
        return canceled_ping  # nothing more to add

    # add ping info
    ping = Ping(sid=sid, status=status)
    for kwarg in data:
        try:
            setattr(ping, kwarg, data[kwarg])
        except:
            continue
    ping.last_ping = get_utc()
    db.merge(ping)
    db.commit()
    return ping


def get_ping(db, sid, status=None, last=True):
    if status:
        return db.query(Ping).filter_by(sid=sid, status=status).first()
    elif last:
        return db.query(Ping).filter_by(sid=sid).first()
    else:
        return db.query(Ping).filter_by(sid=sid).last()


def get_last_ping(db, sid):
    return db.query(Ping).filter_by(sid=sid).last()


def get_pings(db, source_id=None, network_id=None):
    return db.query(Ping).filter_by(source_id=source_id, network_id=network_id).all()


def start_model_run(db, hydra, username, host_url, network_id, guid, config, scenarios, computer_id=None, mq=None):
    # 1. get user input

    default_run_name = 'network-{}'.format(network_id)
    # computer_location = request.json.get('location', 'local')

    # get configuration options
    run_name = config.get('name', default_run_name)
    run_key = config.pop('key', None)
    extra_args = config.get('extra_args', '')
    scenario_ids = scenarios

    # foresight = 'zero'  # get from user or network settings

    # 2. get network, model, and template information

    # model
    network_model = get_network_model(url=hydra.url, network_id=network_id)
    model = get_model(db, id=network_model.model_id)
    model_name = model.name.replace(' ', '_')

    service = model.service or "amqp"

    # 3. Create new hydra scenarios: cartesian product of selected scenarios

    # 4. define arguments

    request_host = host_url if 'localhost' in host_url else host_url.replace('http://', 'https://')

    model_kwargs = dict(
        name=model_name,
        request_host=request_host,
        username=username,
        source_id=hydra.id,
        network_id=network_id,
        scenario_ids=scenario_ids,
        run_id=config.get('id'),
        run_name=run_name,
        guid=guid,
        start_time=datetime.now().isoformat(),
        pubnub_publish_key=getenv('PUBNUB_PUBLISH_KEY'),
        pubnub_subscribe_key=getenv('PUBNUB_SUBSCRIBE_KEY')
    )

    # save unique identifiers to send back so we can track progress.
    # these should match what is in the GUI
    sids = ['-'.join([guid] + [str(i) for i in ids]) for ids in scenario_ids]

    # 4. record request
    # run_secret = uuid.uuid4().hex
    for i, scids in enumerate(scenario_ids):
        source_id = hydra.id
        sid = sids[i]
        add_run(db, sid=sid, model_id=model.id, layout={'run_key': run_key})

        ping_data = dict(
            name=run_name,
            source_id=source_id,
            network_id=network_id,
            extra_info=json.dumps(model_kwargs)
        )
        add_ping(db, sid, ProcessState.REQUESTED, **ping_data)

        ping = {
            'action': 'request',
            'sid': sid,
            'name': run_name,
            'source_id': source_id,
            'network_id': network_id,
            'scids': scids,
            'status': ProcessState.REQUESTED,
            'progress': 0
        }
        emit_progress(source_id=source_id, network_id=network_id, ping=ping)

    error = None

    if service == 'amqp':
        run_model_rabbitmq(mq, model, run_key, model_kwargs)

    elif service == 'local':
        run_model_local(model, model_kwargs, extra_args=extra_args)

    elif service == 'aws':
        run_model_ec2(model, model_kwargs, computer_id, extra_args=extra_args)

    if error:
        # for sid in sids:
        for i, scids in enumerate(scenario_ids):
            sid = sids[i]
            source_id = hydra.id
            add_ping(db, sid=sid, status=ProcessState.ERROR, source_id=source_id, network_id=network_id,
                     extra_info=error)
            ping = {
                'action': 'fail',
                'sid': sid,
                'name': run_name,
                'status': ProcessState.ERROR,
                'scids': scids,
                'progress': 0,
                'network_id': network_id,
                'source_id': source_id,
                'extra_info': "Request failed. Please check main model parameters."
            }
            emit_progress(source_id=source_id, network_id=network_id, ping=ping)

    return error


def publish_callback(result, status):
    """Placeholder"""
    pass
    # Handle PNPublishResult and PNStatus


def publish_model_run_state(db, pubnub, sid, state):
    message = {
        'state': state,
        'sid': sid,
    }

    # model
    run = get_run(db, sid=sid)
    layout = run.get_layout()
    model = get_model(db, id=run.model_id)

    queue_name = 'model-{model_key}-{sid}'.format(model_key=model.key, sid=sid)
    # run_key = layout.get('run_key')
    # if run_key:
    #     queue_name += '-{}'.format(run_key)

    pubnub.publish().channel(queue_name).message(message).pn_async(publish_callback)
    # print(' [*] Stopping model at namespace {}'.format(queue_name))
    # socketio.emit('stop-model', message, namespace='/{}'.format(queue_name))

    return


def get_run(db, sid):
    return db.query(Run).filter_by(sid=sid).first()


def pause_model_run(db, pubnub, sid):
    publish_model_run_state(db, pubnub, sid, ProcessState.PAUSED)


def cancel_model_run(db, pubnub, sid):
    publish_model_run_state(db, pubnub, sid, ProcessState.CANCELED)


def get_run_records(db, source_id=None, network_id=None):
    rows = get_pings(db, source_id=source_id, network_id=network_id)
    sids = [row.sid for row in rows]
    recs = db.query(Ping).filter(Ping.sid.in_(sids)).all() if sids else []
    records = {r.sid: {} for r in recs}
    for rec in recs:
        records[rec.sid][rec.status] = rec
    ret = []
    for sid, rec in records.items():
        ret_rec = {}

        try:
            end_info = rec.extra_info if rec.extra_info != '0' else ''
        except:
            end_info = ''

        requested = rec.get('requested')
        if requested:
            parts = requested.extra_info.split(' --')
            exc = '\n'.join(parts[0].split(' '))
            start_params = exc + '\n--' + '\n--'.join(parts[1:])
            ret_rec['name'] = requested.name
        else:
            start_params = ''

        statuses = rec.keys()
        status = [s for s in statuses if s not in ['requested', 'started']]
        end_info = ''
        if status:
            status = status[0]
            last = rec[status]
            end_info = last.extra_info
        else:
            status = 'started' if rec.get('started') else 'requested'
            last = None

        ret_rec.update(
            id=sid,
            status=status,
            start_time=rec.get('requested') and rec['requested'].last_ping,
            end_time=last.last_ping if last else None,
            start_params=start_params,
            end_info=end_info
        )

        ret.append(ret_rec)

    return ret


def delete_run_record(db, sid):
    db.query(Ping).filter_by(sid=sid).delete()
    db.commit()


def delete_run_records(db, source_id=None, network_id=None):
    rows = get_pings(db, source_id=source_id, network_id=network_id)
    sids = [row.sid for row in rows]
    for sid in sids:
        db.query(Ping).filter_by(sid=sid).delete()
    db.commit()


def end_model_run(db, sid, status, data, report_to_browser=False):
    data.pop('sid', None)
    data.pop('status', None)
    ping = add_ping(db, sid, status, **data)
    if report_to_browser:
        ping = ping.to_json()
        for key in data:
            if key not in ping:
                ping[key] = data[key]
        emit_progress(source_id=ping.get('source_id'), network_id=ping.get('network_id'), ping=ping)


def emit_progress(source_id=None, network_id=None, ping=None):
    if network_id:
        room = config['NETWORK_ROOM_NAME'].format(source_id=source_id, network_id=network_id)
        event = 'network-run'
    else:
        room = config['RUN_STUDY_ROOM_NAME'].format(source_id)
        event = 'update-study-progress'

    socketio.emit(event, ping, room=room)
