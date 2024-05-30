from app.core.users import get_datauser
from app.core.messages import add_message_by_usernames

from app.models import User


def share_resource(db, hydra, user_id, resource_class, resource_id, emails, permissions, message=None, url=None):
    datauser = get_datauser(db, url=hydra.url, user_id=user_id)
    valid_emails = []
    invalids = []
    for email in emails:
        user = db.query(User).filter_by(email=email).first()
        if user:
            valid_emails.append(email)
        else:
            invalids.append(email)

    if valid_emails:
        read_only = permissions.get('edit') == 'N'
        share = permissions.get('share', 'N')

        result = hydra.call('share_{}'.format(resource_class), resource_id, valid_emails, read_only, share)
        if result and 'faultstring' in result:
            error = 2
            result = result['faultstring']
        else:
            error = 0
            item = hydra.call('get_{}'.format(resource_class), resource_id)
            if message:
                default_message = '{} has shared {} {} with you.'.format(
                    datauser.username,
                    resource_id,
                    item['name']
                )
                message = default_message + '\n\n' + default_message

            add_message_by_usernames(valid_emails, message)
            result = None
    else:
        error = 1
        result = 'No valid emails'

    results = {'error': error, 'result': result, 'valids': valid_emails, 'invalids': invalids}

    return results


def set_resource_permissions(hydra, item_class, item_id, usernames, permissions):
    read = permissions['view']
    write = permissions['edit']
    share = permissions['share']

    if type(usernames) == str:
        usernames = [usernames]

    hydra.call('set_{}_permission'.format(item_class), item_id, usernames, read, write, share)

    return
