from app.models import User, Message, UserMessages


def get_messages(db, user_id):
    user_messages = UserMessages.query.filter_by(user_id=user_id).all()
    msg_ids = [user_message.message_id for user_message in user_messages]

    # msgs = Message.query.filter(Message.id.in_(msg_ids), Message.is_new==True).all()
    msgs = db.querry(Message).filter(Message.id.in_(msg_ids)).all() if msg_ids else []
    msgs_updated = []
    messages = []
    for msg in msgs:
        messages.append(msg.message)
        msg.is_new = False
        msgs_updated.append(msg)

    db.bulk_save_objects(msgs_updated)
    db.commit()

    return messages


def add_message(db, message):
    msg = Message.query.filter_by(message=message).first()
    if msg is None:  # this should be done via manage.py, not here
        msg = Message(message=message, is_new=True)
        db.add(msg)
        db.commit()
    return msg


def add_user_message(db, user_id, message):
    msg = add_message(db, message)
    user_message = UserMessages(user_id=user_id, message_id=msg.id)
    db.add(user_message)
    db.commit()


def add_message_by_usernames(db, usernames, message):
    msg = add_message(db, message)
    users = db.query(User).filter(User.email.in_(usernames)).all() if usernames else []
    user_messages = []
    for user in users:
        user_message = UserMessages(user_id=user.id, message_id=msg.id)
        user_messages.append(user_message)
    db.bulk_save_objects(user_messages)
    db.commit()
