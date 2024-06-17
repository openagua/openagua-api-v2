import json

from sqlalchemy_json import mutable_json_type

from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, ForeignKey, UniqueConstraint, JSON
from sqlalchemy.orm import relationship, backref
from app.database import Base


class User(Base):
    __tablename__ = 'user'

    id = Column(Integer, primary_key=True)
    email = Column(String(255), unique=True)
    username = Column(String(31), unique=True)
    password = Column(String(255))
    active = Column(Boolean)
    confirmed_at = Column(DateTime)
    new_user = Column(Boolean, server_default='1')

    # other info
    firstname = Column(String(50))
    lastname = Column(String(50))
    organization = Column(String(50))
    socketid = Column(String(50))

    settings = Column(Text)

    # relationships
    roles = relationship('Role', secondary='user_roles', backref=backref('users', lazy='dynamic'))
    messages = relationship('Message', secondary='user_messages', backref=backref('users', lazy='dynamic'))

    def __init__(self, **kwargs):
        self.email = kwargs['email']
        self.username = kwargs.get('username')
        self.active = kwargs.get('active', True)
        self.password = kwargs.get('password')

    def get(self, setting):
        settings = json.loads(self.settings if self.settings else '{}')
        return settings.get(setting)

    def get_settings(self):
        return json.loads(self.settings if self.settings else '{}')

    def to_json(self, include_id=False):
        user = {
            'username': self.username,
            'email': self.email,
        }
        if include_id:
            user['id'] = self.id

        return user


class Role(Base):
    __tablename__ = 'role'
    id = Column(Integer, primary_key=True)
    name = Column(String(80), unique=True)
    description = Column(String(255))


class UserRoles(Base):
    __tablename__ = 'user_roles'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('user.id', ondelete='CASCADE'))
    role_id = Column(Integer, ForeignKey('role.id', ondelete='CASCADE'))


class APIKey(Base):
    __tablename__ = 'apikey'
    id = Column(String(255), primary_key=True, unique=True)
    user_id = Column(Integer, ForeignKey('user.id', ondelete='CASCADE'))


class DataUser(Base):
    __tablename__ = 'datauser'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('user.id', ondelete='CASCADE'))
    dataurl_id = Column(Integer, ForeignKey('dataurl.id', ondelete='CASCADE'))
    userid = Column(Integer)
    username = Column(String(50))
    password = Column(String(255))  # Note: this is for 3rd party databases, not the main connected database.
    sessionid = Column(String(255))
    settings = Column(Text)

    def get_setting(self, setting):
        s = json.loads(self.settings or "{}")
        return s.get(setting)


class DataUrl(Base):
    __tablename__ = 'dataurl'
    id = Column(Integer, primary_key=True)
    url = Column(String(255), unique=True)

    def to_json(self):
        return dict(
            id=self.id,
            url=self.url
        )


class Study(Base):
    __tablename__ = 'study'
    id = Column(Integer, primary_key=True)
    created_by = Column(Integer, ForeignKey('user.id', ondelete='CASCADE'))
    dataurl_id = Column(Integer, ForeignKey('dataurl.id', ondelete='CASCADE'))
    project_id = Column(Integer)
    settings = Column(Text)
    secrets = Column(Text)  # this should be encrypted
    layout = Column(Text)

    # relationships
    favorites = relationship('Favorite')
    inputsetups = relationship('InputSetup')
    dashboards = relationship('Dashboard', secondary='study_dashboards', lazy='dynamic')

    def get(self, setting):
        settings = json.loads(self.settings if self.settings else '{}')
        return settings.get(setting)


# User-saved objects

class Favorite(Base):
    __tablename__ = 'favorite'
    id = Column(Integer, primary_key=True)
    study_id = Column(Integer, ForeignKey('study.id', ondelete='CASCADE'))
    network_id = Column(Integer)
    name = Column(String(80))
    description = Column(String(255), server_default='')
    provider = Column(String(16))
    type = Column(String(16))
    filters = Column(JSON)
    pivot = Column(JSON)
    analytics = Column(JSON)
    content = Column(JSON)

    def to_json(self):
        j = {}
        for c in self.__table__.columns:
            j[c.name] = getattr(self, c.name)
            if c.name in ['content', 'filters', 'pivot', 'analytics']:
                j[c.name] = j.get(c.name) or {}

        return j


class Message(Base):
    __tablename__ = 'message'
    id = Column(Integer, primary_key=True)
    message = Column(String(512))
    is_new = Column(Boolean)


class UserMessages(Base):
    __tablename__ = 'user_messages'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('user.id', ondelete='CASCADE'))
    message_id = Column(Integer, ForeignKey('message.id', ondelete='CASCADE'))


class Run(Base):
    __tablename__ = 'run'
    sid = Column(String(255), primary_key=True)
    model_id = Column(Integer)
    layout = Column(Text)

    def get_layout(self):
        return json.loads(self.layout)


class Ping(Base):
    __tablename__ = 'ping'
    sid = Column(String(255), ForeignKey('user.id', ondelete='CASCADE'), primary_key=True)
    status = Column(String(10), primary_key=True)
    source_id = Column(Integer, ForeignKey('dataurl.id'))
    name = Column(String(100))
    network_id = Column(Integer)
    # start_time = Column(DateTime)
    # end_time = Column(DateTime)
    last_ping = Column(Integer)
    extra_info = Column(Text)

    def to_json(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class Model(Base):
    __tablename__ = 'model'
    """Models available for simulation and/or optimization."""
    id = Column(Integer, primary_key=True)
    service = Column(String(32))
    name = Column(String(80))
    description = Column(String(255))
    user_id = Column(Integer, ForeignKey('user.id', ondelete='CASCADE'))
    study_id = Column(Integer, ForeignKey('study.id', ondelete='CASCADE'))
    scope = Column(String(32))  # private (only available to user/project) or public (available to anybody)
    image_id = Column(String(32))
    executable = Column(String(128))
    key = Column(String(32))
    init_script = Column(Text)  # bash script to run on newly-launched machine

    templates = relationship('ModelTemplate')

    networks = relationship('NetworkModel')

    def to_json(self, include_templates=False, include_network_ids=False):
        ret = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        if include_templates:
            ret['templates'] = [t.to_json() for t in self.templates]
        ret['network_ids'] = []
        if include_network_ids:
            for network in self.networks:
                ret['network_ids'].append(network.network_id)
        return ret


class NetworkModel(Base):
    __tablename__ = 'network_model'
    """Models available for a study/network"""
    model_id = Column(Integer, ForeignKey('model.id', ondelete='CASCADE'), primary_key=True)
    dataurl_id = Column(Integer, ForeignKey('dataurl.id', ondelete='CASCADE'), primary_key=True)
    network_id = Column(Integer, primary_key=True)
    active = Column(Boolean)
    settings = Column(Text)  # settings for the model

    UniqueConstraint('model_id', 'dataurl_id', 'network_id')

    # model = relationship('Model')

    def get(self, setting):
        settings = json.loads(self.settings if self.settings else '{}')
        return settings.get(setting)

    def to_json(self):
        ret = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        return ret


class UserNetworkSettings(Base):
    __tablename__ = 'user_network_settings'
    """Models available for a study/network"""
    user_id = Column(Integer, ForeignKey('user.id', ondelete='CASCADE'))
    dataurl_id = Column(Integer, ForeignKey('dataurl.id', ondelete='CASCADE'), primary_key=True)
    network_id = Column('network_id', Integer, primary_key=True)
    settings = Column(mutable_json_type(dbtype=JSON, nested=True))  # settings for the network

    UniqueConstraint('user_id', 'dataurl_id', 'network_id')

    def __init__(self, **kwargs):
        self.user_id = kwargs.get('user_id')
        self.dataurl_id = kwargs.get('dataurl_id')
        self.network_id = kwargs.get('network_id')
        self.settings = kwargs.get('settings', {})

    def get(self, setting):
        settings = self.settings or {}
        return settings.get(setting)

    def to_json(self):
        ret = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        return ret


class ModelTemplate(Base):
    __tablename__ = 'model_template'
    """Dependancy relationships between models and templates."""
    model_id = Column(Integer, ForeignKey('model.id', ondelete='CASCADE'), primary_key=True)
    dataurl_id = Column(Integer, ForeignKey('dataurl.id', ondelete='CASCADE'), primary_key=True)
    template_id = Column(Integer, primary_key=True)
    template_name = Column(Text)  # this should be checked before the ID

    def to_json(self, include_models=False):
        ret = {c.name: getattr(self, c.name) for c in self.__table__.columns}

        return ret


class InputSetup(Base):
    __tablename__ = 'input_setup'
    id = Column(Integer, primary_key=True)
    study_id = Column(Integer, ForeignKey('study.id', ondelete='CASCADE'))
    name = Column(String(80), nullable=False)
    description = Column(String(255), server_default='')
    filters = Column(Text, nullable=False)
    setup = Column(Text, nullable=False)


class Card(Base):
    __tablename__ = 'card'
    id = Column(Integer, primary_key=True)
    title = Column(String(31))
    description = Column(String(255))
    type = Column(String(31))
    content = Column(Text)
    layout = Column(Text)
    favorite_id = Column(Integer, ForeignKey('favorite.id', ondelete='CASCADE'))

    favorite = relationship('Favorite', backref=backref('card', lazy='dynamic'))

    def to_json(self):
        return dict(
            id=self.id,
            title=self.title,
            description=self.description,
            type=self.type,
            content=json.loads(self.content),
            layout=json.loads(self.layout),
            favorite_id=self.favorite_id,
            favorite=self.favorite.to_json if self.favorite_id and self.favorite else None
        )


class Dashboard(Base):
    __tablename__ = 'dashboard'
    id = Column(Integer, primary_key=True)
    title = Column(String(80))
    description = Column(String(255))
    layout = Column(Text)

    # relationships
    cards = relationship('Card', single_parent=True, cascade="all, delete-orphan", secondary='dashboard_cards')

    def to_json(self):
        return dict(
            id=self.id,
            title=self.title,
            description=self.description,
            layout=json.loads(self.layout or '{}'),
            cards=[card.to_json for card in self.cards]
        )


class DashboardCards(Base):
    __tablename__ = 'dashboard_cards'
    # id = Column(Integer, primary_key=True)
    dashboard_id = Column(Integer, ForeignKey('dashboard.id', ondelete='CASCADE'), primary_key=True)
    card_id = Column(Integer, ForeignKey('card.id', ondelete='CASCADE'), primary_key=True)

    UniqueConstraint('dashboard_id', 'card_id')


class StudyDashboards(Base):
    __tablename__ = 'study_dashboards'
    id = Column(Integer, primary_key=True)
    study_id = Column(Integer, ForeignKey('study.id', ondelete='CASCADE'))
    network_id = Column(Integer)
    dashboard_id = Column(Integer, ForeignKey('dashboard.id', ondelete='CASCADE'))


class Star(Base):
    __tablename__ = 'star'
    user_id = Column(Integer, ForeignKey('user.id', ondelete='CASCADE'), primary_key=True)
    study_id = Column(Integer, ForeignKey('study.id', ondelete='CASCADE'), primary_key=True)

    study = relationship('Study')
