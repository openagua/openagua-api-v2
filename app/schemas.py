import datetime as dt

from typing import List
from pydantic import BaseModel, ConfigDict, HttpUrl, EmailStr


# security

class User(BaseModel):
    id: int
    username: str | None = None
    email: str
    password: str = ''
    remember: bool = False


class DataUser(BaseModel):
    id: int
    username: str


# Hydra schemas

class Project(BaseModel):
    id: int = -1
    name: str
    description: str = ''
    layout: str = '{}'
    status: str = 'A'
    cr_date: str = dt.datetime.now().isoformat()
    created_by: int | None = None
    is_public: bool = False
    user: DataUser | None = None
    networks: list = []


class Network(BaseModel):
    id: int | None = None
    name: str
    description: str
    layout: object
    status: str = 'A'
    cr_date: str | None = None
    projection: str | None = None
    created_by: int | None = None
    is_public: bool = False
    project: Project | None = None


class ResourceGroupItem(BaseModel):
    ref_key: str
    ref_id: int
    group_id: int


class Template(BaseModel):
    id: int
    parent_id: int | None = None
    name: str
    status: str = 'A'
    description: str | None = None
    created_by: int | None = None
    project_id: int | None = None
    cr_date: str | None = None
    layout: object = {}
    is_public: bool = False
    templatetypes: list


class Scenario(BaseModel):
    id: int
    name: str
    description: str = ''
    layout: str = '{}'
    status: str = 'A'
    network_id: int
    start_time: str
    end_time: str
    locked: str = 'N'
    time_step: str
    cr_date: str | None = None
    created_by: int | None = None
    parent_id: int | None = None
    # network: Network | None = None
    # parent = Scenario | None = None


class ResourceScenarioData(BaseModel):
    action: str
    resource_type: str
    resource_id: int
    data_type: str = 'timeseries'  # TODO: make this not optional
    scenario_data: dict
    attr_id: int
    attr_is_var: str
    res_attr_id: int
    unit_id: int | None = None
    variation: str | None = None
    time_settings: str | None = None
    language: str | None = None
    flavor: str | None = None
    network_folder: str


class Dimension(BaseModel):
    id: int
    name: str
    description: str
    project_id: int


class Unit(BaseModel):
    id: int
    dimension_id: int
    name: str
    abbreviation: str
    lf: str
    cf: str
    description: str
    project_id: int
    dimension: Dimension
    project: Project


class Favorite(BaseModel):
    id: int
    study_id: int
    network_id: int
    name: str
    description: str = ''
    provider: str
    type: str
    filters: dict
    pivot: dict
    analytics: dict
    content: dict


class Card(BaseModel):
    id: int
    title: str
    description: str = ''
    type: str
    content: str = ''
    layout: str = '{}'
    favorite_id: int | None = None
    favorite: Favorite | None = None


class Dashboard(BaseModel):
    id: int
    title: str
    description: str
    layout: object
    cards: List[Card]


class Run(BaseModel):
    model_config = ConfigDict(str_max_length=10)
    model_config['protected_namespaces'] = ()
    sid: str
    model_id: int
    layout: str


class Database(BaseModel):
    userid: int


class HydraCall(BaseModel):
    args: list
    kwargs: dict
