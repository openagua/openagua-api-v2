import hashlib
from os import environ
import json
import sys
import traceback
import pandas
import numpy

from .utils import make_timesteps, make_default_value, EMPTY_VALUES, \
    eval_array, eval_descriptor, eval_scalar, eval_timeseries


def parse_function(user_code, name, argnames, modules=()):
    '''Parse a function into usable Python'''

    # first, parse code
    spaces = '\n    '
    s = user_code.rstrip()
    lines = s.split('\n')
    if 'return ' not in lines[-1]:
        lines[-1] = 'return ' + lines[-1]
    code = spaces.join(lines)

    try:
        eval1 = eval(user_code)
        eval2 = eval(user_code)
        if user_code and eval1 == eval2:
            return '''def {name}(self):{spaces}{code}'''.format(spaces=spaces, code=code, name=name)
    except:
        pass

    # modules
    # modules = spaces.join('import {}'.format(m) for m in modules if m in user_code)

    # getargs (these pass to self.GET)
    kwargs = spaces.join(['{arg} = kwargs.get("{arg}")'.format(arg=arg) for arg in argnames if arg in user_code])

    # final function
    func = '''def {name}(self, **kwargs):{spaces}{spaces}{kwargs}{spaces}{code}''' \
        .format(spaces=spaces, kwargs=kwargs, code=code, name=name)

    return func


class Timestep(object):
    index = -1
    periodic_timestep = 1

    def __init__(self, date, start_date, span):
        if date == start_date:
            type(self).index = 0
            type(self).periodic_timestep = 1
        else:
            type(self).index += 1
        self.index = type(self).index
        self.timestep = self.index + 1
        self.date = date
        self.year = date.year
        self.month = date.month
        self.day = date.day
        self.date_as_string = date.isoformat(' ')

        if start_date:
            if date.month < start_date.month:
                self.water_year = date.year
            else:
                self.water_year = date.year + 1

        if span:
            self.span = span
            self.set_periodic_timestep(date, start_date, span)

    def set_periodic_timestep(self, date, start_date, span):

        if span == 'day':
            if (date.month, date.day) == (start_date.month, start_date.day):
                type(self).periodic_timestep = 1
            else:
                type(self).periodic_timestep += 1
            self.periodic_timestep = type(self).periodic_timestep

        elif span == 'week':
            self.periodic_timestep = self.index % 52 + 1

        elif span == 'month':
            self.periodic_timestep = self.index % 12 + 1

        elif span == 'thricemonthly':
            self.periodic_timestep = self.index % 36 + 1


class InnerSyntaxError(SyntaxError):
    """Exception for syntax errors that will be defined only where the SyntaxError is made.

    Attributes:
        expression -- input expression in which the error occurred
        message    -- explanation of the error
    """

    def __init__(self, expression, message):
        self.expression = expression
        self.message = message


class EvalException(Exception):
    def __init__(self, message, code):
        self.message = message
        self.code = code


class Evaluator(object):

    def __init__(self, hydra=None, scenario_id=None, time_settings=None, data_type='timeseries', nblocks=1,
                 files_path=None, date_format='%Y-%m-%d %H:%M:%S', **kwargs):
        self.hydra = hydra

        self.dates = []
        self.dates_as_string = []
        self.timesteps = []
        self.start_date = None
        self.end_date = None

        if data_type in [None, 'timeseries', 'periodic timeseries']:
            span = kwargs.get('span') or kwargs.get('timestep') or kwargs.get('time_step')
            dates = make_timesteps(data_type=data_type, **time_settings)
            self.timesteps = [Timestep(d, dates[0], span) for d in dates]
            self.dates = [t.date for t in self.timesteps]
            self.dates_as_string = [t.date_as_string for t in self.timesteps]
            self.start_date = self.dates[0].date
            self.end_date = self.dates[-1].date

        self.date_format = date_format
        self.tsi = None
        self.tsf = None
        self.scenario_id = scenario_id
        self.data_type = data_type
        self.default_timeseries = None
        self.default_array = make_default_value('array')
        self.resource_scenarios = {}
        self.external = {}

        self.bucket = environ.get('AWS_S3_BUCKET')
        self.files_path = files_path

    def eval_function(self, code_string):
        # return code_string
        return None

    def eval_data(self, dataset, func=None, flavor=None, flatten=False, fill_value=None,
                  date_format=None, has_blocks=False, data_type=None):
        """
        Evaluate the data and return the appropriate value
        """

        result = None
        date_format = date_format or self.date_format

        try:

            metadata = dataset['metadata']
            if type(metadata) == bytes:
                metadata = metadata.decode()
            if type(metadata) == str:
                metadata = json.loads(metadata)

            input_method = metadata.get('input_method', 'native')

            data_type = data_type or dataset['type']

            if input_method == 'function' or metadata.get('use_function') == 'Y':
                f = metadata.get('function', '')
                result = self.eval_function(f)

            elif data_type == 'scalar':
                try:
                    result = eval_scalar(dataset['value'])
                except:
                    raise

            elif data_type == 'descriptor':
                try:
                    result = eval_descriptor(dataset['value'])
                except:
                    raise

            elif data_type in ['timeseries', 'periodic timeseries']:
                try:
                    result = eval_timeseries(
                        dataset['value'],
                        self.dates_as_string,
                        has_blocks=has_blocks,
                        flatten=(flatten if flatten is not None else not has_blocks),
                        date_format=date_format,
                        fill_value=fill_value,
                        flavor=flavor,
                    )
                except:
                    raise

            elif data_type == 'array':
                try:
                    result = eval_array(
                        dataset['value'],
                        flavor=flavor
                    )
                except:
                    raise

            return result

        except:
            raise
