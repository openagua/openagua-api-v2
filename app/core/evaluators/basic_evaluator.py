from os import environ
import json

from .utils import make_timesteps, make_default_value, EMPTY_VALUES, \
    eval_array, eval_descriptor, eval_scalar, eval_timeseries


class Evaluator:
    def __init__(self, hydra=None, scenario_id=None, time_settings=None, data_type='timeseries', nblocks=1,
                 files_path=None, date_format='%Y-%m-%d %H:%M:%S', **kwargs):
        self.hydra = hydra

        self.dates = []
        self.dates_as_string = []
        self.start_date = None
        self.end_date = None

        if data_type in [None, 'timeseries', 'periodic timeseries']:
            dates = make_timesteps(data_type=data_type, **time_settings)
            self.dates = dates
            self.dates_as_string = [d.isoformat(' ') for d in dates]
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

    def eval_data(self, dataset, func=None, flavor=None, depth=0, flatten=False, fill_value=None,
                  tsidx=None, date_format=None, has_blocks=False, data_type=None, parentkey=None, for_eval=False):
        """
        Evaluate the data and return the appropriate value

        :param dataset:
        :param func:
        :param do_eval:
        :param flavor:
        :param depth:
        :param flatten:
        :param fill_value:
        :param date_format:
        :param has_blocks:
        :param data_type:
        :param parentkey:
        :return:
        """

        result = None
        date_format = date_format or self.date_format

        try:

            # metadata = json.loads(resource_scenario.value.metadata)
            metadata = json.loads(dataset['metadata'])
            if func is None:
                func = metadata.get('function')
            use_function = metadata.get('use_function', 'N') == 'Y'
            data_type = data_type or dataset['type']

            if use_function:
                result = func

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
