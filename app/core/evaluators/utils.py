import pandas
import json
from calendar import isleap
from datetime import datetime

EMPTY_VALUES = {
    'timeseries': {},
    'periodic timeseries': {},
    'scalar': None,
    'array': None,
    'descriptor': None
}


def make_timesteps(data_type='timeseries', **kwargs):
    # TODO: Make this more advanced

    span = kwargs.get('span') or kwargs.get('timestep') or kwargs.get('time_step')
    start = kwargs.get('start') or kwargs.get('start_time')
    end = kwargs.get('end') or kwargs.get('end_time')

    format = kwargs.get('format', 'native')

    dates = []

    if start and end and span:

        if type(start) in [int, float]:
            start = datetime.fromordinal(round(start))
        if type(end) in [int, float]:
            end = datetime.fromordinal(round(end))

        start_date = pandas.to_datetime(start)
        end_date = pandas.to_datetime(end)
        span = span.lower()

        if data_type == 'periodic timeseries':
            start_date = datetime(1678, 1, 1)
            end_date = datetime(1678, 12, 31, 23, 59)

        if span == 'day':
            dates = pandas.date_range(start=start, end=end, freq='D')
        elif span == 'week':
            dates = []
            for i in range(52 * (end_date.year - start_date.year)):
                if i == 0:
                    date = start_date
                else:
                    date = dates[-1] + pandas.DateOffset(days=7)
                if isleap(date.year) and date.month == 3 and date.day == 4:
                    date += pandas.DateOffset(days=1)
                if date.month == 12 and date.day == 31:
                    date += pandas.DateOffset(days=1)
                dates.append(date)
        elif span == 'month':
            dates = pandas.date_range(start=start, end=end, freq='M')
        elif span == 'thricemonthly':
            dates = []
            for date in pandas.date_range(start=start, end=end, freq='M'):
                d1 = pandas.datetime(date.year, date.month, 10)
                d2 = pandas.datetime(date.year, date.month, 20)
                d3 = pandas.datetime(date.year, date.month, date.daysinmonth)
                dates.extend([d1, d2, d3])

    if format == 'iso':
        dates = [d.isoformat() for d in dates]

    return dates


def make_default_value(data_type='timeseries', dates=None, nblocks=1, default_value=0, flavor='json',
                       date_format='iso', time_step=None):
    try:
        if data_type == 'timeseries':
            default_eval_value = empty_data_timeseries(dates, nblocks=nblocks, flavor=flavor, date_format=date_format,
                                                       default_value=default_value)
        elif data_type == 'periodic timeseries':
            start_date = dates[0]
            first_dates = [d for d in dates if (d - start_date).days < 365]
            if time_step == 'month':
                periodic_dates = [d.replace(year=1678) if d.day != 29 else d.replace(year=1678, day=28) for d in
                                  first_dates]
            else:
                feb29 = (2, 29)
                periodic_dates = [d.replace(year=1678) for d in dates if
                                  d in first_dates and (d.month, d.day) != feb29]
            periodic_dates_iso = sorted([d.isoformat() for d in periodic_dates])
            default_eval_value = empty_data_timeseries(periodic_dates_iso, nblocks=nblocks, default_value=default_value)
            default_eval_value = default_eval_value.replace('1678', '9999')
        elif data_type == 'array':
            default_eval_value = '[[],[]]'
        else:
            default_eval_value = ''
        return default_eval_value
    except:
        raise


def empty_data_timeseries(dates, nblocks=1, flavor='json', date_format='iso', default_value=None):
    try:
        timeseries = None
        values = [default_value] * len(dates)
        if flavor == 'json':
            vals = {str(b): values for b in range(nblocks or 1)}
            if date_format == 'iso':
                timeseries = pandas.DataFrame(vals, index=dates).to_json(date_format='iso')
            elif date_format == 'original':
                timeseries = pandas.DataFrame(vals, index=dates)
        elif flavor == 'native':
            vals = {b: values for b in range(nblocks)}
            timeseries = pandas.DataFrame(vals, index=dates).to_dict()
        elif flavor == 'pandas':
            dates = pandas.to_datetime(dates)
            timeseries = pandas.DataFrame([[v] * nblocks for v in values], columns=range(nblocks), index=dates)
            timeseries.index.name = 'date'
        return timeseries
    except:
        raise


def eval_scalar(x):
    try:  # create the function
        if type(x) == str and len(x):
            x = float(x)
        else:
            x = None
    except ValueError as err:  # value error
        # err_class = err.__class__.__name__
        # detail = err.args[0]
        returncode = -1
        errormsg = "\"{}\" is not a number".format(x)
        raise Exception(errormsg)

    return x


def eval_descriptor(s):
    return s


def eval_timeseries(timeseries, dates, fill_value=None, fill_method=None, flatten=False, has_blocks=False, flavor=None,
                    date_format='%Y-%m-%d %H:%M:%S'):
    try:

        df = pandas.read_json(timeseries)
        if df.empty:
            df = pandas.DataFrame(index=dates, columns=['0'])
        else:
            # TODO: determine if the following reindexing is needed; it's unclear why it was added
            # this doesn't work with periodic timeseries, as Pandas doesn't like the year 9999
            # df = df.reindex(pandas.DatetimeIndex(dates))
            if fill_value is not None:
                df.fillna(value=fill_value, inplace=True)
            elif fill_method:
                df.fillna(method=fill_method)

        result = None
        if flatten:
            df = df.sum(axis=1)

        if flavor == 'pandas':
            result = df
        elif flavor == 'native':
            df.index = df.index.strftime(date_format=date_format)
            result = df.to_dict()
        elif flavor == 'json':
            result = df.to_json(date_format='iso')
        else:
            result = df.to_json(date_format='iso')

    except:

        returncode = -1
        errormsg = 'Error parsing timeseries data'
        raise Exception(errormsg)

    return result


def eval_array(array, flavor=None):
    result = None
    try:
        array_as_list = json.loads(array)
        if flavor is None:
            result = array
        elif flavor == 'native':
            result = array_as_list
        elif flavor == 'pandas':
            result = pandas.DataFrame(array_as_list)
        return result
    except:
        errormsg = 'Something is wrong.'
        raise Exception(errormsg)
