from .utils import kwargs_to_cli


def run_model_local(model, model_kwargs, extra_args=''):
    from subprocess import Popen

    args_list = kwargs_to_cli(model_kwargs, extra_args=extra_args)
    args = [model.executable] + args_list
    Popen(args)
