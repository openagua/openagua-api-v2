def kwargs_to_cli(model_kwargs, extra_args=None, join=False):
    args_list = []
    for k, v in model_kwargs.items():
        args_list.extend(['--' + k, str(v)])

    args_list.extend(extra_args.split(' '))

    if join:
        args_list = ' '.join(args_list)

    return args_list
