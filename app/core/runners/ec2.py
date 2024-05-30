from os import path
import boto3
from .utils import kwargs_to_cli


class EC2:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region='us-west-2', id=None):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.resource = boto3.resource(
            service_name='ec2',
            region_name=region,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )
        # self.connection = boto3.connection()

    def get_instance(self, instance_id):
        self.instance = self.resource.Instance(instance_id)

    def launch(self, ami_name, tags, instance_type, min_count, max_count, key_name, security_group_ids):
        filter = {'Name': 'name', 'Values': [ami_name]}
        amis = [ami for ami in self.resource.images.filter(Filters=[filter])]
        if amis:
            ami = amis[0]  # there should be only one
            # see: http://boto3.readthedocs.io/en/latest/reference/services/ec2.html?highlight=images#EC2.ServiceResource.create_instances
            instances = self.resource.create_instances(
                ImageId=ami.id,
                MinCount=min_count,
                MaxCount=max_count,
                InstanceType=instance_type,
                KeyName=key_name,
                SecurityGroupIds=security_group_ids
            )
            Tags = []
            for key, value in tags.items():
                Tags.append({'Key': key, 'Value': str(value)})
            for instance in instances:
                instance.create_tags(Tags=Tags)
            return instances  # for now, there should be only one
        else:
            return None

    # def start(self):
    #     response = self.ec2.launch(self.id)
    #     return response

    def stop(self):
        response = self.ec2.instances.filter(InstanceIds=[self.id]).stop()
        return response

    def terminate(self):
        response = self.ec2.instances.filter(InstanceIds=[self.id]).terminate()
        return response

    def reboot(self):
        response = self.ec2.instances.filter(InstanceIds=[self.id]).reboot()
        return response

    def get_instance_metadata(self, instances):
        metadata = []
        for i in instances:
            meta = {
                'location': 'cloud',
                'id': i.id,
                'state': i.state['Name'],
                'status': None,
                'launch_time': i.launch_time.strftime('%Y-%m-%d %H:%M:%S'),
                'type': i.instance_type
            }

            statuses = self.resource.meta.client.describe_instance_status(InstanceIds=[i.id])['InstanceStatuses']
            if len(statuses):
                meta['status'] = statuses[0]['InstanceStatus']

            metadata.append(meta)

        return metadata


def add_alarm(cloudwatch, action, instance_id, region, account, hours=1):
    # Create alarm with actions enabled
    cloudwatch.put_metric_alarm(
        AlarmName='OpenAgua-{}-CPU-Idle'.format(instance_id),
        ComparisonOperator='LessThanThreshold',
        EvaluationPeriods=2,
        MetricName='CPUUtilization',
        Namespace='AWS/EC2',
        Period=3600 * hours,
        Statistic='Maximum',
        Threshold=1.0,
        Unit='Percent',
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:swf:{region}:{account}:action/actions/AWS_EC2.InstanceId.{action}/1.0'
                .format(region=region, account=account, action=action.title())
        ],
        AlarmDescription='Stop when maximum server CPU falls below 1% (i.e., the computer has not been used at all).',
        Dimensions=[
            {
                'Name': 'InstanceId',
                'Value': instance_id
            }
        ],
    )


def get_available_computers(filters, config):
    ec2client = boto3.client('ec2', region_name=config['AWS_DEFAULT_REGION'])
    reservations = ec2client.describe_instances(
        Filters=filters
    )
    reservations = reservations['Reservations']
    instance_ids = [r['Instances'][0]['InstanceId'] for r in reservations]
    statuses = ec2client.describe_instance_status(InstanceIds=instance_ids)
    statuses = {s['InstanceId']: s for s in statuses['InstanceStatuses']}
    computers = get_reservations_data(reservations, statuses)

    return computers


def get_reservations_data(reservations, statuses):
    reservations_data = []
    for r in reservations:
        instance = r['Instances'][0]
        status = statuses.get(instance['InstanceId'])
        status = status and status['InstanceStatus']['Status']
        instance_data = get_instance_data(instance, status)
        reservations_data.append(instance_data)
    return reservations_data


def get_instance_data(instance, status):
    i = instance
    return {
        'id': i['InstanceId'],
        'type': i['InstanceType'],
        'state': i['State']['Name'],
        'status': status,
        'location': 'aws',
        'tags': {tag['Key']: tag['Value'] for tag in i['Tags']},
        'launchTime': i['LaunchTime'].isoformat()
    }


def launch_instance(computer_type, image_id, init_script, tags, config, auto_terminate=True):
    region = config['AWS_DEFAULT_REGION']

    ec2client = boto3.client('ec2', region_name=region)

    reservation = ec2client.run_instances(
        ImageId=image_id,
        InstanceType=computer_type,
        MinCount=1,
        MaxCount=1,
        KeyName=config['AWS_MODEL_KEY_NAME'],
        SecurityGroupIds=[config['AWS_SSH_SECURITY_GROUP']],
        UserData=init_script.replace("\r", ""),
        TagSpecifications=[{'ResourceType': 'instance', 'Tags': tags}]
        # should create a custom security group to ssh only from this ip address
    )

    instances = reservation['Instances']
    computers = []
    for i in instances:
        computers.append(get_instance_data(i, 'initializing'))

    if auto_terminate:

        # Create CloudWatch client
        cloudwatch = boto3.client('cloudwatch', region_name=region)
        account = config['AWS_ACCOUNT_ID']
        hours = config['AUTOTERMINATE_HOURS']
        for i in instances:
            add_alarm(cloudwatch, 'stop', i['InstanceId'], region, account, hours=hours)

    return computers


def run_model_ec2(model, model_kwargs, computer_id, extra_args=''):
    import paramiko

    args_list = kwargs_to_cli(model_kwargs, extra_args=extra_args, join=True)

    command = '{executable} {args}'.format(executable=model.executable, args=args_list)

    # a. connect to ec2 to get ip address
    ec2 = EC2(
        app.config.get('AWS_ACCESS_KEY_ID'),
        app.config.get('AWS_SECRET_ACCESS_KEY'),
        region=app.config.get('AWS_DEFAULT_REGION')
    )
    instance = ec2.resource.Instance(computer_id)

    # b. connect to ec2 via ssh
    key_name = instance.key_name
    key_file = path.join(app.config.get('KEYS_DIR'), key_name + '.pem')
    private_key = paramiko.RSAKey.from_private_key_file(key_file)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=instance.public_ip_address,
        username=app.config['AWS_MODEL_EC2_USERNAME'],
        pkey=private_key
    )

    # c. run command
    stdin, stdout, stderr = client.exec_command(command)
    stdout = stdout.read().decode().strip()
    stderr = stderr.read().decode().strip()
    data = {'stdout': stdout, 'stderr': stderr}
