import os
import string
import boto3
from botocore.client import Config

from multiprocessing import Pool
from threading import Thread


def add_storage(network, location, force=False):
    # should be moved to site-level

    if 'layout' not in network:
        network['layout'] = {}

    if 'storage' not in network['layout'] or force:
        import random

        n = 12
        folder = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(n))

        network['layout']['storage'] = {
            'location': location,
            'folder': folder
        }
    else:
        network['layout']['storage']['location'] = location

    return network


def s3_resource():
    host = os.environ.get('AWS_S3_HOST')
    port = os.environ.get('AWS_S3_PORT', 9000)
    if host:
        s3 = boto3.resource(
            service_name='s3',
            endpoint_url='{}:{}'.format(host, port)
        )
    else:
        s3 = boto3.resource(service_name='s3')
    return s3


def s3_bucket(bucket_name, s3=None):
    if s3:
        return s3.Bucket(bucket_name)
    else:
        return s3_resource().Bucket(bucket_name)


def object_url(obj):
    endpoint_url = 'https://{}.s3.amazonaws.com/{}'.format(
        obj.bucket_name,
        obj.key
    )
    return endpoint_url


def s3_object_summary(path, key):
    return s3_resource().ObjectSummary(path, key)


def s3_object(path, key):
    return s3_resource().Object(path, key)


def add_to_s3(bucket_name, key, content_type='text/plain', body=None, acl='public-read', return_url=True, s3=None):
    bucket = s3_bucket(bucket_name, s3=s3)
    obj = None
    url = None
    if body:
        obj = bucket.put_object(Body=body, Key=key, ACL=acl, ContentType=content_type)
        # obj.Acl().put(ACL='public-read')
    elif key[-1] == '/':
        obj = bucket.put_object(Key=key, ACL=acl)
    if obj and return_url:
        url = object_url(obj)
    return url


def get_from_s3(bucket, key, data):
    try:
        bucket.download_fileobj(key, data)
        return data
    except:
        return None


def delete_from_s3(bucket, key):
    try:
        object = bucket.Object(key)
        object.delete()
    except:
        pass

    return


def add_folder(bucket_name, prefix, s3=None):
    key = add_folder_to_s3(bucket_name, prefix, s3=s3)
    return key


def get_file_list(bucket_name, prefix, s3=None):
    # for a better version, see: https://alexwlchan.net/2018/01/listing-s3-keys-redux/
    if prefix[:-1] != '/':
        prefix += '/'
    bucket = s3_bucket(bucket_name, s3=s3)
    files = []
    folders = []
    objects = bucket.meta.client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    for obj in objects.get('Contents', []):
        if obj['Key'] == prefix:
            continue
        files.append(obj)
    for obj in objects.get('CommonPrefixes', []):
        folders.append(obj.get('Prefix'))

    return folders, files


def upload_network_data(network, bucket_name, filename, text=None, body=None, s3=None):
    storage = network['layout'].get('storage', {})
    location = storage.get('location')
    folder = storage.get('folder')
    key = os.path.join(folder, filename).replace('\\', '/')

    if 's3' in location.lower():
        if text:
            body = text.encode()
        url = add_to_s3(bucket_name, key, body=body, s3=s3)
    else:
        url = None

    return url


def upload_data(bucket_name, filename, text=None, body=None, s3=None):
    key = filename
    if text:
        body = text.encode()
    url = add_to_s3(bucket_name, key, body=body, s3=s3)
    return url


def bulk_upload_data(network, bucket_name, data, mode='threading', s3=None):
    storage = network['layout'].get('storage', {})
    location = storage.get('location')
    folder = storage.get('folder')

    if 's3' in location.lower():

        bucket = s3_bucket(bucket_name, s3=s3)

        # See http://ls.pwd.io/2013/06/parallel-s3-uploads-using-boto-and-threads-in-python/
        def upload(filename):
            key = os.path.join(folder, filename).replace('\\', '/')
            body = data[filename]
            if body:
                bucket.put_object(Body=body, Key=key, ACL='private')
            elif key[-1] == '/':
                bucket.put_object(Key=key, ACL='private')

        filenames = data.keys()
        if mode == 'sync':
            for filename in filenames:
                upload(filename)
        if mode == 'threading':
            for filename in filenames:
                t = Thread(target=upload, args=(filename,)).start()
        elif mode == 'multiprocessing':
            with Pool() as pool:
                pool.apply_async(upload, filenames)

    return


def delete_all_network_files(network, bucket_name, s3=None):
    storage = network['layout'].get('storage', {})
    if not storage:
        return
    location = storage.get('location')
    folder = storage.get('folder')
    key = '/' + folder

    if 's3' in location.lower():
        bucket = s3_bucket(bucket_name, s3=s3)
        delete_from_s3(bucket, key)
    else:
        pass

    return


def copy_object(client, bucket_name, old_key, new_key, object=None):
    """Copy an item in S3"""
    result = client.copy_object(
        CopySource={"Bucket": bucket_name, "Key": old_key},
        Bucket=bucket_name,
        Key=new_key
    )

    if object:
        object.update({
            "key": new_key,
            "LastModified": result["CopyObjectResult"]["LastModified"]
        })

    return object


def duplicate_objects(bucket_name, objects, network_key, s3):
    bucket = s3_bucket(bucket_name, s3=s3)

    for object in objects:
        new_key = object.get('new_key', '')
        if len(network_key) < 12 or network_key != new_key[:len(network_key)]:
            raise Exception("Network key must be provided")

    new_files = []
    new_folders = []

    old_files = [obj for obj in objects if not obj.get('isDir') or obj['old_key'][-1] != '/']
    old_folders = [obj for obj in objects if obj.get('isDir') or obj['old_key'][-1] == '/']

    def copy_file(object):
        old_key = object.get('old_key', '')
        new_key = object.get('new_key', '')
        new_object = copy_object(bucket.meta.client, bucket_name, old_key, new_key, object=object)
        new_files.append(new_object)

    def copy_folder(object):
        old_key = object.get('old_key', '')
        new_key = object.get('new_key', '')
        threads = []
        for obj in bucket.objects.filter(Prefix=old_key):
            if obj.key == old_key:
                new_folders.append(new_key)
            process = Thread(target=copy_object,
                             args=[bucket.meta.client, bucket_name, obj.key, obj.key.replace(old_key, new_key)])
            process.start()
            threads.append(process)
        for process in threads:
            process.join()

    # new_files = [copy_file(file) for file in old_files]
    threads = []
    for old_file in old_files:
        process = Thread(target=copy_file, args=[old_file])
        process.start()
        threads.append(process)
    for process in threads:
        process.join()

    for folder in old_folders:
        copy_folder(folder)

    return new_files, new_folders


def rename_object(bucket_name, old_key, new_key, s3=None):
    bucket = s3_bucket(bucket_name, s3=s3)
    delete_keys = {'Objects': []}
    return_object = None

    if old_key[-1] != '/':
        # rename the file object
        new_object = bucket.meta.client.copy_object(
            CopySource={"Bucket": bucket_name, "Key": old_key},
            Bucket=bucket_name,
            Key=new_key
        )

        return_object = new_object['CopyObjectResult']
        delete_keys['Objects'].append({'Key': old_key})

    else:
        # rename objects within the folder
        for object in bucket.objects.filter(Prefix=old_key):
            src_key = object.key
            bucket.meta.client.copy_object(
                CopySource={"Bucket": bucket_name, "Key": src_key},
                Bucket=bucket_name,
                Key=src_key.replace(old_key, new_key)
            )
            delete_keys['Objects'].append({'Key': src_key})

    bucket.meta.client.delete_objects(Bucket=bucket_name, Delete=delete_keys)

    return return_object


def duplicate_folder(old_bucket_name, new_bucket_name, old_folder, new_folder, prefix=None):
    if prefix:
        old_folder += '/' + prefix
        new_folder += '/' + prefix
    old_bucket = s3_bucket(old_bucket_name)
    new_bucket = s3_bucket(new_bucket_name)
    for obj in old_bucket.objects.filter(Prefix=old_folder):
        old_source = {'Bucket': old_bucket_name,
                      'Key': obj.key}
        # replace the prefix
        new_key = obj.key.replace(old_folder, new_folder)
        new_obj = new_bucket.Object(new_key)
        new_obj.copy(old_source)


def delete_folder(bucket, folder):
    bucket.objects.filter(Prefix=folder).delete()


def delete_objects(bucket_name, files, folders, s3=None):
    bucket = s3_bucket(bucket_name, s3=s3)

    # delete files
    if files:
        delete_keys = {'Objects': [{'Key': k} for k in files]}
        bucket.meta.client.delete_objects(Bucket=bucket_name, Delete=delete_keys)

    # see: https://stackoverflow.com/questions/11426560/amazon-s3-boto-how-to-delete-folder
    if folders:
        threads = []
        for folder in folders:
            process = Thread(target=delete_folder, args=[bucket, folder])
            process.start()
            threads.append(process)
        for process in threads:
            process.join()


def generate_presigned_post(region, bucket_name, file_name, file_type, dest=None):
    # see: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3.html#generating-presigned-posts
    s3client = boto3.client('s3', region_name=region, config=Config(signature_version='s3v4'))
    acl = 'public-read' if dest == 'images' else 'private'
    presigned_post = s3client.generate_presigned_post(
        Bucket=bucket_name,
        Key=file_name,
        Fields={
            "acl": acl,
            "Content-Type": file_type,
        },
        Conditions=[
            {"acl": acl},
            {"Content-Type": file_type},
        ],
        ExpiresIn=3600
    )

    return presigned_post

    # return json.dumps({
    #     'data': presigned_post,
    #     'url': 'https://%s.s3.amazonaws.com/%s' % (bucket_name, file_name)
    # })


def s3_presigned_url(bucket_name, key_name):
    client = boto3.client(
        service_name='s3',
        config=Config(signature_version='s3v4')
    )
    url = client.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': bucket_name,
            'Key': key_name
        }
    )
    return url


def generate_presigned_urls(bucket_name, keys, client_method):
    s3client = s3_resource().meta.client
    return [generate_presigned_url(bucket_name, key, client_method=client_method, s3client=s3client) for key in keys]


def generate_presigned_url(bucket_name, key, client_method='get_object', s3client=None):
    if s3client is None:
        s3client = s3_resource().meta.client

    return s3client.generate_presigned_url(
        ClientMethod=client_method,
        Params={
            'Bucket': bucket_name,
            'Key': key
        }
    )


def add_folder_to_s3(bucket_name, key, acl='public-read', s3=None):
    bucket = s3_bucket(bucket_name, s3=s3)
    obj = bucket.put_object(Key=key, ACL=acl)
    return obj.key
