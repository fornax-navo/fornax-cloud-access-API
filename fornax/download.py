"""
Utilties for downloading data
"""
import threading
import os
from urllib.parse import urlparse, unquote, parse_qs

from posixpath import split

import astropy
from astropy.utils.console import ProgressBarOrSpinner

from pyvo.utils.http import use_session


def _extract_filename(url):
    """Extract file name from uri/url
    handle cases of urls of the form:
    http://somesite.com/service?file=/location/file.jpg&size=large
    http://somesite.com/location/file.jpg
    
    Parameters
    ----------
    url: str
        url or uri
    
    """
    parsed_url = urlparse(url)
    filename = None

    # Check if the file path is included as a query parameter
    query_params = parse_qs(parsed_url.query)
    for param, values in query_params.items():
        if len(values) > 0:
            file_path = unquote(values[0])
            filename = os.path.basename(file_path)
            break

    # If no file name found in query parameters, check the path itself
    if filename is None:
        file_path = parsed_url.path
        if file_path:
            filename = os.path.basename(file_path)

    return filename


# adapted from astroquery._download_file.
def http_download(url,
                  local_filepath=None,
                  cache=False,
                  timeout=None,
                  session=None,
                  verbose=False,
                  **kwargs):
    """Download file from perm url
    
    Parameters
    ----------
    url: str
        The URL of the file to download
    local_filepath: str
        Local path, including filename, where the file is to be downloaded.
    cache : bool
        If True, check if a cached file exists before download
    timeout: int
        Time to attempt download before failing
    session: requests.Session
        Session to use. If None, create a new one.
    verbose: bool
        If True, print progress and debug text
        
    Keywords
    --------
    additional keywords to be passed to session.request()
    
    """

    _session = use_session(session)
    method = 'GET'

    if not local_filepath:
        local_filepath = _extract_filename(url)


    response = _session.request(method, url, timeout=timeout,
                               stream=True, **kwargs)

    response.raise_for_status()
    if 'content-length' in response.headers:
        length = int(response.headers['content-length'])
        if length == 0:
            if verbose:
                print(f'URL {url} has length=0')
    else:
        length = None


    if cache and os.path.exists(local_filepath):
        if length is not None:
            statinfo = os.stat(local_filepath)
            if statinfo.st_size != length:
                if verbose:
                    print(f'Found cached file {local_filepath} with size {statinfo.st_size} '
                          f'that is different from expected size {length}')
            else:
                if verbose:
                    print(f'Found cached file {local_filepath} with the expected size.')
                response.close()
                return
        else:
            if verbose:
                print(f'Found cached file {local_filepath}.')
            response.close()
            return
    else:
        response = _session.request(method, url, timeout=timeout,
                                         stream=True, **kwargs)
        response.raise_for_status()

    blocksize = astropy.utils.data.conf.download_block_size

    n_bytes = 0
    with ProgressBarOrSpinner(length, f'Downloading URL {url} to {local_filepath} ...') as pb:
        with open(local_filepath, 'wb') as f:
            for block in response.iter_content(blocksize):
                f.write(block)
                n_bytes += len(block)
                if length is not None:
                    pb.update(n_bytes if n_bytes <= length else length)
                else:
                    pb.update(n_bytes)
    response.close()


def _s3_is_accessible(s3_resource, bucket_name, key):
    """Do a head_object call to test access

    Paramters
    ---------
    s3_resource : s3.ServiceResource
        the service resource used for s3 connection.
    bucket_name : str
        bucket name.
    key : str
        key to file to test.

    Return
    -----
    (accessible, msg) where accessible is a bool and msg is the failure message

    """

    s3_client = s3_resource.meta.client

    try:
        header_info = s3_client.head_object(Bucket=bucket_name, Key=key)
        accessible, msg = True, ''
    except Exception as e:
        accessible = False
        msg = str(e)

    return accessible, msg


# adapted from astroquery.mast.
def aws_download(uri=None,
                 bucket_name=None,
                 key=None,
                 local_filepath=None,
                 cache=False,
                 timeout=None,
                 aws_profile=None,
                 session=None,
                 verbose=False):
    """Download file from AWS.

    Adapted from astroquery.mast

    Parameters
    ----------
    uri: str
        The URI for s3 location of the form: s3://bucket-name/key.
        If given, bucket_name and key are ignored.
        if None, both bucket_name and key are required.
    bucket_name: str
        Name of the s3 bucket
    key: str
        s3 key to the file.
    local_filepath: str
        Local path, including filename, where the file is to be downloaded.
    cache : bool
        If True, check if a cached file exists before download
    timeout: int
        Time to attempt download before failing
    aws_profile: str
        name of the user's profile for credentials in ~/.aws/config
        or ~/.aws/credentials. Use to authenticate the AWS user with boto3.
    session: boto3.session.Session
        Session to use that include authentication if needed. 
        If None, create an annonymous one. If given, aws_profile is ignored
    verbose: bool
        If True, print progress and debug text

    """
    try:
        import boto3
        import botocore
    except ImportError:
        raise ImportError('aws_download requires boto3. Make sure it is installed first')

    if uri is None and (bucket_name is None and key is None):
        raise ValueError('Either uri or both bucket_name and key must be given')

    if uri:
        parsed = urlparse(uri, allow_fragments=False)
        bucket_name = parsed.netloc
        key = parsed.path[1:]
    if verbose:
        print(f'bucket: {bucket_name}, key: {key}')

    if not local_filepath:
        local_filepath = _extract_filename(f's3://{bucket_name}/{key}')


    if session:
        if not isinstance(session, boto3.session.Session):
            raise ValueError('session has to be instance of boto3.session.Session')
        s3_config = botocore.client.Config(connect_timeout=timeout)
        s3_resource = session.resource(service_name='s3', config=s3_config)
    else:
        if aws_profile is None:
            s3_config = botocore.client.Config(signature_version=botocore.UNSIGNED, connect_timeout=timeout)
            s3_resource = boto3.resource(service_name='s3', config=s3_config)
        else:
            session = boto3.session.Session(profile_name=aws_profile)
            s3_config = botocore.client.Config(connect_timeout=timeout)
            s3_resource = session.resource(service_name='s3', config=s3_config)

    # check access
    accessible, message1 = _s3_is_accessible(s3_resource, bucket_name, key)
    if verbose:
        print(f'Access with profile or annonymous: {accessible}. Message: {message1}')

    # If access with profile fails, attemp to use any credientials
    # in the user system e.g. environment variables etc. boto3 should find them.
    if not accessible:
        s3_resource = boto3.resource(service_name='s3')
        accessible, message2 = _s3_is_accessible(s3_resource, bucket_name, key)
        if verbose:
            print(f'Access with system credentials: {accessible}. Message: {message1}')
        # is still not accessible, fail
        if not accessible:
            raise PermissionError((f'{key} in {bucket_name} is '
                                   f'inaccessible:\n{message1}\n{message2}'))

    # proceed with download
    s3_client = s3_resource.meta.client
    bkt = s3_resource.Bucket(bucket_name)

    # Ask the webserver what the expected content length is and use that.
    info_lookup = s3_client.head_object(Bucket=bucket_name, Key=key)
    length = info_lookup["ContentLength"]

    # if we have cache, use it and return, otherwise download data
    if cache and os.path.exists(local_filepath):

        if length is not None:
            statinfo = os.stat(local_filepath)
            if statinfo.st_size == length:
                # found cached file with expected size. Stop
                if verbose:
                    print(f'Found cached file {local_filepath}.')
                return
            if verbose:
                print(f'Found cached file {local_filepath} with size {statinfo.st_size} '
                      f'that is different from expected size {length}')

    with ProgressBarOrSpinner(length, (f'Downloading {key} to {local_filepath} ...')) as pb:

        # Bytes read tracks how much data has been received so far
        # This variable will be updated in multiple threads below
        global bytes_read
        bytes_read = 0

        progress_lock = threading.Lock()

        def progress_callback(numbytes):
            # Boto3 calls this from multiple threads pulling the data from S3
            global bytes_read

            # This callback can be called in multiple threads
            # Access to updating the console needs to be locked
            with progress_lock:
                bytes_read += numbytes
                pb.update(bytes_read)

        bkt.download_file(key, local_filepath, Callback=progress_callback)