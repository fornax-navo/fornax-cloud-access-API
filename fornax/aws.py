import boto3
import botocore
import json

from astropy.utils.console import ProgressBarOrSpinner
from pathlib import Path

from .prem import AccessPoint


__all__ = ['AWSAccessPoint']


class AWSAccessPoint(AccessPoint):
    """Handles a single access point on AWS"""
    
    name = 'aws'
    
    def __init__(self, *, 
                 bucket_name = None, 
                 key = None,
                 uri = None,
                 region = None,
                 **kwargs
                ):
        """Define an access point for aws.
        Either uri or both bucket_name/key need to be given
        
        Parameters
        ----------
        bucket_name: str
            name of the s3 bucket
        key: str
            the key or path to the file/directory location
        region : str
            region of the bucket.
            
        Keywords:
        ---------
        meta data needed to download the data, such as authentication profile
        
        aws_profile : str
            name of the user's profile for credentials in ~/.aws/config
            or ~/.aws/credentials. Use to authenticate the AWS user with boto3.
            When authenticating in aws, either aws_profile, or a s3_resource is needed
        
        """
        
        
        # check input 
        if uri is None and (bucket_name is None or key is None):
            raise ValueError('either uri or both bucket_name and key are required')
        
        if uri is None:
            assert(isinstance(bucket_name, str))
            assert(isinstance(key, str))

            if key[0] == '/':
                key = key[1:]
            
            uri = f's3://{bucket_name}/{key}'
        else:
            if not uri.startswith('s3://'):
                raise ValueError(
                    f'uri needs to be of the form "s3://bucket/key". '
                    f'found: {uri}'
                )
            # TODO: handle case of region in the uri
            uris = uri.split('/')
            bucket_name = uris[2]
            key = '/'.join(uris[3:])
    
        self.s3_uri = uri
        self.s3_bucket_name = bucket_name
        self.s3_key = key
        self.region = region
        self.id = uri
        self._accessible = None
        
        # prepare the s3 resource
        profile = kwargs.get('aws_profile', None)
        self.s3_resource = self._build_s3_resource(profile)
    
    
    def __repr__(self):
        return f'|{self.name.ljust(5)}| {self.s3_uri}'
        
        
    def _build_s3_resource(self, profile):
        """Construct a boto3 s3 resource
        
        Parameters:
        profile: str
            name of the user's profile for credentials in ~/.aws/config
            or ~/.aws/credentials. Use to authenticate the AWS user with boto3.
        
        """
        
        
        # if profile is give, use it.
        if profile is not None:
            session = boto3.session.Session(profile_name=profile)
            s3_resource = session.resource(service_name='s3')
        else:
            # access anonymously
            config = botocore.client.Config(signature_version=botocore.UNSIGNED)
            s3_resource = boto3.resource(service_name='s3', config=config)
        
        return s3_resource
    
    
    
    
    def is_accessible(self):
        """Check if the aws endpoint is accessible
        
        Do a head_object call to test access

        Return
        ------
        (accessible, msg) where accessible is a bool and msg is the failure message
        
        
        """
        if self._accessible is None:
        
            s3_client = self.s3_resource.meta.client
            try:
                header_info = s3_client.head_object(Bucket=self.s3_bucket_name, Key=self.s3_key)
                accessible, msg = True, ''
            except Exception as e:
                accessible = False
                msg = str(e)
            self._accessible = (accessible, msg)
                
        return self._accessible

    
    # adapted from astroquery.mast.
    def download(self, cache=True):
        """
        downloads the product used in inializing this object into
        the given directory.
        
        
        Parameters
        ----------
        cache : bool
            Default is True. If file is found on disc it will not be downloaded again.
            
        """
        
        s3 = self.s3_resource
        s3_client = s3.meta.client

        key = self.s3_key
        bucket_name = self.s3_bucket_name
        
        bkt = s3.Bucket(bucket_name)
        if not key:
            raise Exception(f"Unable to locate file {key}.")

        local_path = Path(key).name

        # Ask the webserver (in this case S3) what the expected content length is and use that.
        info_lookup = s3_client.head_object(Bucket=bucket_name, Key=key)
        length = info_lookup["ContentLength"]

        if cache and os.path.exists(local_path):
            if length is not None:
                statinfo = os.stat(local_path)
                if statinfo.st_size != length:
                    log.info(f"Found cached file {local_path} with size {statinfo.st_size} "
                             f"that is different from expected size {length}.")
                else:
                    log.info(f"Found cached file {local_path} with expected size {statinfo.st_size}.")
                    return

        with ProgressBarOrSpinner(length, (f'Downloading {self.s3_uri} to {local_path} ...')) as pb:

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

            bkt.download_file(key, local_path, Callback=progress_callback)
        return local_path
    
    
    @staticmethod
    def access_meta(**kwargs):
        """Extract access metadata from the user-supplied list of keywords.
        The result is to be passed to download and is_accessible methods
        
        Returns:
        meta: dict
            a dictionary of access meta data, such authentication
            that will be passed to the class initializer
            
        aws_profile : str
            name of the user's profile for credentials in ~/.aws/config
            or ~/.aws/credentials. Use to authenticate the AWS user with boto3.
        """
        
        profile = kwargs.get('aws_profile', None)
        
        return dict(aws_profile=profile)