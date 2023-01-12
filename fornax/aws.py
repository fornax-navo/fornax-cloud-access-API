import boto3
import botocore
import json

from astropy.utils.console import ProgressBarOrSpinner
from pathlib import Path

from .prem import AccessPoint


__all__ = ['AWSAccessPoint']


class AWSAccessPoint(AccessPoint):
    """Handles a single access point on AWS"""
    
    def __init__(self, s3_pointer, profile=None, region=None):
        """An access point for aws
        
        Parameters
        ----------
        s3_pointer : str or a list of str:
            either an s3_uri (s3://..) or a pair of (bucket_name, key) 
        profile : str
            name of the user's profile for credentials in ~/.aws/config
            or ~/.aws/credentials. Use to authenticate the AWS user with boto3.
        region : str
            region of the bucket.
        """
        
        super().__init__(url=None)
        self.type = 'aws'
        
        # check input 
        uri = bucket = key = None
        if isinstance(s3_pointer, str):
            
            if not s3_pointer.startswith('s3://'):
                raise ValueError(f'{s3_pointer} is not a vaild uri')
            
            uri = s3_pointer
            s3_s = uri.split('/')
            bucket, key = s3_s[2], '/'.join(s3_s[3:])
        elif isinstance(s3_pointer, (list, tuple, set)):
            if len(s3_pointer) != 2:
                raise ValueError(f'{s3_pointer} should contain (bucket_name, key)')
            bucket, key = s3_pointer
            uri = f's3://{bucket}/{key}'
            
        
        self.s3_uri = uri
        self.s3_bucket_name = bucket
        self.s3_key = key
        self.region = region
        
        # construct a boto3 s3 resource object
        self.s3_resource = self._build_s3_resource(profile)
    
    @staticmethod
    def from_json(aws_json, profile=None):
        """Construct an AWSAccessPoint instance from the json text in 
        a cloud_access column
        
        Parameters
        ----------
        aws_json: str
            A string of the json text that contains the aws access information
        profile : str
            name of the user's profile for credentials in ~/.aws/config
            or ~/.aws/credentials. Use to authenticate the AWS user with boto3.
            
        Returns:
        a list of AWSAccessPoint instances
        
        """
        
        # read json provided by the archive server
        cloud_access = json.loads(aws_json)
        
        if not 'aws' in cloud_access:
            raise ValueError('There is no aws entry in the json text')
        
            
        # do we have multiple aws access points?
        aws_access = cloud_access['aws']
        if isinstance(aws_access, dict):
            aws_access = [aws_access]
        
        # we should have list. If not, fail
        if not isinstance(aws_access, list):
            raise ValueError(
                f'aws entry can be either a dict or a list of dict. '
                'Found {type(aws_access)}'
            )
            
        aws_access_points = []
        for aws_info in aws_access:
            
            # region and access mode
            bucket = aws_info['bucket_name']
            key    = aws_info['key']
            region = aws_info.get('region', None)
            
            s3_pointer = (bucket, key)
            aws_access_points.append(AWSAccessPoint(s3_pointer, profile=profile, region=region))
            
        return aws_access_points
        
        
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