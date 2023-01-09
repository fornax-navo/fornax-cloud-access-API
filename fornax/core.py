import requests
import boto3
import botocore


__all__ = ['AccessPoint', 'AWSAccessPoint']



class AccessPoint:
    """A base class to handle a single data access point"""
    
    def __init__(self, url):
        """An access point for on-prem server with direct url
        
        Parameters
        ----------
        url : the url to access the data
        
        """
        
        self.url  = url
        self.type = 'prem'
        self._accessible = None
        
    
    def download(self):
        """Download data. Can be overloaded with different implimentation"""
        
        if self.url is None:
            raise ValueError(f'No on-prem url has been defined.')
        
        log.info(f'downloading data from {self.type} using: {self.url}')
        
        return download_file(self.url)
    

    def is_accessible(self):
        """Check if the url is accessible
        
        Return
        ------
        (accessible, msg) where accessible is a bool and msg is the failure message
        
        """
        msg = ''
        if self._accessible is None:
            response = requests.head(self.url)
            accessible = response.status_code == 200
            if not accessible:
                msg = response.reason
            self._accessible = (accessible, msg)
        return self._accessible
    

    
class AWSAccessPoint(AccessPoint):
    """Handles a single access point on AWS""""
    
    def __init__(self, s3_pointer, url=None, profile=None):
        """An access point for aws
        
        Parameters
        ----------
        s3_pointer: str or a list of str:
            either an s3_uri (s3://..) or a pair of (bucket_name, key) 
        url : str
            the url of the on-prem data to be used as a fallback.
        profile : str
            name of the user's profile for credentials in ~/.aws/config
            or ~/.aws/credentials. Use to authenticate the AWS user with boto3.
        
        """
        
        super().__init__(url)
        self.type = 'aws'
        
        # check input 
        uri = bucket = key = None
        if isinstance(s3_pointer, str):
            
            if not s3_pointer.startswith('s3://'):
                raise ValueError(f'{s3_pointer} is not a vaild uri')
            
            uri = s3_pointer
            s3_s = uri.split('/')
            bucket, key = s3_s[2], '/'.join(s3_s[3:])
        elif isintance(s3_pointer, (list, tuple, set)):
            if len(s3_pointer) != 2:
                raise ValueError(f'{s3_pointer} should contain (bucket_name, key)')
            bucket, key = s3_pointer
            uri = f's3://{bucket}/{key}'
            
        
        self.s3_uri = uri
        self.s3_bucket_name = bucket
        self.s3_key = key
        
        # construct a boto3 s3 resource object
        self.s3_resource = self._build_s3_resource(profile)
        
        
    def _build_s3_resource(profile):
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
            
            s3_client = s3_resource.meta.client
            try:
                header_info = s3_client.head_object(Bucket=self.s3_bucket_name, Key=self.s3_key)
                accessible, msg = True, ''
            except Exception as e:
                self._accessible = False
                msg = str(e)
            self._accessible = (accessible, msg)
                
        return self._accessible
