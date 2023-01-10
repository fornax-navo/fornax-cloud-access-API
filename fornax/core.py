import requests
import boto3
import botocore
import Path
import pyvo

from astropy.utils.data import download_file
from astropy.utils.console import ProgressBarOrSpinner
from astropy.table import Table
from astropy.io import votable


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
        
    
    def download(self, cache=True):
        """Download data. Can be overloaded with different implimentation
        
        Parameters
        ----------
        cache : bool
            Default is True. If file is found on disc it will not be downloaded again.
            
            
        Return
        ------
        local_path : str
            Returns the local path that the file was download to.
            
        """
        
        if self.url is None:
            raise ValueError(f'No on-prem url has been defined.')
        
        log.info(f'downloading data from {self.type} using: {self.url}')
        
        path = download_file(self.url, cache=cache)
        return path
    

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
    
    
    
class DataHandler:
    """A container for multiple AccessPoint instances"""
    
    def __init__(self, data_product):
        """
        Parameters
        ----------
        data_product: astropy.table or pyvo.dal.DALResults
        
        """
        
        if not isinstance(dal_result, (pyvo.dal.DALResults, Table)):
            raise ValueError(f'data_prodcut should be either '
                              'astropy.table.Table or '
                              'pyvo.dal.DALResults')
        
        # if we have an astropy table, convert to a pyvo.dal.DALResults
        if isinstance(data_product, Table):
            vot = votable.from_table(data_product)
            dal_result = pyvo.dal.DALResults(vot)  
        else:
            dal_result = data_product
        
        
        ## column name with direct access url 
        # SIA v1
        url_colname = dal_result.fieldname_with_ucd('VOX:Image_AccessReference')
        if url_colname is None:
            # SIA v2
            if 'access_url' in dal_result.fieldnames:
                url_colname = 'access_url'
            else:
                # try by ucd as a final resort
                url_colname = dal_result.fieldname_with_ucd('meta.ref.url')
        7y
        # if still None, raise
        # TODO allow the user the pass the name to avoid failing
        if url_colname is None:
            raise ValueError(f'Could not figure out the column with direct access url')
        
        # base AccessPoint
        base_ap = AccessPoint()