import requests
from astropy.utils.data import download_file


__all__ = ['AccessPoint']


class AccessPoint:
    """A base class to handle a single data access point"""
    
    name = 'prem'
    
    def __init__(self, url):
        """An access point for on-prem server with direct url
        
        Parameters
        ----------
        url : the url to access the data
        
        """
        
        self.url  = url
        self.id   = url
        self._accessible = None
    
    
    def __repr__(self):
        return f'|{self.name.ljust(5)}| {self.url}'
    
    
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
        
        log.info(f'downloading data from {self.name} using: {self.url}')
        
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