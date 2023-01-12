import pyvo

from astropy.table import Table
from astropy.io import votable

from .prem import AccessPoint
from .aws import AWSAccessPoint


__all__ = ['AccessManager', 'DataHandler']
    
    
class AccessManager:
    """AccessPoint container and manager"""
    
    def __init__(self, base_access):
        """Initilize an AccessManager with a basic AccessPoint.
        
        Parameters
        ----------
        base_access: AccessPoint
            a minimum access point with a simple url.
        
        """
        
        if not isinstance(base_access, AccessPoint):
            raise ValueError(
                f'type(base_access) is expected with be '
                f'AccessApoint not {type(base_access)}'
            )
        
        self.access_points = {base_access.type: [base_access]}
        
        # the default is the one to use. One of access_points
        self.default_access_point = {base_access.type: base_access}
        
    
    def add_access_point(self, access_point):
        """Add a new AccessPoint to the manager
        
        Parameters:
        -----------
        access_point: AccessPoint, a subclass, or a list of them.
            the access point to be added to the manager
                
        """
        
        # if a list, loop through the elements
        if isinstance(access_point, list):
            for ap in access_point:
                self.add_access_point(ap)
        else:
            if not isinstance(access_point, AccessPoint):
                raise ValueError(
                    f'type(base_access) is expected with be AccessApoint, '
                    f'a subclass or a list not {type(base_access)}'
                )

            ap_type = access_point.type
            if not ap_type in self.access_points:
                self.access_points[ap_type] = []
            self.access_points[ap_type].append(access_point) 
    
    
class DataHandler:
    """A container for multiple AccessPoint instances"""
    
    def __init__(self, data_product, **kwargs):
        """
        Parameters
        ----------
        data_product: astropy.table or pyvo.dal.DALResults
        
        kwargs: keywrods arguments used to initialize the AccessPoint
            instance or its subclasses.
        
        """
        
        if not isinstance(data_product, (pyvo.dal.DALResults, Table)):
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
        
        # if still None, raise
        # TODO allow the user the pass the name to avoid failing
        if url_colname is None:
            raise ValueError(f'Could not figure out the column with direct access url')
        
        # AccessPoint
        # - self.access_points: some type of ap manager (could be a simple container; e.g. list)
        # - each row has its access_points manager.
        # - If we have a cloud_access column, use it. it is easier than datalinks as it does not
        # require a new call to server.
        # - elif we have datalinks, get all data links in one call, then pass on to AWS ap.
        # - else: no aws info; fall back to on-prem
        
        # minimum access point that uses on-prem data
        self.nrows = len(dal_result)
        self.access_manager = [AccessManager(AccessPoint(url)) for url in dal_result[url_colname]]
        
        
        # if there is a 'cloud_access' json column, give it priority
        # as it does not require a new call to the server
        if 'cloud_access' in dal_result.fieldnames:
            profile = kwargs.get('profile', None)
            
            for irow in range(self.nrows):
                jsontxt = dal_result[irow]['cloud_access']
                awsAp = AWSAccessPoint.from_json(jsontxt, profile=profile)
                self.access_manager[irow].add_access_point(awsAp)
        
        else:
            # check for datalinks
            try:
                dlinks = dal_result.get_adhocservice_by_ivoid(
                    pyvo.dal.adhoc.DATALINK_IVOID
                )
            except pyvo.DALServiceError:
                raise ValueError(
                    'No cloud information available in either '
                    'cloud_access column, or in datalinks'
                )
            # Look for the 'source' <PARAM> element inside the inputParams <GROUP> element.
            # pyvo already handles part of this.
            if not hasattr(dlinks, 'groups'):
                raise ValueError(
                    'Datalinks resource does not have a group '
                    'as required by the standard'
                )
                
            # look for the 'source' param in the datalinks resource tree
            source_elems = [p for p in dlinks.groups[0].entries if p.name == 'source']
            if len(source_elems) == 0:
                raise ValueError(
                    'No <PARAM> named "source" found in the Datalinks resource. '
                    'No access points will be extracted'
                )
                
            # we have a source parameters, process it
            source_elem  = source_elems[0] 
            
            # list the available options in the `source` element:
            access_options = source_elem.values.options
            
            
        from IPython import embed;embed();exit(0)
        base_ap = [AccessPoint(url) for url in dal_result[url_colname]]
        base_ap = AccessPoint()