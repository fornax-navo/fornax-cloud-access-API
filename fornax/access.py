import pyvo
import json

from astropy.table import Table, unique
from astropy.io import votable

from .prem import AccessPoint
from .aws import AWSAccessPoint


__all__ = ['AccessManager', 'DataHandler']


ACCESS_POINTS = [
    AccessPoint,
    AWSAccessPoint
]
class_mapper = {ap.name: ap for ap in ACCESS_POINTS}
    
    
class AccessManager:
    """AccessPoint container and manager"""
    
    def __init__(self, access_point):
        """Initilize an AccessManager with a some AccessPoint.
        
        Parameters
        ----------
        access_point: AccessPoint or a subclass
            a minimum access point with a simple url.
        
        """
        
        if not isinstance(access_point, AccessPoint):
            raise ValueError(
                f'type(base_access) is expected with be '
                f'AccessApoint not {type(base_access)}'
            )
        
        self.access_points = {access_point.name: [access_point]}
        
        # the default is the one to use. One of access_points
        self.default_access_point = {access_point.name: access_point}
    
    
    def __repr__(self):
        summary = ', '.join([f'{k}:{len(g)}' for k,g in self.access_points.items()])
        return f'<Access: {summary}>'
    
    
    @property
    def ids(self):
        """Return a list of current access id's """
        return [ap.id for aplist in self.access_points.values() for ap in aplist]
    
    
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

            ap_name = access_point.name
            if not ap_name in self.access_points:
                self.access_points[ap_name] = []
            if not access_point.id in self.ids:
                self.access_points[ap_name].append(access_point)
    
    
    def summary(self):
        """Print a summary of the access points"""
        
        text = ''
        for name,apoints in self.access_points.items():
            text += '\n'
            text += '\n'.join([str(ap) for ap in apoints])
        print(text)
    
    
class DataHandler:
    """A container for multiple AccessPoint instances"""
    
    def __init__(self, 
                 data_product, 
                 source='prem', 
                 fallback=True, 
                 url_column=None,
                 **kwargs
                ):
        """
        Parameters
        ----------
        data_product: astropy.table or pyvo.dal.DALResults
            The data to be accessed or downloaded
        source: str
            The source of the data. prem | aws
        fallback: bool
            Fallback to prem if other source fail
        url_column: str or None
            Name of the column that contains the direct url.
            If None, attempt to figure it out following VO standards
        
        kwargs: keywrods arguments used to initialize the AccessPoint
            instance or its subclasses.
        
        """
        
        if not isinstance(data_product, (pyvo.dal.DALResults, Table)):
            raise ValueError(f'data_prodcut should be either '
                              'astropy.table.Table or '
                              'pyvo.dal.DALResults')
            
        if source not in class_mapper.keys():
            raise ValueError(f'Expected prem or aws for source. Found {source}')
        
        # if we have an astropy table, convert to a pyvo.dal.DALResults
        if isinstance(data_product, Table):
            vot = votable.from_table(data_product)
            dal_product = pyvo.dal.DALResults(vot)  
        else:
            dal_product = data_product
        
        # if url_column is not a column, fail
        if url_column is not None and url_column not in dal_product.fieldnames:
            raise ValueError(f'No column named {url_column} in the data product')
        
        ## column name with direct access url 
        if url_column is None:
            # SIA v1
            url_column = dal_product.fieldname_with_ucd('VOX:Image_AccessReference')
            if url_column is None:
                # SIA v2
                if 'access_url' in dal_product.fieldnames:
                    url_column = 'access_url'
                else:
                    # try by ucd as a final attempt
                    url_column = dal_product.fieldname_with_ucd('meta.ref.url')
        
        # base prem access point
        access_manager = [AccessManager(AccessPoint(url)) for url in dal_product[url_column]]
        self.access_manager = access_manager
        
        ## ------------------- ##
        ## other access points ##
        ## ------------------- ##
        
        # process the json column if it exists
        self.process_json_column(dal_product)
        
        # process datalinks if they exist
        self.process_datalinks(dal_product)
    
    
    def __getitem__(self, item):
        """Enable access to the access_manager list directly"""
        return self.access_manager[item]
        
      
    def process_json_column(self, dal_product, colname='cloud_access'):
        """Process the json text in the access column
        
        Parameters
        ----------
        dal_product: pyvo.dal.DALResults
            A pyvo DALResults object containing the requested data product
        colname: str
            The name of the column that contains the access information 
            in json format.
        
        """
        
        # if no cloud_access column, there is nothing to do
        if colname not in dal_product.fieldnames: 
            return
        
        for irow, jsontxt in enumerate(dal_product[colname]):
            desc = json.loads(jsontxt)

            # search for the known access types in desc
            for ap_name, APclass in class_mapper.items():

                if ap_name not in desc:
                    continue

                # TEMPORARY
                if 'access' in desc[ap_name]:
                    del desc[ap_name]['access']
                new_ap = APclass(**desc[ap_name])
                self.access_manager[irow].add_access_point(new_ap)
                
    
    def process_datalinks(self, dal_product):
        """Look for and process access point in datalinks
        
        Parameters
        ----------
        dal_product: pyvo.dal.DALResults
            A pyvo DALResults object containing the requested data product
            
            
        """
        
        # do we have datalinks?
        try:
            dlinks = dal_product.get_adhocservice_by_ivoid(
                pyvo.dal.adhoc.DATALINK_IVOID
            )
        except pyvo.DALServiceError:
            dlinks = None
        
        # if no datalinks, there is nothing to do here
        if dlinks is None:
            return
        
        # input parameters for the datalink call
        input_params = pyvo.dal.adhoc._get_input_params_from_resource(dlinks)
        dl_col_id = [p.ref for p in input_params.values() if p.ref is not None]
        dl_col_name = [f.name for f in dal_product.fielddescs if f.ID in dl_col_id]
        
        
        # proceed only if we have a PARAM named source, 
        if 'source' in input_params.keys():
            # we have a 'source' element, process it
            source_elem  = input_params['source']

            # list the available options in the `source` element:
            access_options = source_elem.values.options
            for description,option in access_options:
                
                # TEMPORARY
                option = option.replace('main-server', 'prem')
                if option == 'prem': continue
                
                soption = option.split(':')                
                query = pyvo.dal.adhoc.DatalinkQuery.from_resource(
                    dal_product, dlinks, 
                    source=option
                )
                
                dl_result = query.execute()
                dl_table = dl_result.to_table()
                
                ap_type = option.split(':')[0]
                ApClass = class_mapper[ap_type]
                for irow in range(len(dal_product)):
                    dl_res = dl_table[dl_table['ID'] == dal_product[dl_col_name[0]][irow]]
                    for dl_row in dl_res:
                        ap = ApClass(uri=dl_row['access_url'])
                        self.access_manager[irow].add_access_point(ap)
                                