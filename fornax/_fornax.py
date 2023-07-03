"""
Cloud-related utils
"""

import json
from collections import UserDict

from astropy.table import Table, Row
from pyvo.dal import Record, DALResults, adhoc, DALServiceError
from .download import http_download, aws_download


# global variables
# JSON_COLUMN is the name of the column that contain the cloud json text
JSON_COLUMN = 'cloud_access'

# supported providers & their parameters
PROVIDERS = {
    'prem': ['url'],
    'aws' : ['uri', 'bucket_name', 'key']
}


__all__ = ['ProviderHandler', 'find_product_access']



def find_product_access(product, provider, mode='all', urlcolumn='auto', verbose=False, **kwargs):
    """Search for data product access information in some data product.

    This finds all available access information from prem, aws etc.

    Parameters
    ----------
    product: Record, DALResults, astropy Table or Row
        The data product.
    provider: str
        name of data provider: prem, aws, etc.
    mode: str
        The mode to use. Options include: json, datalink, ucd, or all.
    urlcolumn: str
        The name of the column that contains the url link to on-prem data.
        If 'auto', try to find the url by:
            - use getdataurl if product is either Record or DALResults
            - Use any column that contain http links if product is Row or Table.
    verbose: bool
        If True, print progress and debug text

    Keywords
    --------
    meta data needed to download the data, such as authentication profile
    which will be used to create access points. 

    prem:
        No keywords needed
    aws:
        aws_profile : str
            name of the user's profile for credentials in ~/.aws/config
            or ~/.aws/credentials. Use to authenticate the AWS user with boto3.
    
    Return
    ------
    ...

    """

    # check product
    if not isinstance(product, (Record, DALResults, Table, Row)):
        raise ValueError((
            f'product has the wrong type. Expecting dal.Record, '
            f'dal.DALResults, Table or Row. Found {type(product)}'
        ))

    if provider not in PROVIDERS:
        raise ValueError(f'provider {provider} is not supported. See PROVIDERS')

    # check mode
    if mode not in ['json', 'datalink', 'ucd', 'all']:
        raise ValueError((
            'mode has to be one of json, datalink, ucd or all'
        ))

    # convert product as a list of rows
    if isinstance(product, (Record, Row)):
        rows = [product]
    else:
        rows = [_ for _ in product]
    
    # get the prem url first
    prem_ap = [[] for _ in rows]
    if provider == 'prem':
        prem_ap = [[{'url':_getdataurl(row, urlcolumn, verbose)}] for row in rows]

    json_ap = [[] for _ in rows]
    if mode in ['json', 'all']:
        json_ap = _process_json_column(rows, provider, verbose=verbose)

    ucd_ap = [[] for _ in rows]
    if mode in ['ucd', 'all']:
        ucd_ap = _process_ucd_column(rows, provider, verbose=verbose)

    dl_ap = [[] for _ in rows]
    if mode in ['datalink', 'all']:
        dl_ap = _process_cloud_datalinks(rows, provider, verbose=verbose)

    # put them in one list of nrow lists of access points
    ap_list = [prem_ap[irow] + json_ap[irow] + ucd_ap[irow] + dl_ap[irow]
                  for irow in range(len(rows))]

    if isinstance(product, (Record, Row)):
        ap_list = ap_list[0]

    return ap_list


class ProviderHandler(UserDict):
    """Container for a list of providers as dict
    
    This a dict whose keys are suppored providers: prem, aws, etc
    and for each provider, the value is a list of strings that allow
    access to the data product. This list contain of the values of 
    parameters defined in PROVIDERS[provider]. The first element of the
    list (e.g. url for prem, and uri for aws) will be used as a unique 
    identifier for that handler
    
    """

    def __init__(self):
        """Initialize each provider with an empty list"""
        super(ProviderHandler, self).__init__()
        for provider, params in PROVIDERS.items():
            self.data[provider] = []

    def __repr__(self):
        return json.dumps(self.data, indent=1) 
    
    def __add__(self, val2):
        """Combine two Provider instances by concatenating their provider lists"""
        newp = Provider()
        for cval in [self, val2]:
            for key,val in cval.items():
                newp.data[key] += val
        return newp
    
    def __setitem__(self, key, value):
        """Not allowed directly; use add_provider
        """
        raise NotImplemented(f'use add_provider to add providers')

    
    def add_handler(self, provider, access_id=None, **kwargs):
        """Add a data handler
        
        Parameters
        ----------
        provider: str
            Provider name: prem, aws, etc. 
            The list is in the keys of PROVIDERS
        access_id: str
            A data access id, e.g. url or uri of the data.
            If given, it overrides the value of the first parameter 
            PROVIDERS[provider] in kwargs, if given. Used to avoid
            duplicates.
        
        Keywords
        --------
        verbose: bool
            If True, print progress and debug text
        
        Other parameters needed for each provider.
        The list is in the values of PROVIDERS
        
        """
        if provider not in PROVIDERS:
            raise ValueError(f'provider: {provider} is not supported')
        
        verbose = kwargs.pop('verbose', False)
        
        if access_id is not None:
            if not isinstance(access_id, str):
                raise ValueError('access_id has to be a str')
            kwargs[PROVIDERS[provider][0]] = access_id
        
        # if 'access_id' already exists; skip
        access_id = kwargs.get(PROVIDERS[provider][0], None)
        if access_id is not None:
            for link in self[provider]:
                if access_id == link[0]:
                    if verbose:
                        print(f'access_id {access_id} already exists. skipping ...')
                    return
        
        
        params = [kwargs.get(par, None) for par in PROVIDERS[provider]]
        if all([_ is None for _ in params]):
            require_p = ', '.join(PROVIDERS[provider])
            raise ValueError(f'Wrong parameter. Parameters for {provider} are: {require_p}')
        else:
            self[provider].append(params)

    
    def download(self,
                 provider, 
                 local_filepath=None,
                 cache=False,
                 timeout=None,
                 verbose=False,
                 **kwargs):
        """Download data from provider to local_filepath
        
        Parameters
        ----------
        provider: str
            options are prem, aws etc.
        local_filepath: str
            Local path, including filename, where the file is to be downloaded.
        cache : bool
            If True, check if a cached file exists before download
        timeout: int
            Time to attempt download before failing
        verbose: bool
            If True, print progress and debug text
        
        Keywords
        --------
        Other parameters to be passed to http_download, aws_download etc
        
        """
        
        download = {
            'prem': http_download,
            'aws' : aws_download
        }
        if provider not in download:
            raise ValueError(f'Unsupported provider {provider}')
        download_func  = download[provider]
        download_links = self[provider]
        func_keys = PROVIDERS[provider]
        
        errors = ''
        for link in download_links:
            kpars = {k:v for k,v in zip(func_keys, link)}
            kpars.update(local_filepath=local_filepath, cache=cache, 
                         timeout=timeout, verbose=verbose)
            kpars.update(**kwargs)
            try:
                if verbose:
                    print(f'Downloading from {provider} ...')
                download_func(**kpars)
                return
            except Exception as e:
                err_msg = f'Downloading from {provider} failed: {str(e)}'
                if verbose:
                    print(err_msg)
                if link != download_links[-1]:
                    msg2 = 'Trying other available links.'
                    if verbose:
                        print(msg2)
                    err_msg += f'\n{msg2}'
                errors += f'\n{err_msg}'
        # if we are here, then download has failed. Report the errors
        raise RuntimeError(errors)


def _getdataurl(product, urlcolumn='auto', verbose=False):
    """Work out the prem data url

    Parameters
    ----------
    product: Record or Row
    urlcolumn: str or None
        The name of the column that contains the url link to on-prem data.
        If 'auto', try to find the url by:
            - use getdataurl if product is either pyvo.dal.Record
            - Use any column that contain http(s) links if product is Row.
        If None, do not use url for on-prem access
    verbose: bool
        If True, print progress and debug text

    Return
    ------
    url (as str) if found or None

    """

    if not isinstance(product, (Record, Row)):
        raise ValueError('product has to be either dal.Record or Row')

    # column names
    if hasattr(product, 'fieldnames'):
        # DALResults
        colnames = product.fieldnames
    elif hasattr(product, '_results'):
        # dal.Record
        colnames = product._results.fieldnames
    else:
        colnames = product.colnames


    if urlcolumn == 'auto':
        if isinstance(product, Record):
            url = product.getdataurl()
            if verbose:
                print('Found url using product.getdataurl()')
        else:
            # try to find it
            for col in colnames:
                if isinstance(product[col], str) and 'http' in product[col]:
                    url = product[col]
                    if verbose:
                        print(f'Using url in column {col}')
                    break
    elif urlcolumn is None:
        url = None
    else:
        if urlcolumn not in colnames:
            raise ValueError(f'colname {urlcolumn} not available in data product')
        url = product[urlcolumn]
        if verbose:
            print(f'Using url in column {urlcolumn}')

    return url


def _process_json_column(products, provider, colname=JSON_COLUMN, verbose=False):
    """Look for and process any cloud information in a json column
    
    Parameters
    ----------
    products: list of Record or Row
        A list of product rows
    provider: str
        name of data provider: prem, aws, etc.
    colname: str
        The name for the column that contain the cloud json information
    verbose: bool
        If True, print progress and debug text
       
    
    Return
    ------
    A dict or a list of dict of parameters for every row in products
    
    
    """
    if not isinstance(products, list):
        raise ValueError('products is expected to be a list')
    
    if provider not in PROVIDERS:
        raise ValueError(f'provider {provider} is not supported. See PROVIDERS')
    
    if verbose:
        print(f'searching for and processing json column {colname}')
    
    rows_access_points = [[] for _ in products]
    for irow,row in enumerate(products):
        
        providers = []
        
        # if no colname column, there is nothing to do    
        try:
            jsontxt  = row[colname]
        except KeyError:
            # no json column, continue
            if verbose:
                print(f'No column {colname} found for row {irow}')
            rows_access_points.append(providers)
            continue
        
        jsonDict = json.loads(jsontxt)
        
        params = PROVIDERS[provider]
            
        if provider not in jsonDict:
            if verbose:
                print(f'No provider {provider} found for row {irow} in colum {colname}')
            continue

        p_params = jsonDict[provider]
        if not isinstance(p_params, list):
            p_params = [p_params]

        for ppar in p_params:
            rows_access_points[irow].append(ppar)

        
    return rows_access_points


def _process_ucd_column(products, provider, verbose=False):
    """Look for and process any cloud information in columns
    with ucd of the form: 'meta.ref.{provider}'.
    
    Note that products needs to be a Record. astropy
    table Row objects do not handle UCDs.
    
    Parameters
    ----------
    products: list
        A list of Record
    provider: str
        name of data provider: prem, aws, etc.
    verbose: bool
        If True, print progress and debug text
       
    
    Return
    ------
    A dict or a list of dict of parameters for every row in products
    
    """
    if not isinstance(products, list):
        raise ValueError('products is expected to be a list')
    
    if not provider in PROVIDERS:
        raise ValueError(f'provider {provider} is not supported. See PROVIDERS')
    
    if not isinstance(products[0], Record):
        raise ValueError((
            f'products has the wrong type. Expecting a list of '
            f'Record. Found {type(products[0])}'
        ))
    
    if verbose:
        print(f'searching for and processing cloud ucd column(s)')
    
    rows_access_points = [[] for _ in products]
    for irow,row in enumerate(products):
                    
        uri = row.getbyucd(f'meta.ref.{provider}')
        if uri is not None:
            parname = PROVIDERS[provider][0]
            rows_access_points[irow].append({parname:uri})
        

    return rows_access_points


def _process_cloud_datalinks(products, provider, verbose=False):
    """Look for and process any cloud information in datalinks
    
    Note that products needs to be a Record. astropy
    table Row objects do not handle datalinks.
    
    Parameters
    ----------
    products: list
        A list of dal.Record
    provider: str
        name of data provider: prem, aws, etc.
    verbose: bool
        If True, print progress and debug text
        
    Return
    ------
    A dict or a list of dict of parameters for every row in products
    
    """
    if not isinstance(products, list):
        raise ValueError('products is expected to be a list')
    
    if not isinstance(products[0], Record):
        raise ValueError((
            f'products has the wrong type. Expecting a list of '
            f'Record. Found {type(products[0])}'
        ))
    
    if provider not in PROVIDERS:
        raise ValueError(f'provider {provider} is not supported. See PROVIDERS')
    
    if verbose:
        print(f'searching for and processing datalinks')
    
    dalResult = products[0]._results
    
    rows_access_points = [[] for _ in products]
    
    # get datalink service
    try:
        _datalink = dalResult.get_adhocservice_by_id('cloudlinks')
    except (DALServiceError, AttributeError):
        # No datalinks; return
        return rows_access_points
    
    nrows = len(products)
    
    # input parameters for the datalink call
    in_params = adhoc._get_input_params_from_resource(_datalink)
    dl_col_id = [p.ref for p in in_params.values() if p.ref is not None]
    
    # name of parameter to specify the provider; we initially used source
    provider_par = 'provider'
    
    # proceed only if we have a PARAM named provider_par, 
    if provider_par in in_params.keys():
        # we have a 'provider' element, process it
        provider_elem  = in_params[provider_par]
        
        # list the available providers in the `provider_par` element:
        provider_options = provider_elem.values.options
        
        
        for description,option in provider_options:
            
            dl_provider = option.split(':')[0]
            if dl_provider != provider:
                # provider in datalinnks (dl_provider) does not
                # much what the user requested (provider)
                continue

            # TODO: consider including batch_size simialr to 
            # DatalinkResultsMixin.iter_datalinks
            query = adhoc.DatalinkQuery.from_resource(
                products, _datalink, 
                **{provider_par:option}
            )
            
            dl_result = query.execute()
            dl_table = dl_result.to_table()
                
            parname = PROVIDERS[provider][0]
            
            for irow in range(nrows):
                dl_res = dl_table[dl_table['ID'] == products[irow][dl_col_id[0]]]
                for dl_row in dl_res:
                    rows_access_points[irow].append(
                        {parname:dl_row['access_url']}
                    )
    
    return rows_access_points