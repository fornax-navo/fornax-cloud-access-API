"""
Cloud-related utils
"""

import json
from collections import UserDict, UserList

from astropy.table import Table, Row
from pyvo.dal import Record, DALResults, adhoc, DALServiceError
from .download import http_download, aws_download


# global variables
# JSON_COLUMN is the name of the column that contain the cloud json text
JSON_COLUMN = 'cloud_access'

# supported providers & their parameters
# The keys are the names of supported providers.
# The values is a list where the last element is the name
# of the download function, and the rest are the parameters
# that need to be passed to it.
PROVIDERS = {
    'prem': ['url', http_download],
    'aws' : ['uri', 'bucket_name', 'key', aws_download]
}


__all__ = ['supported_providers', 'get_data_product2']



def get_data_product2(product, provider, mode='all', urlcolumn='auto', verbose=False, **kwargs):
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
    extra_prem: bool
        If True, find extra prem links from datalinks, json etc. Default is False,
        which finds the the url based on the value of urlcolumn.

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

    extra_prem = kwargs.pop('extra_prem', False)

    # check product
    if not isinstance(product, (Record, DALResults, Table, Row)):
        raise ValueError((
            f'product has the wrong type. Expecting dal.Record, '
            f'dal.DALResults, Table or Row. Found {type(product)}'
        ))

    if provider not in PROVIDERS:
        raise ValueError(f'provider {provider} is not supported. See supported_providers')

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
        ap_list = prem_ap

    if provider != 'prem' or extra_prem:
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


    handlers = [ProviderHandler(provider, aplist) for aplist in ap_list]
    if isinstance(product, (Record, Row)):
        handlers = handlers[0]
    return handlers



def supported_providers():
    """Return a list of supported providers"""
    return list(PROVIDERS.keys())


class ProviderHandler(UserList):
    """Container for a list of access links from a provider.
    Basically a list that has a download method
    
    """
    
    def __init__(self, provider, *args):
        """Initialize the handler by defining the provider and list of access links
        
        Parameters
        ----------
        provider: str
            Provider name: prem, aws, etc. 
            The list is returned by supported_providers()
        args:
            a list of dict values defining the links. e.g the elements of
            the value returned by get_data_product
        
        """
        if provider not in PROVIDERS:
            raise ValueError(f'provider {provider} is not supported. See supported_providers')
        
        super(ProviderHandler, self).__init__(*args)
        self.provider = provider
        
    def __repr__(self):
        return f'ProviderHandler(nlinks: {len(self.data)})'
    
    
    def download(self,
                 local_filepath=None,
                 cache=False,
                 timeout=None,
                 verbose=False,
                 **kwargs):
        """Download data to local_filepath

        This loops through the available access links, and download
        data from the first one that is accessible.
        
        Parameters
        ----------
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

        provider = self.provider
        download_func  = PROVIDERS[provider][-1]
        download_links = self.data
        func_keys = PROVIDERS[provider][:-1]

        errors = ''
        exceptions = []
        for link in download_links:
            kpars = {k:v for k,v in link.items() if k in func_keys}
            kpars.update(local_filepath=local_filepath, cache=cache, 
                         timeout=timeout, verbose=verbose)
            kpars.update(**kwargs)
            try:
                if verbose:
                    print(f'Downloading from {provider} with parameters {kpars}')
                download_func(**kpars)
                return
            except Exception as e:
                exceptions.append(e)
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
        if verbose:
            print(errors)
        raise exceptions[-1]



def _getdataurl(product, urlcolumn='auto', verbose=False):
    """Work out the prem data url

    Parameters
    ----------
    product: Record or Row
    urlcolumn: str or None
        The name of the column that contains the url link to on-prem data.
        If 'auto', try to find the url by:
            - use getdataurl if product is pyvo.dal.Record
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
        raise ValueError(f'provider {provider} is not supported. See supported_providers')
    
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
        
        params = PROVIDERS[provider][:-1]
            
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
        raise ValueError(f'provider {provider} is not supported. See supported_providers')
    
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
        raise ValueError(f'provider {provider} is not supported. See supported_providers')
    
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