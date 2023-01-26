import sys
import os

import astropy.coordinates as coord

import pyvo

sys.path.insert(0, os.getcwd())
import fornax
print(f'\nUsing fornax library in: {fornax.__file__}\n')



pos = coord.SkyCoord.from_name("ngc 4151")

# chandra #
query_url = 'https://heasarc.gsfc.nasa.gov/xamin_aws/vo/sia?table=chanmaster&'

# hst #
#query_url = 'https://mast.stsci.edu/portal_vo/Mashup/VoQuery.asmx/SiaV1?MISSION=HST&'

query_result = pyvo.dal.sia.search(query_url, pos=pos, size=0.0, maxrec=3)
table_result = query_result.to_table()
access_url_column = query_result.fieldname_with_ucd('VOX:Image_AccessReference')



# data handler
# data_product = table_result[0]

# line = '+'*40
# print(f'\nData product:\n{line}')
# print(data_product[['instrument_name', access_url_column]])
# print(f'{line}\n')

## ------------------------------------------------------------------------------ ##
## If testing access with credentials, either set profile to the profile 
## name in ~/.aws/credentials, or define the environment variables: 
## AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN to authenticate.
## Then rename the bucket name in data_product. (e.g. from dh-fornaxdev 
## that does not require credentials to heasarc-1, which does). 
## Injecting the bucket name here is quick hack that we use instead of 
## modifying the server to return data-products with a different bucket name

#data_product['cloud_access'] = data_product['cloud_access'].replace('dh-fornaxdev', 'heasarc-1')
myprofile = None
## ------------------------------------------------------------------------------ ##


# OLD: pass a pyvo.dal.Record object to use datalinks
#fornax.get_data_product(query_result[0], 'aws', access_url_column=access_url_column, profile=myprofile)

line = '+'*25
print(f'\nCreating the DataHandler:\n{line}')
d_handler = fornax.DataHandler(query_result, aws_profile=None)

# download from on-prem
print(f'\nDownloading from prem:\n{line}')
ids = d_handler.download('prem', dryrun=True)
print('\n'.join(ids))
print()

# download from aws
#from IPython import embed;embed();exit(0)
print(f'\nDownloading from aws:\n{line}')
ids = d_handler.download('aws', dryrun=True)
print('\n'.join(ids))
print()
