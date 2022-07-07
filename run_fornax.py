import sys
import os

import astropy.coordinates as coord

import pyvo

sys.path.insert(0, os.getcwd())
import fornax
print(f'\nUsing fornax library in: {fornax.__file__}\n')



# do a simple sia query to chanmaster
pos = coord.SkyCoord.from_name("ngc 4151")
query_result = pyvo.dal.sia.search('https://heasarc.gsfc.nasa.gov/xamin_aws/vo/sia?table=chanmaster&', pos=pos)
table_result = query_result.to_table()
access_url_column = query_result.fieldname_with_ucd('VOX:Image_AccessReference')


# data handler
data_product = table_result[5]

line = '+'*40
print(f'\n{line}\nData product:\n{line}')

print(data_product[['obs_id','target_name','instrument_name', 'access_format']])
print(f'{line}\n')


# inject a differnt region name; easier to do here than on the server
#row_1['cloud_access'] = row_1['cloud_access'].replace('us-east-1', 'us-east-2')
handler = fornax.AWSDataHandler(data_product, access_url_column)
handler._summary()
#handler.download()


## test access with credentials:
## heasarc-1 is at us-west-2 and allows cross region access with credentials (ngaps).
## Generate credentials in Kion and put them in a profile called ngap_temp_user in ~/.aws/credentials.
## Here we change the bucket name manually for testing
# data_product['cloud_access'] = data_product['cloud_access'].replace('dh-fornaxdev', 'heasarc-1')
# handler = fornax.AWSDataHandler(data_product, profile='ngap_temp_user')
# handler._summary()
# handler.download()