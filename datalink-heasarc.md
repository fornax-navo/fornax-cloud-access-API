### 1. Introduction
This notenbook contains a simple example of using datalinks to serve data from the cloud. 

This is example uses the HEASARC SIA service. The changes needed on the server side for this to work are:

---
1. In the SIA (or any other service where datalinks can work), add a `<PARAM>` element inside the `<GROUP>` element in service resources, as defined in the [datalinks standars document](https://ivoa.net/documents/DataLink/20150617/REC-DataLink-1.0-20150617.html).
The `<PARAM>` element have a name `source`, and contain sources from where the data can be accessed. 

The following shows an example where the data can be access from four sources:

    - On prem servers
    - AWS US east1
    - AWS US east2
    - Google Cloud (GC).


```xml
<RESOURCE utype="adhoc:service" type="meta">
    <PARAM datatype="char" arraysize="*" name="standardID" value="ivo://ivoa.net/std/DataLink#links-1.0"/>
    <PARAM datatype="char" arraysize="*" name="accessURL" value="http://localhost:8080/xamin/vo/datalink/chanmaster"/>
    <GROUP name="inputParams">
        <PARAM ref="DataLinkID" datatype="char" arraysize="*" name="id" value=""/>
        <PARAM datatype="char" arraysize="*" name="source" value="main-server">
            <VALUES>
                <OPTION name="On prem servers" value="main-server"/>
                <OPTION name="AWS region 1" value="aws:us-east1"/>
                <OPTION name="AWS some other region" value="aws:us-east2"/>
                <OPTION name="GC some region" value="gc"/>
            </VALUES>
        </PARAM>
    </GROUP>
</RESOURCE>

```

---

2. The datalink service source be able to interpret the source parameter, and serve the appropriate `access_url`. So a request to the datalink url with `&source=main-server` should give something like:

```xml
<TABLE>
    <FIELD datatype="char" arraysize="*" ucd="meta.id;meta.main" name="ID"/>
    <FIELD datatype="char" arraysize="*" ucd="meta.ref.url" name="access_url"/>
    ...
    <DATA>
        <TABLEDATA>
            <TR>
                <TD>[SOME_ID]</TD>
                <TD>https://someurl/path/to/some/file.fits</TD>
                ...
            </TR>
        </TABLEDATA>
    </DATA>
</TABLE>
```

Passing `&source=aws:us-east1` for example would give:

```xml
<TABLE>
    <FIELD datatype="char" arraysize="*" ucd="meta.id;meta.main" name="ID"/>
    <FIELD datatype="char" arraysize="*" ucd="meta.ref.url" name="access_url"/>
    ...
    <DATA>
        <TABLEDATA>
            <TR>
                <TD>[SOME_ID]</TD>
                <TD>s3://somebucket/path/to/some/file.fits</TD>
                ...
            </TR>
        </TABLEDATA>
    </DATA>
</TABLE>
```

---


### 2. Setup an SIA Query



```python
import pyvo
from astropy.coordinates import SkyCoord

# set some sky position to use in the queries
pos = SkyCoord.from_name('NGC 4151')
```


```python
# make a simple SIA query. If not using in HEASARC, change sia_url.
#xaminUrl = 'http://localhost:8080/xamin'
xaminUrl = 'https://heasarc.gsfc.nasa.gov/xamin_aws'
sia_url = f'{xaminUrl}/vo/sia?table=chanmaster'

sia_result = pyvo.dal.sia.search(sia_url, pos=pos, resultmax=2)
```


```python
# explore the returned SIA result
#sia_result.votable.to_xml('sai_result.xml')
sia_result.to_table()
```




<div><i>Table length=2</i>
<table id="table5089544464" class="table-striped table-bordered table-condensed">
<thead><tr><th>obsid</th><th>status</th><th>name</th><th>ra</th><th>dec</th><th>time</th><th>detector</th><th>grating</th><th>exposure</th><th>type</th><th>pi</th><th>public_date</th><th>datalink</th><th>t_min</th><th>t_resolution</th><th>t_max</th><th>t_exptime</th><th>em_res_power</th><th>s_region</th><th>s_ra</th><th>s_dec</th><th>s_resolution</th><th>access_estsize</th><th>s_fov</th><th>o_ucd</th><th>access_url</th><th>obs_publisher_did</th><th>obs_id</th><th>obs_collection</th><th>target_name</th><th>instrument_name</th><th>facility_name</th><th>pol_states</th><th>calib_level</th><th>access_format</th><th>dataproduct_type</th><th>em_min</th><th>em_max</th><th>SIA_title</th><th>SIA_scale</th><th>SIA_naxis</th><th>SIA_naxes</th><th>SIA_format</th><th>SIA_reference</th><th>SIA_ra</th><th>SIA_dec</th><th>SIA_instrument</th><th>cloud_access</th></tr></thead>
<thead><tr><th></th><th></th><th></th><th>deg</th><th>deg</th><th>mjd</th><th></th><th></th><th>s</th><th></th><th></th><th>mjd</th><th></th><th>d</th><th>s</th><th>d</th><th>s</th><th></th><th>deg</th><th>deg</th><th>deg</th><th>arcsec</th><th>kbyte</th><th>deg</th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th>m</th><th>m</th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th></tr></thead>
<thead><tr><th>object</th><th>object</th><th>object</th><th>float64</th><th>float64</th><th>float64</th><th>object</th><th>object</th><th>float64</th><th>object</th><th>object</th><th>int32</th><th>object</th><th>float64</th><th>float64</th><th>float64</th><th>float64</th><th>float64</th><th>object</th><th>float64</th><th>float64</th><th>float32</th><th>int32</th><th>float64</th><th>object</th><th>object</th><th>object</th><th>object</th><th>object</th><th>object</th><th>object</th><th>object</th><th>object</th><th>int32</th><th>object</th><th>object</th><th>float64</th><th>float64</th><th>object</th><th>object</th><th>object</th><th>int32</th><th>object</th><th>object</th><th>float64</th><th>float64</th><th>object</th><th>object</th></tr></thead>
<tr><td>15158</td><td>archived</td><td>RBS1066</td><td>181.29000</td><td>39.34700</td><td>56363.8531</td><td>ACIS-I</td><td>NONE</td><td>8080</td><td>GO</td><td>Reiprich</td><td>56729</td><td>18244:chandra.obs.img</td><td>56363.8531481481</td><td>--</td><td>64443.8531481481</td><td>8080.0</td><td>--</td><td></td><td>181.29</td><td>39.347</td><td>--</td><td>32447</td><td>--</td><td></td><td>https://heasarc.gsfc.nasa.gov/FTP/chandra/data/byobsid/8/15158/primary/acisf15158N003_cntr_img2.fits.gz</td><td>HEASARC</td><td>15158</td><td>CHANDRA ACIS-I</td><td>RBS1066</td><td>ACIS-I</td><td>Chandra</td><td></td><td>3</td><td>image/fits</td><td>Image</td><td>1.24e-10</td><td>1.24e-08</td><td>acisf15158N003_cntr_img2.fits</td><td>[-0.0013666666666667 0.0013666666666667]</td><td>[1024 1024]</td><td>2</td><td>image/fits</td><td>https://heasarc.gsfc.nasa.gov/FTP/chandra/data/byobsid/8/15158/primary/acisf15158N003_cntr_img2.fits.gz</td><td>181.29</td><td>39.347</td><td>CHANDRA ACIS-I</td><td>{&quot;aws&quot;: {   &quot;bucket_name&quot;: &quot;dh-fornaxdev&quot;,   &quot;region&quot;: &quot;us-east-1&quot;,   &quot;access&quot;: &quot;region&quot;,   &quot;key&quot;: &quot;/FTP/chandra/data/byobsid/8/15158/primary/acisf15158N003_cntr_img2.fits.gz&quot; }}</td></tr>
<tr><td>15158</td><td>archived</td><td>RBS1066</td><td>181.29000</td><td>39.34700</td><td>56363.8531</td><td>ACIS-I</td><td>NONE</td><td>8080</td><td>GO</td><td>Reiprich</td><td>56729</td><td>18244:chandra.obs.img</td><td>56363.8531481481</td><td>--</td><td>64443.8531481481</td><td>8080.0</td><td>--</td><td></td><td>181.29</td><td>39.347</td><td>--</td><td>228059</td><td>--</td><td></td><td>https://heasarc.gsfc.nasa.gov/FTP/chandra/data/byobsid/8/15158/primary/acisf15158N003_cntr_img2.jpg</td><td>HEASARC</td><td>15158</td><td>CHANDRA ACIS-I</td><td>RBS1066</td><td>ACIS-I</td><td>Chandra</td><td></td><td>3</td><td>image/jpeg</td><td>Image</td><td>1.24e-10</td><td>1.24e-08</td><td>acisf15158N003_cntr_img2.jpg</td><td>[-0.0013666666666667 0.0013666666666667]</td><td>[1024 1024]</td><td>2</td><td>image/jpeg</td><td>https://heasarc.gsfc.nasa.gov/FTP/chandra/data/byobsid/8/15158/primary/acisf15158N003_cntr_img2.jpg</td><td>181.29</td><td>39.347</td><td>CHANDRA ACIS-I</td><td>{&quot;aws&quot;: {   &quot;bucket_name&quot;: &quot;dh-fornaxdev&quot;,   &quot;region&quot;: &quot;us-east-1&quot;,   &quot;access&quot;: &quot;region&quot;,   &quot;key&quot;: &quot;/FTP/chandra/data/byobsid/8/15158/primary/acisf15158N003_cntr_img2.jpg&quot; }}</td></tr>
</table></div>



### 3. A Standard Datalink Query from the SIA Result


```python
# get the datalink for the first row
dlink = sia_result[0].getdatalink()

# explore the returned datalink result
#dlink.votable.to_xml('datalink_result.xml')
dlink.to_table()
```




<div><i>Table length=4</i>
<table id="table5089872144" class="table-striped table-bordered table-condensed">
<thead><tr><th>ID</th><th>access_url</th><th>service_def</th><th>error_message</th><th>description</th><th>semantics</th><th>content_type</th><th>content_length</th><th>cloud_access</th></tr></thead>
<thead><tr><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th>byte</th><th></th></tr></thead>
<thead><tr><th>object</th><th>object</th><th>object</th><th>object</th><th>object</th><th>object</th><th>object</th><th>int64</th><th>object</th></tr></thead>
<tr><td>18244:chandra.obs.img</td><td>https://heasarc.gsfc.nasa.gov/FTP/chandra/data/byobsid/8//15158/primary/acisf15158N003_cntr_img2.fits.gz</td><td></td><td></td><td>Center Image</td><td>https://localhost:8080/xamin/jsp/products.jsp#chandra.obs.img.cntr.fits</td><td>application/fits</td><td>--</td><td>{&quot;aws&quot;: {   &quot;bucket_name&quot;: &quot;dh-fornaxdev&quot;,   &quot;region&quot;: &quot;us-east-1&quot;,   &quot;access&quot;: &quot;region&quot;,   &quot;key&quot;: &quot;/FTP/chandra/data/byobsid/8//15158/primary/acisf15158N003_cntr_img2.fits.gz&quot; }}</td></tr>
<tr><td>18244:chandra.obs.img</td><td>https://heasarc.gsfc.nasa.gov/FTP/chandra/data/byobsid/8//15158/primary/acisf15158N003_full_img2.fits.gz</td><td></td><td></td><td>Full Image</td><td>https://localhost:8080/xamin/jsp/products.jsp#chandra.obs.img.full.fits</td><td>application/fits</td><td>--</td><td>{&quot;aws&quot;: {   &quot;bucket_name&quot;: &quot;dh-fornaxdev&quot;,   &quot;region&quot;: &quot;us-east-1&quot;,   &quot;access&quot;: &quot;region&quot;,   &quot;key&quot;: &quot;/FTP/chandra/data/byobsid/8//15158/primary/acisf15158N003_full_img2.fits.gz&quot; }}</td></tr>
<tr><td>18244:chandra.obs.img</td><td>https://heasarc.gsfc.nasa.gov/FTP/chandra/data/byobsid/8//15158/primary/acisf15158N003_cntr_img2.jpg</td><td></td><td></td><td>Center Image</td><td>https://localhost:8080/xamin/jsp/products.jsp#chandra.obs.img.cntr.jpg</td><td>image/jpeg</td><td>--</td><td>{&quot;aws&quot;: {   &quot;bucket_name&quot;: &quot;dh-fornaxdev&quot;,   &quot;region&quot;: &quot;us-east-1&quot;,   &quot;access&quot;: &quot;region&quot;,   &quot;key&quot;: &quot;/FTP/chandra/data/byobsid/8//15158/primary/acisf15158N003_cntr_img2.jpg&quot; }}</td></tr>
<tr><td>18244:chandra.obs.img</td><td>https://heasarc.gsfc.nasa.gov/FTP/chandra/data/byobsid/8//15158/primary/acisf15158N003_full_img2.jpg</td><td></td><td></td><td>Full Image</td><td>https://localhost:8080/xamin/jsp/products.jsp#chandra.obs.img.full.jpg</td><td>image/jpeg</td><td>--</td><td>{&quot;aws&quot;: {   &quot;bucket_name&quot;: &quot;dh-fornaxdev&quot;,   &quot;region&quot;: &quot;us-east-1&quot;,   &quot;access&quot;: &quot;region&quot;,   &quot;key&quot;: &quot;/FTP/chandra/data/byobsid/8//15158/primary/acisf15158N003_full_img2.jpg&quot; }}</td></tr>
</table></div>



### 4. Process the New Cloud Information
Read the cloud information from the datalink resource in the SIA result. 

This is done by exposing what `getdatalink()` does inside `pyvo`, and we add the part that processes the extra parameters


```python
# expose what goes on inside pyvo when doing getdatalink()
dlink_resource = sia_result.get_adhocservice_by_ivoid(pyvo.dal.adhoc.DATALINK_IVOID)

# Look for the 'source' <PARAM> element inside the inputParams <GROUP> element.
# pyvo already handles part of this.
source_elem = [p for p in dlink_resource.groups[0].entries if p.name == 'source'][0]
print(type(source_elem))
print(source_elem)
```

    <class 'astropy.io.votable.tree.Param'>
    <PARAM ID="source" arraysize="*" datatype="char" name="source" value="main-server"/>



```python
# list the available options in the `source` element:
access_options = source_elem.values.options

print(f'There are {len(access_options)} options:')
for opt in access_options:
    print(f'\t{opt[1]:13}: {opt[0]}')
```

    There are 4 options:
    	main-server  : On prem servers
    	aws:us-east1 : AWS region 1
    	aws:us-east2 : AWS some other region
    	gc           : GC some region


---

Given these options, we can query for the datalink we want by including the parameter `source` in the query, where its value takes one of the options in `access_options`

#### a. Use the `main-server` option (default):


```python
## main-server; this is the default
source_1 = access_options[0][1]
query_1  = pyvo.dal.adhoc.DatalinkQuery.from_resource(
                sia_result[0], dlink_resource, sia_result._session, source=source_1
            )
result_1 = query_1.execute()

print(f'access option: {source_1}')
print('access_url: ')
print(result_1[0].access_url)
```

    access option: main-server
    access_url: 
    https://heasarc.gsfc.nasa.gov/FTP/chandra/data/byobsid/8//15158/primary/acisf15158N003_cntr_img2.fits.gz


#### b. Use the `aws:us-east1` option:
Note that `access_url` is now an s3 uri.


```python
## aws:us-east1
source_2 = access_options[1][1]
query_2  = pyvo.dal.adhoc.DatalinkQuery.from_resource(
                sia_result[0], dlink_resource, sia_result._session, source=source_2
            )
result_2 = query_2.execute()

print(f'access option: {source_2}')
print('access_url: ')
print(result_2[0].access_url)
```

    access option: aws:us-east1
    access_url: 
    s3://dh-fornaxdev/FTP/chandra/data/byobsid/8//15158/primary/acisf15158N003_cntr_img2.fits.gz


#### c. Use `gc` option:
This is not supported, so we fall back to the default


```python
## gc; GC is not implemented so the server defaults http from main server
source_3 = access_options[3][1]
query_3  = pyvo.dal.adhoc.DatalinkQuery.from_resource(
                sia_result[0], dlink_resource, sia_result._session, source=source_3
            )
result_3 = query_3.execute()

print(f'access option: {source_3}')
print('access_url: ')
print(result_3[0].access_url)
```

    access option: gc
    access_url: 
    https://heasarc.gsfc.nasa.gov/FTP/chandra/data/byobsid/8//15158/primary/acisf15158N003_cntr_img2.fits.gz



```python

```
