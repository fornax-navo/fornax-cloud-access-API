# fornax-cloud-access-API
Repository for cloud access considerations

We consider two options for serving cloud data along with standard VO services. In the following, we focus on the Simple Image Access service (SIA). The same suggested changes can be applied to other services, and will be considered at a later date. The code in this reporsitory is the client that handles the cloud inforomation provided by the server.

The two options are: 
- 1. An extra column that contains the cloud access information
- 2. Using the VO Datalinks service.
These two options are discussed in detail below:

---
### 1. Additional JSON Column:
This option provides the most flexible and general solution. It can even be used when the returned products are not VO products.

We suggest adding an extra column called `cloud_access`, that has JSON text that describe where the data is located on the cloud.

An example JSON text to access the file `chandra/data/byobsid/0/3480/primary/acisf03480N004_cntr_img2.jpg` from a bucket  called `heasarc-bucket` is given below:

```json
{
    "aws": 
        { 
        "bucket_name": "heasarc-bucket", 
        "key": "chandra/data/byobsid/0/3480/primary/acisf03480N004_cntr_img2.jpg" 
        "region": "us-east-1"        
        }
}
```
The region of the bucket is given in `"region"`, and it is optional.

If the data is available from multiple locations, lists can be used. For example, the data is available from multiple aws buckets, they can be served as:
```json
{
    "aws": 
        [
            { 
            "bucket_name": "bucket-1", 
            ...        
            },
            { 
            "bucket_name": "bucket-2", 
            ...        
            }
        ]
}
```

The following are two XML files showing the implementation of the JSON column from the [HEASARC](https://heasarc.gsfc.nasa.gov/xamin_aws/vo/sia?table=chanmaster&pos=182.63,39.40&resultformat=text/xml&resultmax=2) and [MAST](https://mast.stsci.edu/portal_vo/Mashup/VoQuery.asmx/SiaV1?MISSION=HST&pos=182.63,39.40) service.

---
### 2. Cloud Data with Datalinks:
In the SIA (or any other service where datalinks can work), add a `<PARAM>` element inside the `<GROUP>` element in the `adhoc:service` `<RESOURCE>`, as defined in the [datalinks standars document](https://ivoa.net/documents/DataLink/20150617/REC-DataLink-1.0-20150617.html).
The `<PARAM>` element have a name `source`, and contains sources from where the data can be accessed. The default is `main-server`, that indicates accessing data from on-prem servers.

The following shows an example where the data can be access from four sources:

    - On prem servers (`value="main-server"`)
    - AWS US east1 (value="aws:us-east1")
    - AWS US east2 (value="aws:us-east2")
    - Google Cloud (value="gc").
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

The datalink service should be able to interpret the `source` parameter that the clients sends with the datalink request, and serve the appropriate `access_url`. So a request to the datalink url with `&source=main-server` should give something like:

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
