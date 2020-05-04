### EE Uploader

_easy python uploads to GEE from feature collections_

---

1. [Install](#install)
2. [Quick Start](#quickstart)
3. [Project Setup](#setup)
4. [EEImagesUp Docs](#pydocs)

---

<a name="install"/>

### INSTALL

```bash
git clone https://github.com/wri/dl_exporter.git
pushd dl_exporter
pip install -e .
popd
```

---

<a name="quickstart"/>

### QUICK START EXAMPLES

Note: these examples use a feature collection file ([fc.geojson](#fcgeojson)) and args file ([upargs.yaml](#upargsyaml)) that are described in detail [below](#setup).

##### CLI

```bash
# print info before run output includes:
# - the number-of-features
# - an example upload manifest (defaults to the first feature)
eeuploader info fc.geojson upargs.yaml
# or using kwargs instead of arg-config file
eeuploader info fc.geojson user=brookwilliams collection=IM_COLLECTION_NAME


# upload images to a collection (as above kwargs can be used instead of an arg-config file)
# - all images
eeuploader upload fc.geojson upargs.yaml
# - the first 10 image features
eeuploader upload fc.geojson upargs.yaml --limit 10
# - image features from 10 to 20
eeuploader upload fc.geojson upargs.yaml --index_range 10,20
# - image features 3,400,24
eeuploader upload fc.geojson upargs.yaml --index_range 3,400,24
# - image features 3,400,24 with overwrite=True
eeuploader upload fc.geojson upargs.yaml --index_range 3,400,24 force=True
```

##### PYTHON

```python
import eeuploader.image as eup

up=eup.EEImagesUp(
    USER,
    features='fc.geojson',
    collection=IC,
    start_time_key='date',
    no_data=0,
    force=False)

# print manifest for first feature
pprint(up.manifest(1))

# upload first feature
up.upload(1)
""" ouput (no wait):
{'id': 'XER4J2RLIOWJRYNRMC7EALXH',
 'name': 'projects/earthengine-legacy/operations/XER4J2RLIOWJRYNRMC7EALXH',
 'started': 'OK'}
"""

# upload_collection
up.upload_collection()
print(up.tasks)
""" output
[{'creation_timestamp_ms': 1588615171535,
  'description': 'Ingest image: '
                 '"projects/earthengine-legacy/assets/users/..."',
  'destination_uris': ['https://...'],
  'id': 'VRJ3JFOPHL5ZFKVXTOKB4JEA',
  'name': 'projects/earthengine-legacy/operations/VRJ3JFOPHL5ZFKVXTOKB4JEA',
  'start_timestamp_ms': 1588615180457,
  'state': 'COMPLETED',
  'task_type': 'INGEST_IMAGE',
  'update_timestamp_ms': 1588615262051},...]
"""

# upload some random thing
up.upload(
    uri='gs://bucket/path/to/image.tif',
    crs='epsg:32717',
    properties={
        'property_1': 123
        'property_2': '2018-01-01',
        'propery_3': 'important piece of information'
    },
    start_time='2019-04-02' )
```

---

<a name="setup"/>

### PROJECT SETUP

1. features_collection file*: JSON containing a `features`-list, where each feature must has a `properties`-dict.
2. (optional) args file: a yaml file containing default arguments for `EEImagesUp.__init__`

* NOTE: Technically you can use `EEImagesUp` without the features_collection file, either for single uploads or by passing a feature-collection-python-dict instead of a file path.

<a name="fcgeojson"/>

##### FEATURE COLLECTION EXAMPLE:

```json
# fc.geojson
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "id": "0",
      "geometry": {
        "type": "Polygon",
        "coordinates": [[[-63.374182,-3.93245],[-63.328243,-3.932469],[-63.328225,-3.886331],[-63.374161,-3.886312],[-63.374182,-3.93245]]]
      },
      "properties": {
        "gcs": "v1/data/dev/WH/1/dw_-66.3899849022_-2.5792093972-20190820.tif",
        "crs": "epsg:32720",
        "date": "2019-08-12",
        "biome": 1,
        "biome_name": "Tropical & Subtropical Moist Broadleaf Forests",
        "NBPixels": 250596,
        "BareGround": 0.0004948203482896778,
        "BuiltArea": 0.001017574103337643,
        "Clouds": 0,
        "Crops": 0,
        "FloodedVegetation": 0.00739836230426663,
        "Grass": 0.00702724704304937,
        "Scrub": 0,
        "Snow Ice": 0,
        "Trees": 0.4854546760522913,
        "Water": 0.4986073201487654,
        "S2_DATASTRIP_ID": "S2B_OPER_MSI_L2A_DS_SGS__20190812T165657_S20190812T143758_N02.13",
        "S2_GEE_ID": "20190812T143759_20190812T143758_T20MMA",
        "S2_GRID": "20MMA",
        "S2_LEVEL": "2A",
        "S2_METHOD": "firstNonNull",
        "S2_PRODUCT_ID": "S2B_MSIL2A_20190812T143759_N0213_R096_T20MMA_20190812T165657",
        "S2_SENSING_ORBIT_DIRECTION": "DESCENDING",
        "dw_id": "dw_-63.3512901800_-3.9093045532-20190812",
        "eco_region": "Purus v�rzea",
        "flipped": false,
        "folder": "v1/data/dev/WorkForce/WH/1",
        "lat": -3.9093045532,
        "lon": -63.35129018,
        "map": "https://www.google.com/maps/@-3.9093046188354488,-63.35129165649414,14z/data=!3m1!1e3",
        "timestamp": 1565568000000,
      }
    },
    ...
  ]
}
```

<a name="upargsyaml"/>

##### UPLOAD ARGS EXAMPLE:

```yaml
# upargs.yaml
user: projects/wri-datalab
collection: image_collection_name
bands: null
band_names: 
    - lulc
pyramiding_policy: mode
no_data: 0
exclude: 
    - flipped
    - map
start_time_key: date
end_time_key: null
days_delta: 1
crs_key: crs
uri_key: gcs
name_key: ee_name
force: false
noisy: false 
raise_error: false
```
 
---

<a name="pydocs"/>

### EEImagesUp DOCS

METHODS:

1. [Initializer](#up-init)
2. [manifest](#up-manifest)
3. [upload](#up-upload)
4. [upload_collection](#up-upload_collection)

<a name="up-init"/>

##### EEImagesUp.\_\_init\_\_

```python
"""
Args:

    user<str>:
        gee user or project root
        * if it begins with "users" or "projects" the string is unaltered
        * otherwise it is pre-pended with "users"
    features<dict|list|str|None>:
        features list or file path to (geo)json feature collection
        * if dict or loaded from file path the features list is assumed to 
          be under the the key "features"
        * if None feat(s) or feat properties must be passed directly to the
          public methods.
        * otherwise feature indices can be used for manifest/upload/upload_collection
    collection<str|False>:
        name of image_collection/folder to upload the images.

        note: since the main purpose of this script is to upload many features
              it attempts to force you to use specify a collection. if you want
              to upload to your user/project folder root pass "False" 
    bands<list[dict]|None>:
        ** alternatively specify `band_names` (see below) **
        a manifest band list as specified here https://developers.google.com/earth-engine/image_manifest#bands
        
        note: every image being uploaded must have the same band structure
    band_names<list[dict]|None>:
        ** ignored if `bands` is not specified **
        generates a manifest band list as specified here https://developers.google.com/earth-engine/image_manifest#bands
        from a list of band_names

        note: every image being uploaded must have the same band structure
    pyramiding_policy<str>:
        one of MEAN, MODE, SAMPLE (upper or lower case is fine). default=MEAN

        note: for band-level control must use `bands` not `band_names` above
    no_data<int|list|dict>:
        no_data value(s) or "missing_data" object described here https://developers.google.com/earth-engine/image_manifest#bands
    include<list|None>:
        feature-property-keys to include as ee.image-properties
        * if None all the feature-property-keys will be included unless `exclude` list is provided
    exclude<list|None>:
        ** ignored if `include` is provided **
        feature-property-keys to exclude as ee.image-properties
    start_time_key,end_time_key,crs_key,uri_key,name_key<str|None>:
        if start_time/end_time/crs/... not provided at run time the system will
        attempt to find them in the feature-properties using these keys
    days_delta<int|False>:
        if not False, and start_time is provided (or found with start_time_key), and end_time is 
        not provided or found, end_time will be created by adding `days_delta` number of days
        to the start_time.
    timeout<int>:
        how quickly to timeout if `wait` is set to true. defaults to TIMEOUT above.
    force<bool>:
        set to true to overwrite existing assets
    noisy<bool>:
        print progress during `upload_collection`
    raise_error<bool>:
        raise_errors during `upload_collection`

Usage:

    import eeuploader.image as eup

    up=eup.EEImagesUp(
        'projects/wri-datalab',
        features='dw_organized_features.geojson',
        collection='image_collection_name',
        start_time_key='date',
        no_data=0,
        force=True)

    # print nb-features and manifest for first feature    
    print('NB FEATURES:',len(up.features))
    pprint(up.manifest(0))


    # upload the first feature / print task status
    # note: `upload` does not wait for task to complete.
    #       set `wait=True` to wait for task to complete 
    print(up.upload(0))
    eup.EEImagesUp.task_info(up.task_id)


    # upload the first 3 features / print task final task status for each
    up.upload_collection(limit=3)
    print(up.tasks)

"""
```

<a name="up-manifest"/>

##### EEImagesUp.manifest

```python
""" manifest for single upload

Args:

    feat<dict>: 
        a feature dictionary containing a properties dictionary
        from which it can pull the uri, crs, ee.image-properties, ...
    uri<str|None>:
        google cloud storage uri (with or without the preceding "gs://")
        or gcs url for image asset.
    name<str|None>:
        name of the new ee.image.  if not provided it will create a name
        from the uri. `.`s will be replaced with `d` due to ee-naming policies.
    tileset_id<str|None>:
        if not provided one will be created from the name
    crs<str|None>:
        crs of image (for example 'epsg:4326')
    propertie<dict>:
        updates any features existing in feat['properties']
    start/end_time<str|datetime|None>:
        strings should be in YYYY-MM-DD format
        
        if start_time but end_time is None, and self.days_delta end_time
        will be set start_time+(self.days_delta)days

Returns:
    
    <dict> Manifest for a single upload
"""
```

<a name="up-upload"/>

##### EEImagesUp.upload

```python
""" single upload

Note: if `wait=False` the upload will not wait for task to complete.

Args:

    **feat/uri/.../start_time/end_time (see manifest doc-string)**

    manifest<dict>:
        upload manifest.  if provided ignores all other arguments an upload
        using this manifest
    wait<bool>:
        wait for task to complete
    noisy<bool>:
        print progress during upload
    raise_error<bool>:
        raise_errors during upload

Sets:
    self.task_id<str>: task id
    self.task<dict>: task status

Returns:
    
    <dict> task status
"""
```

<a name="up-upload_collection"/>

##### EEImagesUp.upload_collection

```python
""" upload set of features in batches

* This method will always wait for tasks to complete before returning.
* `nb_batches` should be understood as the max number of simultaneous 
  requests for ee-image-uploads 

Args:

    features<list|None>:
        * list of features or feature indices in self.features to upload
        * if not provided upload all the features in self.features
    limit<int|None>:
        * limit features to first `limit`-elements
    nb_batches:
        divide uploads into `nb_batches` groups and upload them simultaneously

Sets:
    self.tasks<list>: list of task status

"""
```
