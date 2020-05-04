### EE Uploader

_easy python uploads to GEE from feature collections_

---

1. [Install](#install)
2. [Quick Start](#quickstart)
3. [Project Setup](#setup)

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

Note: these examples use a feature collection file (fc.geojson) and args file (upargs.yaml) that are described in detail [below](#setup).

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
