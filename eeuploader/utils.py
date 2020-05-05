import json
import geojson
#
# I/O
#
def read_json(path,*key_path,is_geo=False):
    """ read json/geojson
    Args: 
        - path<str>: path to geojson file
        - args(key_path<str|int>):
            an ordered series of dict-keys/list-indices

            Ex: 
                * read_json('path.geojson','features') 
                  returns the feature list
                * read_json('path.geojson','features',0,'properties') 
                  returns the properties of the first feature

    """
    with open(path,'r') as file:
        if is_geo:
            jsn=geojson.load(file)
        else:
            jsn=json.load(file)
    for k in key_path:
        jsn=jsn[k]
    return jsn


def write_json(obj,path,indent):
    pass
