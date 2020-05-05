from pathlib import Path, PurePath
import json
import geojson
import yaml
import pickle
#
# I/O
#
def ensure_dir(path=None,directory=None):
    """ create directory if it does not exist 

    Args:
        * if path: check/create parent directory of path
        * else: check/create directory

    """
    if path:
        directory=PurePath(path).parent
    Path(directory).mkdir(
            parents=True,
            exist_ok=True)


def read_json(path,*key_path,is_geo=False,mode='r'):
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
    with open(path,mode) as file:
        if is_geo:
            jsn=geojson.load(file)
        else:
            jsn=json.load(file)
    return _obj(jsn,key_path)


def read_yaml(path,*key_path,mode='rb'):
    """ read yaml file
    Args: 
        - path<str>: path to yaml file
        - args(key_path<str|int>):
            an ordered series of dict-keys/list-indices
    """    
    with open(path,mode) as file:
        obj=yaml.safe_load(file)
    return _obj(obj,key_path)


def read_pickle(path,*key_path,mode='rb'):
    """ read pickle file

    Note: *key_path should only be used for pickled-dicts

    Args: 
        - path<str>: path to pickle file
        - args(key_path<str|int>):
            an ordered series of dict-keys/list-indices    
    """    
    with open(path,mode) as file:
        obj=pickle.load(file)
    return _obj(obj,key_path)


def save_json(obj,path,indent=4,sort_keys=False,mkdirs=True,mode='w'):
    """ save object to json file
    """ 
    if mkdirs:
        ensure_dir(path)
    with open(path,mode) as file:
        json.dump(obj,file,indent=indent,sort_keys=sort_keys)


def save_yaml(obj,path,mkdirs=True,mode='w+'):
    """ save object to yaml file
    """ 
    if mkdirs:
        ensure_dir(path)
    with open(path,mode) as file:
        file.write(yaml.safe_dump(config, default_flow_style=False))


def save_pickle(obj,path,mkdirs=True,mode='wb',protocol=pickle.HIGHEST_PROTOCOL):
    """ save object to pickle file
    """ 
    if mkdirs:
        ensure_dir(path)
    with open(path,mode) as file:
        pickle.dump(obj,file,protocol=protocol)


#
# INTERNAL
#
def _obj(obj,key_path):
    for k in key_path:
        obj=obj[k]
    return obj

