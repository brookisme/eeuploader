from __future__ import print_function
import os,sys
sys.path.append(os.environ.get('PROJECT_DIR','..'))
import re
from pprint import pprint
import yaml
import click
import eeuploader.image as eeup

#
# DEFAULTS
#
UPLOAD_HELP='upload feature collection'
RANGE_HELP='restrict to index range'
LIMIT_HELP='limit to first N'
INDEX_HELP='index of feature to generate manifiest'
LIMIT=None
NOISY=False
INDEX=0
INDICES='comma separated feature index list'
NOISY_HELP='be noisy'
INFO_HELP='print number of features and manifiest for first feature'
ARG_KWARGS_SETTINGS={
    'ignore_unknown_options': True,
    'allow_extra_args': True
}


ERROR_MISSING_PARAM_FILE="ee.uploader.cli: {} is not a file"



#
# CLI INTERFACE
#
@click.group()
@click.pass_context
def cli(ctx):
    ctx.obj={}


@click.command(
    help=UPLOAD_HELP,
    context_settings=ARG_KWARGS_SETTINGS ) 
@click.argument('feature_collection',type=str)
@click.option(
    '--range',
    help=RANGE_HELP,
    default=None,
    type=str)
@click.option(
    '--indices',
    help=INDICES,
    default=None,
    type=str)
@click.option(
    '--limit',
    help=LIMIT_HELP,
    default=LIMIT,
    type=int)
@click.option(
    '--noisy',
    help=NOISY_HELP,
    default=NOISY,
    type=bool)
@click.pass_context
def upload(ctx,feature_collection,range,indices,limit,noisy):
    upkwargs=_upload_kwargs(ctx.args)
    up=eeup.EEImagesUp(
        features=feature_collection,
        **upkwargs)
    print(feature_collection,upkwargs,range,indices,limit,noisy)
    print(up._path_parts)
    pass




@click.command(
    help=INFO_HELP,
    context_settings=ARG_KWARGS_SETTINGS ) 
@click.argument('feature_collection',type=str)
@click.option(
    '--index',
    help=INDEX_HELP,
    default=INDEX,
    type=int)
@click.pass_context
def info(ctx,feature_collection,index):
    upkwargs=_upload_kwargs(ctx.args)
    up=eeup.EEImagesUp(
        features=feature_collection,
        **upkwargs)
    print('\n'*2)
    print('eeuploader.cli.info:')
    print()
    print('- feature_collection:',feature_collection)
    print('- nb_features:',len(up.features))
    print(f'- ex manifest[{index}]:')
    print()
    pprint(up.manifest(index))
    print('\n'*2)


#
# INTERNAL
#
def _upload_kwargs(ctx_args):
    args,kwargs=_args_kwargs(ctx_args)
    if args:
        path=args[0]
        if os.path.isfile(path):
            upkwargs=_read_yaml(path)
        else:
            raise ValueError(ERROR_MISSING_PARAM_FILE.format(path))
    else:
        upkwargs={}
    upkwargs.update(kwargs)
    return upkwargs



def _args_kwargs(ctx_args):
    args=[]
    kwargs={}
    for a in ctx_args:
        if re.search('=',a):
            k,v=a.split('=')
            kwargs[k]=v
        else:
            args.append(a)
    return args,kwargs


def _read_yaml(path,*key_path):
    """ read yaml file
    path<str>: path to yaml file
    *key_path: keys to go to in object
    """    
    with open(path,'rb') as file:
        obj=yaml.safe_load(file)
    for k in key_path:
        obj=obj[k]
    return obj


#
# MAIN
#
cli.add_command(upload)
cli.add_command(info)
if __name__ == "__main__":
    cli()