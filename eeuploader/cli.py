from __future__ import print_function
import os,sys
sys.path.append(os.environ.get('PROJECT_DIR','..'))
import re
import click

#
# DEFAULTS
#
UPLOAD_HELP='upload feature collection'
RANGE_HELP='restrict to index range'
LIMIT_HELP='limit to first N'
INDICES='comma separated feature index list'
NOISY_HELP='be noisy'
INFO_HELP='print number of features and manifiest for first feature'
ARG_KWARGS_SETTINGS={
    'ignore_unknown_options': True,
    'allow_extra_args': True
}




#
# CLI INTERFACE
#
@click.group()
@click.pass_context
def cli(ctx):
    ctx.obj={}


@click.command(help=UPLOAD_HELP) 
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
    help=NOISE_HELP,
    default=NOISY,
    type=bool)
@click.pass_context
def upload(ctx,feature_collection,range,indices,limit,noisy):
    pass



@click.command(help=INFO_HELP) 
@click.argument('feature_collection',type=str)
@click.option(
    '--index',
    help=INDEX_HELP,
    default=INDEX,
    type=int)
@click.pass_context
def info(ctx,feature_collection,index):
    pass



#
# MAIN
#
cli.add_command(upload)
cli.add_command(info)
if __name__ == "__main__":
    cli()