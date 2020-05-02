import re
import math
from datetime import datetime
from unidecode import unidecode
import geojson
import mproc
import ee.data
from ee.cli.utils import wait_for_task
#
# CONFIG
# 
NB_BATCHES=6
DOT='d'
TIMEOUT=5*60
#
# CONSTANTS
#
NAME_PREFIX="projects/earthengine-legacy/assets"
GCS_PREFIX='gs://'
GCS_URL_ROOT_REGX=r'^(https|http)://storage.(googleapis|cloud.google).com/'
# MESSAGES
WARNING_SPECIFY_COLLECTION=(
	"No collection set. Use `collection=False`"
	"to upload image(s) to project root" )


# GEE INTERNAL
TASK_TYPES = {
    'EXPORT_FEATURES': 'Export.table',
    'EXPORT_IMAGE': 'Export.image',
    'EXPORT_TILES': 'Export.map',
    'EXPORT_VIDEO': 'Export.video',
    'INGEST': 'Upload',
    'INGEST_IMAGE': 'Upload',
    'INGEST_TABLE': 'Upload',
}



#
# HELPERS
#
def _format_time(millis):
	return datetime.fromtimestamp(millis / 1000)



#
# MAIN
#
class EEImagesUp(object):
	
	@staticmethod
	def read_geojson(path,*key_path):
		""" read geojson
		Args: 
			- path<str>: path to geojson file
			- args(key_path<str|int>):
				an ordered series of dict-keys/list-indices

				Ex: 
					* read_geojson('path.geojson','features') 
					  returns the feature list
					* read_geojson('path.geojson','features',0,'properties') 
					  returns the properties of the first feature

		"""
		with open(path,'r') as file:
			gjsn=geojson.load(file)
		for k in key_path:
			gjsn=gjsn[k]
		return gjsn
	

	@staticmethod
	def task_info(task_id):
		""" print task info (copied from ee.cli)
		Args:
			- task_id<str|dict>:
				* <str> gee task id
				* <dict> must contain id=<TASK_ID> key-value pair
		"""
		if isinstance(task_id,dict):
			task_id=task_id['id']
		for i, status in enumerate(ee.data.getTaskStatus(task_id)):
			if i:
				print()
			print('%s:' % status['id'])
			print('  State: %s' % status['state'])
			if status['state'] == 'UNKNOWN':
				continue
			print('  Type: %s' % TASK_TYPES.get(status.get('task_type'), 'Unknown'))
			print('  Description: %s' % status.get('description'))
			print('  Created: %s'
						% _format_time(status['creation_timestamp_ms']))
			if 'start_timestamp_ms' in status:
				print('  Started: %s' % _format_time(status['start_timestamp_ms']))
			if 'update_timestamp_ms' in status:
				print('  Updated: %s'
							% _format_time(status['update_timestamp_ms']))
			if 'error_message' in status:
				print('  Error: %s' % status['error_message'])
			if 'destination_uris' in status:
				print('  Destination URIs: %s' % ', '.join(status['destination_uris']))

	
	#
	# PUBLIC
	#
	def __init__(
			self,
			user,
			features={},
			collection=None,
			bands=None,
			pyramiding_policy=None,
			no_data=None,
			force=False,
			include=None,
			start_time_key='start_time',
			end_time_key='end_time',
			days_delta=1,
			crs_key='crs',
			uri_key='gcs',
			name_key='ee_name',
			timeout=TIMEOUT):
		self._set_destination(user,collection)
		self._set_features(features)
		self.bands=self._bands(bands)        
		self.pyramiding_policy=self._pyramiding_policy(pyramiding_policy)
		self.no_data=self._no_data(no_data)
		self.force=force
		self.crs_key=crs_key
		self.uri_key=uri_key
		self.name_key=name_key
		self.include=include
		self.start_time_key=start_time_key
		self.end_time_key=end_time_key
		self.days_delta=days_delta
		self.timeout=timeout
		

		
	def manifest(self,
				 feat={},
				 uri=None,
				 name=None,
				 ident=None,
				 crs=None,
				 properties={},
				 start_time=None,
				 end_time=None):
		feat=self._feature(feat)
		fprops=feat.get('properties',{})
		uri=self._uri(uri or fprops[self.uri_key])
		name=self._name(uri,name or fprops.get(self.name_key))
		crs=crs or fprops.get(self.crs_key)
		tilesets=self._tilesets(uri,crs,ident,name)
		properties=self._clean_properties(fprops,properties)
		tstart,tend=self._start_end_time(
			start_time or fprops.get(self.start_time_key),
			end_time or fprops.get(self.end_time_key))
		mfest={
			"name": name,
			"tilesets": tilesets,
			"properties": properties,
			"start_time": tstart,
			"end_time": tend
		}
		return self._add_group_properties(mfest)

	
	def upload(
			self,
			feat={},
			uri=None,
			name=None,
			ident=None,
			crs=None,
			properties={},
			start_time=None,
			end_time=None,
			manifest=None,
			wait=False):
		if not manifest:
			manifest=self.manifest(
				feat=feat,
				uri=uri,
				name=name,
				ident=ident,
				crs=crs,
				properties=properties,
				start_time=start_time,
				end_time=end_time)
		resp=ee.data.startIngestion(
			ee.data.newTaskId()[0], 
			manifest, 
			self.force)
		task_id=resp['id']
		if wait:
			wait_for_task(task_id, self.timeout)
		return resp

	
	def upload_collection(self,features=None,limit=None,nb_batches=NB_BATCHES):
		feats=features or self.features
		if limit:
			feats=feats[:limit]
		total=len(feats)
		bs=int(math.ceil(total//nb_batches))
		nb_batches=int((total+bs-1)//bs)
		batches=[feats[b*bs:(b + 1)*bs] for b in range(nb_batches)]  
		out=mproc.map_with_threadpool(self._upload_batch,batches,max_processes=nb_batches)

		
	def _upload_batch(self,feats):
		print('UB',len(feats))
		return [self.upload(f,wait=True) for f in feats]
		
		
	#
	# INTERNAL
	#
	def _set_destination(self,user,collection):
		if collection is None:
			raise ValueError(WARNING_SPECIFY_COLLECTION)
		self.user=user
		self.collection=collection
		self._path_parts=[NAME_PREFIX,user]
		if collection:
			self._path_parts.append(collection)

			
	def _set_features(self,features):
		if isinstance(features,str):
			features=EEImagesUp.read_geojson(features,'features')
		self.features=features

		
	def _feature(self,feat):
		if isinstance(feat,int):
			feat=self.features[feat]
		return feat
		
		
	def _uri(self,uri):
		uri=re.sub(GCS_URL_ROOT_REGX,'',uri)
		if not re.search(f'^{GCS_PREFIX}',uri):
			uri=f'{GCS_PREFIX}{uri}'
		return uri
	
	
	def _name(self,uri,name):
		if not name:
			name=self._uri_to_name(uri)
		name=re.sub('\.',DOT,name)
		for p in self._path_parts:
			name=re.sub(f'^{p}/','',name)
		name="/".join(self._path_parts+[name])
		return name

		
	def _uri_to_name(self,uri):
		name=uri.split('/')[-1]
		parts=name.split('.')
		if len(parts)>1:
			name=".".join(parts[:-1])
		return name
		
	
	def _tilesets(self,uri,crs,ident,name):
		if not ident:
			ident=name.split('/')[-1][:99]
		tset={
			"id": ident,
			"sources": [{ "uris": uri }]}
		if crs:
			tset['crs']=crs
		return  [tset]
		
		
	def _clean_properties(self,feat_props,props):
		cprops=feat_props.copy()
		cprops.update(props.copy())
		if self.include:
			cprops={ k:cprops[k] for k in self.include }
		return { re.sub(' ','',k): self._clean_value(v) 
				 for k,v in cprops.items() }

		
	def _clean_value(self,value):
		if isinstance(value,(str)):
			value=unidecode(value)
		return str(value)
		
		
	def _start_end_time(self,start_time,end_time):
		pass
		return None, None
	
	
	def _bands(self,bands):
		pass
	
	
	def _pyramiding_policy(self,policy):
		pass
	
	
	def _no_data(self,no_data):
		pass
	
	
	def _add(self,name,obj,data):
		if obj is not None:
			data[name]=obj
		return data
	
	def _add_group_properties(self,manifest):
		manifest=self._add('bands',self.bands,manifest)
		manifest=self._add('pyramiding_policy',self.pyramiding_policy,manifest)
		manifest=self._add('no_data',self.no_data,manifest)
		return manifest