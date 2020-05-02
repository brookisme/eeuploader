import re
import math
import time
from datetime import datetime
from unidecode import unidecode
import geojson
import mproc
import ee.data
import ee.ee_exception
from itertools import chain
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
    'INGEST_TABLE': 'Upload' }
TASK_FINISHED_STATES=[
	'COMPLETED',
	'FAILED',
	'CANCELLED' ]



#
# HELPERS
#
def _format_time(millis):
	return datetime.fromtimestamp(millis / 1000)


def _flatten(lists):
	return list(chain.from_iterable(lists))


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

	
	@staticmethod
	def wait(task_id, timeout, noisy=True, raise_error=False):
		""" modified ee.cli.utils.wait_for_task 
			* silent mode
			* optional raise error
			* return final task status
		"""
		start = time.time()
		elapsed = 0
		last_check = 0
		while True:
			elapsed = time.time() - start
			status = ee.data.getTaskStatus(task_id)[0]
			state = status['state']
			if state in TASK_FINISHED_STATES:
				error_message = status.get('error_message', None)
				if noisy: 
					print('Task %s ended at state: %s after %.2f seconds'
							% (task_id, state, elapsed))
				if raise_error and error_message:
					raise ee.ee_exception.EEException('Error: %s' % error_message)
				return status
			remaining = timeout - elapsed
			if remaining > 0:
				time.sleep(min(10, remaining))
			else:
				break
		timeout_msg='Wait for task %s timed out after %.2f seconds' % (task_id, elapsed)
		status['TIMEOUT']=timeout_msg
		if noisy:
			print(timeout_msg)
		return status


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
			exclude=None,
			start_time_key='start_time',
			end_time_key='end_time',
			days_delta=1,
			crs_key='crs',
			uri_key='gcs',
			name_key='ee_name',
			timeout=TIMEOUT,
			noisy=False, 
			raise_error=False):
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
		self.exclude=exclude
		self.start_time_key=start_time_key
		self.end_time_key=end_time_key
		self.days_delta=days_delta
		self.timeout=timeout
		self.noisy=noisy
		self.raise_error=raise_error
		

		
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
			wait=False,
			noisy=True, 
			raise_error=None):
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
			resp=EEImagesUp.wait(task_id, self.timeout, noisy=noisy, raise_error=raise_error)
			if resp and isinstance(resp,list):
				resp=resp[0]
		self.task_id=task_id
		self.task=resp
		return resp

	
	def upload_collection(self,features=None,limit=None,nb_batches=NB_BATCHES):
		feats=features or self.features
		if limit:
			feats=feats[:limit]
		total=len(feats)
		bs=int(math.ceil(total/nb_batches))
		nb_batches=int((total+bs-1)//bs)
		batches=[feats[b*bs:(b + 1)*bs] for b in range(nb_batches)]  
		tasks=mproc.map_with_threadpool(self._upload_batch,batches,max_processes=nb_batches)
		self.tasks=_flatten(tasks)


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


	def _upload_batch(self,feats):
		return [ self._upload_feat(f) for f in feats]


	def _upload_feat(self,feat):
		return self.upload(feat,wait=True,noisy=self.noisy,raise_error=self.raise_error) 


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
		elif self.exclude:
			for k in self.exclude:
				cprops.pop(k,None)
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