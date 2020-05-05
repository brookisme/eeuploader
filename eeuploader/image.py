import ee
ee.Initialize()
import re
import math
from datetime import datetime, timedelta
from unidecode import unidecode
import json
import geojson
import mproc
import ee.data
import ee.ee_exception
from itertools import chain
from . import gee_utils as gutils
from . import utils
#
# CONFIG
# 
NB_BATCHES=50
TIMEOUT=5*60
PICKLE='pickle'
JSON='json'
#
# CONSTANTS
#
GCS_PREFIX='gs://'
GCS_URL_ROOT_REGX=r'^(https|http)://storage.(googleapis|cloud.google).com/'
PP_VALUES=[
	"MEAN",
	"MODE",
	"SAMPLE" ]
DATE_FMT='%Y-%m-%d'
# MESSAGES
WARNING_SPECIFY_COLLECTION=(
	"No collection set. Use `collection=False`"
	"to upload image(s) to project root" )
ERROR_PP=(
	f"pyramiding_policy must be None or one of {str(PP_VALUES)}" )



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
	#
	# PUBLIC
	#
	def __init__(
			self,
			user,
			features={},
			collection=None,
			bands=None,
			band_names=None,
			pyramiding_policy=None,
			no_data=None,
			include=None,
			exclude=None,
			start_time_key='start_time',
			end_time_key='end_time',
			days_delta=1,
			crs_key='crs',
			uri_key='gcs',
			name_key='ee_name',
			skip_existing=True,
			force=False,
			timeout=TIMEOUT,
			noisy=False,
			raise_error=False):
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
			skip_existing<bool>:
				if true skip uploads for existing assets
			force<bool>:
				set to true to overwrite existing assets
			noisy<bool>:
				print progress during `upload_collection`
			raise_error<bool>:
				raise_errors during `upload_collection`

		Usage:

			import eeuploader.image as eup
			import eeuploader.gee_utils as gutils

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
			gutils.task_info(up.task_id)


			# upload the first 3 features / print task final task status for each
			up.upload_collection(limit=3)
			print(up.tasks)
		
		"""
		self._set_destination(user,collection)
		self._set_features(features)
		self._set_existing(skip_existing)
		self.band_names=band_names  
		self.bands=bands    
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
			tileset_id=None,
			crs=None,
			properties={},
			start_time=None,
			end_time=None,
			features=False,
			dest=None,
			save_as=PICKLE,
			indent=None):
		""" manifest for single upload or manifests list

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
			features<list>:
				list of features or feature-indices. if exists the returned manifest
				will be a list of upload manifests
			dest<str>:
				if dest: manifest will be saved and dest will be returned
				otherwise: manifest will be returned
			save_as:
				* file-type: one of ['json', 'pickle']
				* defaults to 'pickle'
			indent<int|None>:
				if saving to json: indent pretty printing arg.
		
		Returns<str|dict>:
			
			* <dict> Manifest for a single upload
			* <str> Destination of saved file
		"""
		if features:
			if features is True:
				features=self.features
			manifest=[]
			for feat in features:
				manifest.append(self._feature_manifest(
					feat=feat,
					uri=uri,
					name=name,
					tileset_id=tileset_id,
					crs=crs,
					properties=properties,
					start_time=start_time,
					end_time=end_time))
		else:
			manifest=self._feature_manifest(
				feat=feat,
				uri=uri,
				name=name,
				tileset_id=tileset_id,
				crs=crs,
				properties=properties,
				start_time=start_time,
				end_time=end_time)
		if dest:
			if save_as==JSON:
				utils.save_json(manifest,dest,indent=indent)
			else:
				utils.save_pickle(manifest,dest)
			return dest
		else:
			return manifest


	def upload(
			self,
			feat={},
			uri=None,
			name=None,
			tileset_id=None,
			crs=None,
			properties={},
			start_time=None,
			end_time=None,
			manifest=None,
			wait=False,
			noisy=True, 
			raise_error=None):
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
		if not manifest:
			manifest=self.manifest(
				feat=feat,
				uri=uri,
				name=name,
				tileset_id=tileset_id,
				crs=crs,
				properties=properties,
				start_time=start_time,
				end_time=end_time)
		if self._check_existing(manifest):
			resp={
				'WARNING': f'Asset {manifest["name"]} exists. Upload Skipped',
				'manifest': manifest }
		else:
			resp=ee.data.startIngestion(
				ee.data.newTaskId()[0], 
				manifest, 
				self.force)
			task_id=resp['id']
			if wait:
				resp=gutils.wait(
					task_id,
					self.timeout,
					noisy=noisy,
					raise_error=raise_error)
				if resp and isinstance(resp,list):
					resp=resp[0]
			self.task_id=task_id
			self.task=resp
		return resp

	
	def upload_collection(self,features=None,limit=None,nb_batches=NB_BATCHES):
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
		self.user=gutils.asset_id(user,prefix=False)
		self.collection=collection

			
	def _set_features(self,features):
		if isinstance(features,str):
			features=utils.read_json(features,'features')
		elif isinstance(features,(dict)):
			features=features['features']
		self.features=features


	def _set_existing(self,skip_existing):
		self.skip_existing=skip_existing
		if skip_existing:
			self.existing_assets=gutils.assets(
				self.user,
				collection=self.collection,
				strip_prefix=False)
		else:
			self.existing_assets=False


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
		return gutils.asset_id(
			self.user,
			collection=self.collection,
			name=name,
			prefix=True,
			safe=True)

		
	def _uri_to_name(self,uri):
		name=uri.split('/')[-1]
		parts=name.split('.')
		if len(parts)>1:
			name=".".join(parts[:-1])
		return name
		

	def _check_existing(self,name):
		if isinstance(name,dict):
			name=name['name']
		if self.skip_existing:
			return name in self.existing_assets


	def _tileset_id(self,tileset_id,name):
		if not tileset_id:
			tileset_id=name.split('/')[-1][:99]
		return tileset_id

	
	def _tilesets(self,uri,crs,tileset_id):
		tset={
			"id": tileset_id,
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
		if start_time:
			if isinstance(start_time,str):
				start_time=datetime.strptime(start_time,DATE_FMT)
			if end_time:
				if isinstance(start_time,str):
					end_time=datetime.strptime(end_time,DATE_FMT)
			elif self.days_delta:
				end_time=start_time+timedelta(days=self.days_delta)
		elif end_time:
			end_time=datetime.strptime(end_time,DATE_FMT)
		return self._timestamp(start_time), self._timestamp(end_time)


	def _timestamp(self,dtime):
		if dtime:
			return { "seconds": int(dtime.timestamp()) }


	def _bands(self,tileset_id):
		if self.bands:
			return self.bands
		elif self.band_names:
			return [ self._band(n,tileset_id,i) for i,n in enumerate(self.band_names) ]


	def _band(self,name,tileset_id,index):
		return {
			'id': name,
			'tileset_id': tileset_id,
			'tileset_band_index': index }

	
	def _pyramiding_policy(self,policy):
		if policy:
			policy=policy.upper()
			if policy not in PP_VALUES:
				raise ERROR_PP
			return policy
	
	
	def _no_data(self,no_data):
		if no_data is not None:
			if isinstance(no_data,(int,float)):
				no_data=[no_data]
			if isinstance(no_data,list):
				no_data={ "values": no_data }
			return no_data


	def _add(self,name,obj,data):
		if obj is not None:
			data[name]=obj
		return data
	

	def _feature_manifest(
			self,
			feat={},
			uri=None,
			name=None,
			tileset_id=None,
			crs=None,
			properties={},
			start_time=None,
			end_time=None):
		feat=self._feature(feat)
		fprops=feat.get('properties',{})
		uri=self._uri(uri or fprops[self.uri_key])
		name=self._name(uri,name or fprops.get(self.name_key))
		tileset_id=self._tileset_id(tileset_id,name)
		crs=crs or fprops.get(self.crs_key)
		tilesets=self._tilesets(uri,crs,tileset_id)
		bands=self._bands(tileset_id)
		properties=self._clean_properties(fprops,properties)
		start_time,end_time=self._start_end_time(
			start_time or fprops.get(self.start_time_key),
			end_time or fprops.get(self.end_time_key))
		return self._build_manifest(name,tilesets,properties,bands,start_time,end_time)


	def _build_manifest(self,name,tilesets,properties,bands,start_time,end_time):
		manifest={
			"name": name,
			"tilesets": tilesets
		}
		manifest=self._add('properties',properties,manifest)
		manifest=self._add('bands',bands,manifest)
		manifest=self._add('start_time',start_time,manifest)
		manifest=self._add('end_time',end_time,manifest)
		manifest=self._add('pyramiding_policy',self.pyramiding_policy,manifest)
		manifest=self._add('missing_data',self.no_data,manifest)
		return manifest


