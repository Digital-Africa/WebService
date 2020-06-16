# -*- coding: utf-8 -*

#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# [START imports]

import os
import urllib
import webapp2
import jinja2
import json
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
from google.cloud import bigquery
import itertools
from google.appengine.api import taskqueue
from google.appengine.api import app_identity
from google.appengine.api import users
import csv
import logging
from time import gmtime, strftime
import cloudstorage as gcs
import datetime

JINJA_ENVIRONMENT = jinja2.Environment(
	loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
	extensions=['jinja2.ext.autoescape'],
	autoescape=True)


class Context(object):
	def __init__(self):
		
		self.creds_path = 'creds/orange-user-access-2ebad1f50605.json'
		self.scope = ['https://www.googleapis.com/auth/tagmanager.manage.users','https://www.googleapis.com/auth/tagmanager.edit.containers','https://www.googleapis.com/auth/analytics.manage.users','https://www.googleapis.com/auth/analytics','https://www.googleapis.com/auth/analytics.edit','https://www.googleapis.com/auth/analytics.readonly']
		self.project_id = 'orange-user-access'
		self.bucket = 'user-acces-data'
		self.dataset_id = 'user_access_orange'
		self.raw_upload = '{}/raw/raw_upload'.format(self.bucket)
		self.table_id_raw = '0_custom'
		self.table_id_raw_analytics = '0_custom_data_analytics'
		self.table_id_raw_tagmanager = '0_custom_data_tagmanager'
		self.table_id_raw_container = '0_custom_data_container'
		self.table_id_analytics = '1_analytics'
		self.table_id_tagmanager = '1_tagmanager'
		self.table_id_container = '1_container'
		self.version = 'User Access V3.0'

	def context(self):

		context = {
			'global': {'creds_path':self.creds_path,
						'scope':self.scope,
						'dataset_id':self.dataset_id,
						'table_id_raw': self.table_id_raw,
						'path': '/{}/raw'.format(self.bucket)
						},	
			'analytics': {'location': '{}/raw/data_analytics'.format(self.bucket),
						'table_id_raw':{'id': self.table_id_raw_analytics, 'schema':['email', 'AccountName', 'level','levelname','permissions','ReferentEmail','Status','YYYY','MM']},
						'table_id':{'id': self.table_id_analytics, 'keygen':'key_gen_analytics'},
						'interface':{'id': 'interface', 'schema':['account_id','level','email','permissions_level','permissions','level_name','key']},
							},
			'tagmanager': {'location': '{}/raw/data_tagmanager'.format(self.bucket),
						'table_id_raw':{'id': self.table_id_raw_tagmanager, 'schema':['Email','AccountName','AccountLevelPermissions','ContainerName','ContainerLevelPermissions','ReferentEmail','Status','ExpirationYear','ExpirationMonth']},
						'table_id':{'id': self.table_id_tagmanager, 'keygen':'key_gen_gtm'},
						'interface':{'id': 'interface_tagmanager', 'keygen':'tagmanager_interface'},
							},
			'container': {'location': '{}/raw/data_container'.format(self.bucket),
							'table_id_raw':{'id': self.table_id_raw_container, 'schema':['Email','AccountName','container_name', 'Permissions','ReferentEmail','Status','YYYY','MM']},
							'table_id':{'id': self.table_id_container, 'keygen':'key_gen_container'},
							},
			}
		return context

	def update_context(self,context,key,val):
		context[key] = val
		return context

def get_service():
	
	from googleapiclient.errors import HttpError
	from googleapiclient.discovery import build
	from oauth2client.service_account import ServiceAccountCredentials
	import httplib2

	context = Context().context()
	key_file_location = context['global']['creds_path']
	scopes = context['global']['scope']
	credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file_location, scopes=scopes)
	http = credentials.authorize(httplib2.Http())
	credentials = credentials.refresh(http)

	service_analytics = build('analytics', 'v3', http=http)
	service_tagmanager = build('tagmanager', 'v2', http=http)
	
	return service_analytics, service_tagmanager

def create_correspondance(): 
	service_analytics, service_tagmanager = get_service()
	summary = service_analytics.management().accountSummaries().list().execute()['items']
	list_view = [[i['id'],i['name'], j['id'],j['name']] for i in summary for j in i['webProperties']]
	lookup_analytics = [[i['id'],i['name'].encode('utf-8')] for i in summary]+[[j['id'],j['name'].encode('utf-8')] for i in summary for j in i['webProperties']]
	return list_view, lookup_analytics

def open_query(step):
	with open('data/{}.sql'.format(step), 'r') as q:
		a = q.read()
	return a

def get_bigquery_data(options):
	client = bigquery.Client(project = 'the-lab-dl')
	query_object = {}
	query_object['identifier'] = open_query('identifier')
	query_object['email'] = open_query('email')
	query_object['init'] = open_query('benoit')
	query_object['email_tagmanager'] = open_query('email_tagmanager')
	query_object['email_container'] = open_query('email_container')
	query_object['identifier_tagmanager'] = open_query('identifier_tagmanager')
	query_object['identifier_container'] = open_query('identifier_container')
	query_object['identifier_gtm'] = open_query('identifier_gtm')
	query_object['identifier_gtm_total'] = open_query('identifier_gtm_total')	
	query_object['email_gtm'] = open_query('email_gtm')
	query_object['correspondance_gtm'] = open_query('correspondance_gtm')

	job_config = bigquery.QueryJobConfig() 
	job_config.use_legacy_sql = True

	type = options['type']
	query = query_object[type]
	if type == 'email':
		query = query.format(options['data'])
		query_job = client.query(query, job_config= job_config)
		results = query_job.result()
	elif type == 'init':	
		query_job = client.query(query, job_config= job_config)
		results = query_job.result()
	elif type == 'identifier':	
		query = query.format(options['data'], options['data'],options['data'])
		query_job = client.query(query, job_config= job_config)
		results = query_job.result()		
	elif type == 'email_tagmanager':	
		query_job = client.query(query.format(options['data']), job_config= job_config)
		results = query_job.result()	
	elif type == 'identifier_tagmanager':	
		query_job = client.query(query.format(options['data']), job_config= job_config)
		results = query_job.result()
	elif type == 'email_container':	
		query_job = client.query(query.format(options['data']), job_config= job_config)
		results = query_job.result()	
	elif type == 'identifier_container':	
		query_job = client.query(query.format(options['data']), job_config= job_config)
		results = query_job.result()	
	elif type == 'identifier_gtm':	
		query_job = client.query(query.format(options['data'][0],options['data'][1]), job_config= job_config)
		results = query_job.result()
	elif type == 'identifier_gtm_total':
		query_job = client.query(query.format(options['data']), job_config= job_config)
		results = query_job.result()			
	elif type == 'email_gtm':	
		query_job = client.query(query.format(options['data']), job_config= job_config)
		results = query_job.result()	
	elif type == 'correspondance_gtm':	
		query_job = client.query(query, job_config = job_config)
		results = query_job.result()	
	return results

def at_least_one_property(summary_):
	summary = []
	ind = []
	for i in range(0,5):
		for j in range(0,len(summary_[i]['webProperties'])):
			try:
				summary_[i]['webProperties'][j]['profiles']
				summary.append(summary_[i])
			except:
				ind.append((i,j))
	return summary

def store_to_gcs(data):
	
	import csv
	context = Context().context()
	bucket_path = '/{}/'.format(context["location"])
	gcs_file = gcs.open(bucket_path+'data_analytics.csv', 'w')
	writer = csv.writer(gcs_file, delimiter=',')
	
	for element in data:
		writer.writerow(element)
	gcs_file.close()



def listing_properties_by_account(account):

	data = {'account' : account,
	'property' : ''}
	service_analytics, service_tagmanager = get_service()
	summary = service_analytics.management().accountSummaries().list().execute()['items']
	list_view = [[i['id'],i['name'], j['id'],j['name']] for i in summary for j in i['webProperties']]
	
	if data['property'] == '':

		output =  [[property_id,property_name] for account_id,account_name, property_id,property_name in list_view if account_id == data['account']]
	
	else:

		output =  [[account_id,account_name] for account_id,account_name, property_id,property_name in list_view if property_id == data['property']][0]
	
	return output

def get_random_name(output,element):
	return filter(lambda x: x[0] == element ,output)[0][1]

def get_property_name(output,element):
	return filter(lambda x: x[2] == element ,output)[0][3]

def get_account_name(output,element):
	return filter(lambda x: x[0] == str(element) ,output)[0][1]

def nice_print_permissions(element):
	return ','.join([element_ for element_ in element.split(',') if element_ != ''])

def isFullPropertyLevel(collaborate,edit,manage_users,read_n_nanalyze):
	def rename_collaborate(element):
		if element:
			return 'collaborate'
	def rename_edit(element):
		if element:
			return 'edit'	
	def rename_manage_users(element):
		if element:
			return 'manage_users'
	def rename_read_n_analyze(element):
		if element:
			return 'read_and_analyze'
	permissions = [x for x in [rename_collaborate(collaborate),rename_edit(edit),rename_manage_users(manage_users),rename_read_n_analyze(read_n_nanalyze)] if x is not None]

	return '|'.join(permissions)

def isFullPropertyLevel(point):
	if point:
		return True
	else:
		return False

def write_correspondance():

	output, _ = create_correspondance()
	with open('correspondance.csv','wb') as file:

		correspondance = csv.writer(file, delimiter = ',')
		
		for element in output: 
			correspondance.writerow(element)

	return correspondance

def timestamp():
	from time import gmtime, strftime
	time_value = strftime("%Y-%m-%d %H:%M:%S", gmtime())	
	return time_value

class LandingPage(webapp2.RequestHandler):

	def get(self):
		user = users.get_current_user()
		if user:
			nickname = user.nickname()
			logout_url = users.create_logout_url('/')

		template_values = {'version':Context().version, 'nickname':nickname}
		template = JINJA_ENVIRONMENT.get_template('home.html')
		self.response.write(template.render(template_values))

class AnalyticsSummary(webapp2.RequestHandler):

	def get(self):

		account = self.request.get('account')
		level = self.request.get('level')
		correspondance, _ = create_correspondance()
		list_account = list(set((element[1], element[0]) for element in correspondance if str(element[0]) != '108488354'))
		list_property = list(set((element[3], element[2]) for element in correspondance if str(element[0]) == str(account)))

		user = users.get_current_user()
		if user:
			nickname = user.nickname()
			logout_url = users.create_logout_url('/')

		template_values = {
		'version':Context().version,
		'nickname':nickname,
		'logout_url':logout_url,
		'account_selected': account,
		'screen_name': 'Analytics by identifiers',
		'proplist': [['Select a Property', 'Select a Property']] + [[e[0], e[1]] for e in sorted(list_property, key = lambda n: n[0])],
		'acclist':  [[element[0],element[1]] for element in list_account if str(element[1]) == str(account)] + [[e[0], e[1]] for e in sorted(list_account, key = lambda n: n[0])]
			}
			
		template = JINJA_ENVIRONMENT.get_template('identifier.html')
		self.response.write(template.render(template_values))	

class LandingPageAnalytics(webapp2.RequestHandler):

	def get(self):
		correspondance,_ = create_correspondance()
		list_account = list(set((element[1], element[0]) for element in correspondance if element[0] != '108488354'))
		user = users.get_current_user()
		if user:
			nickname = user.nickname()
			logout_url = users.create_logout_url('/')

		template_values = {
		'version':Context().version,
		'nickname':nickname,
		'logout_url':logout_url,
		'screen_name': 'Analytics by identifiers',
		'acclist': [['Choose an account','Choose an account']] + sorted(list_account, key = lambda n: n[0]),
		'proplist': [['---------','---------']]
				}
		template = JINJA_ENVIRONMENT.get_template('identifier.html')
		self.response.write(template.render(template_values))

class Endpoints(webapp2.RequestHandler):

	def get(self):

		def once_name(element):
			return filter(lambda x: x[0] == element)[1]

		def set_permission(element):
			permissions = set(element.split('|'))
			return ','.join(permissions)

		def manage_expiration(element,element_):
			return '--'

		def screen(typ):
			if typ == 'email':
				correspondance, lookup = create_correspondance()

				options = {'type': 'email', 'data': individu}
				data = [[element['email'],get_account_name(correspondance,element['account_id']),element['permissions_level'],element['level_name'],nice_print_permissions(element['permissions']),element['referents'], manage_expiration(element['YYYY'],element['MM']),element['YYYY'],element['MM']] for element in get_bigquery_data(options)]
				
				template_values = {
				'email':individu,
				'gadatalist': data,
				'screen_name':'Google Analytics by email'
				}
				template = JINJA_ENVIRONMENT.get_template('email.html')
				return template, template_values

			elif typ == 'identifier':
				correspondance, lookup = create_correspondance()
				options =  {'data' : property, 'type':'identifier'}
				data = [[element['email'],get_account_name(correspondance,element['account_id']),element['permissions_level'],element['level_name'],nice_print_permissions(element['permissions']),element['referents'], manage_expiration(element['YYYY'],element['MM']),element['YYYY'],element['MM']] for element in get_bigquery_data(options)]
						
				list_account = list(set((element[1], element[0]) for element in correspondance if element[0] != '108488354'))
				list_property = list(set((element[3], element[2]) for element in correspondance if element[0] == str(account)))
				
				template_values = {
					'correspondance': correspondance,
					'gadatalist': data,
					'account_selected': account,
					'property_selected': property,
					'screen_name':'Google Analytics by identifier',
					'proplist': [[element[0],element[1]] for element in list_property if str(element[1]) == str(property)] + sorted(list_property, key = lambda n: n[0]),
					'acclist':  [[element[0],element[1]] for element in list_account if str(element[1]) == str(account)] + sorted(list_account, key = lambda n: n[0])
				}
				template = JINJA_ENVIRONMENT.get_template('identifier.html')
				return template, template_values

			elif typ == 'email_tagmanager':
				options = {'type': 'email_tagmanager', 'data': individu}
				data = [[element['emailAddress'],element['account_name'], nice_print_permissions(element['account_level_permissions']),element['ReferentEmail'] , manage_expiration(element['YYYY'],element['MM']),element['YYYY'],element['MM']] for element in get_bigquery_data(options)]
				
				template_values = {
				'email':individu,
				'gadatalist': data,
				'screen_name':'Google Tagmanager Manager by email'
				}
				template = JINJA_ENVIRONMENT.get_template('email_tagmanager.html')
				return template, template_values

			elif typ == 'identifier_tagmanager':
				options = {'type':'correspondance_gtm'}
				gtm_account_list = [[element['accountId'], element['name']] for element in get_bigquery_data(options)]
				if gtm_account in [str(accountId) for accountId, accountname in gtm_account_list]:	
					options = {'type': 'identifier_tagmanager', 'data': gtm_account}
					data = [[element['emailAddress'],element['account_name'], nice_print_permissions(element['account_level_permissions']),element['ReferentEmail'] , manage_expiration(element['YYYY'],element['MM']),element['YYYY'],element['MM']] for element in get_bigquery_data(options)]
					acclist = [[element[0],element[1]] for element in gtm_account_list if str(element[0]) == str(gtm_account)] + sorted(gtm_account_list, key = lambda n: n[0])
					
					template_values = {
					'account_selected':gtm_account,
					'acclist':acclist,
					'gadatalist': data,
					'screen_name':'Google Tagmanag Manager by identifier'
					}
				else:
					acclist = [['Choose an account','Choose an account']] + sorted(gtm_account_list, key = lambda n: n[0])
					template_values = {
					'account_selected':gtm_account,
					'acclist':acclist,
					'gtm_account_list': [accountId for accountId, accountname in gtm_account_list],
					'screen_name':'Google Tagmanag Manager by identifier'
					}
				template = JINJA_ENVIRONMENT.get_template('identifier_tagmanager.html')
				return template, template_values

			elif typ == 'email_container':
				options = {'type': 'email_container', 'data': individu}
				data = [[element['emailAddress'],element['account_name'], element['container_name'], nice_print_permissions(element['container_level_permissions']),element['ReferentEmail'] , manage_expiration(element['YYYY'],element['MM']),element['YYYY'],element['MM']] for element in get_bigquery_data(options)]
				
				template_values = {
				'email':individu,
				'gadatalist': data,
				'screen_name':'GTM Containers by email'
				}

				template = JINJA_ENVIRONMENT.get_template('email_container.html')
				return template, template_values

			elif typ == 'identifier_container':
				options = {'type':'correspondance_gtm'}
				gtm_account_list = [[element['accountId'], element['name']] for element in get_bigquery_data(options)]
				if gtm_account in [str(accountId) for accountId, accountname in gtm_account_list]:	
					options = {'type': 'identifier_container', 'data': gtm_account}
					data = [[element['emailAddress'],element['account_name'], element['container_name'], nice_print_permissions(element['container_level_permissions']),element['ReferentEmail'] , manage_expiration(element['YYYY'],element['MM']),element['YYYY'],element['MM']] for element in get_bigquery_data(options)]
					acclist = [[element[0],element[1]] for element in gtm_account_list if str(element[0]) == str(gtm_account)] + sorted(gtm_account_list, key = lambda n: n[0])
					
					template_values = {
					'account_selected':gtm_account,
					'acclist':acclist,
					'gadatalist': data,
					'screen_name':'GTM Containers by identifier'
					}
				else:
					acclist = [['Choose an account','Choose an account']] + sorted(gtm_account_list, key = lambda n: n[0])
					
					template_values = {
					'account_selected':gtm_account,
					'acclist':acclist,
					'gtm_account_list': [accountId for accountId, accountname in gtm_account_list]	,
					}
				template_values['screen_name'] = 'GTM Containers by identifier'
				template = JINJA_ENVIRONMENT.get_template('identifier_container.html')
				return template, template_values

			elif typ == 'identifier_gtm':
				options = {'type':'correspondance_gtm'}
				gtm_correspondance = [[element['accountId'], element['name'],element['container_Id'], element['container_name']] for element in get_bigquery_data(options)]
				gtm_account_list = set((accountId,name) for accountId,name,container_Id,container_name in gtm_correspondance)
				gtm_container_list= set((container_Id,container_name) for accountId,name,container_Id,container_name in gtm_correspondance if accountId == gtm_account)
				if gtm_account in [str(accountId) for accountId, accountname, container_Id, container_name in gtm_correspondance] and gtm_container in [str(container_Id) for accountId, accountname, container_Id, container_name in gtm_correspondance]:
					options = {'type': 'identifier_gtm', 'data': (gtm_account,gtm_container)}
					data = [[element['emailAddress'],element['Account_Name'], nice_print_permissions(element['account_level_permissions']),element['container_name'], nice_print_permissions(element['container_level_permissions']),element['ReferentEmail'] , manage_expiration(element['YYYY'],element['MM']),element['YYYY'],element['MM']] for element in get_bigquery_data(options)]
					acclist = [[element[0],element[1]] for element in gtm_account_list if str(element[0]) == str(gtm_account)] + sorted([[element[0],element[1]] for element in gtm_account_list], key = lambda n: n[1])
					contlist = [[element[2],element[3]] for element in gtm_correspondance if str(element[2]) == str(gtm_container)] + sorted([[element[2],element[3]] for element in gtm_correspondance if str(element[0]) == str(gtm_account)], key = lambda n: n[1])
					
					template_values = {
					'account_selected':gtm_account,
					'container_selected':gtm_container,
					'acclist': acclist,
					'contlist': contlist,
					'gadatalist': data,
					'screen_name':'GTM by identifier'
					}
				elif gtm_account in [str(accountId) for accountId, accountname, container_Id, container_name in gtm_correspondance]:
					options = {'type': 'identifier_gtm_total', 'data': gtm_account}
					data = [[element['emailAddress'],element['Account_Name'], nice_print_permissions(element['account_level_permissions']),element['container_name'], nice_print_permissions(element['container_level_permissions']),element['ReferentEmail'] , manage_expiration(element['YYYY'],element['MM']),element['YYYY'],element['MM']] for element in get_bigquery_data(options)]
					acclist = [[element[0],element[1]] for element in gtm_account_list if str(element[0]) == str(gtm_account)] + sorted([[element[0],element[1]] for element in gtm_account_list], key = lambda n: n[0])
					contlist = [[element[2],element[3]] for element in gtm_correspondance if str(element[2]) == str(gtm_container)] + sorted([[element[2],element[3]] for element in gtm_correspondance if str(element[0]) == str(gtm_account)], key = lambda n: n[1])
					
					template_values = {
					'account_selected':gtm_account,
					'container_selected':gtm_container,
					'acclist':acclist,
					'gadatalist': data,
					'contlist': [['Choose a container','Choose a container']]+contlist,
					'screen_name':'GTM by identifier',
					}
				else:
					acclist = [['Choose an account','Choose an account']] + sorted(gtm_account_list, key = lambda n: n[1])
					contlist = [['Choose a container','Choose a container']]
					
					template_values = {
					'account_selected':gtm_account,
					'acclist':acclist,
					'contlist': contlist,
					'gtm_account_list': [accountId for accountId, accountname in gtm_account_list],
					'screen_name':'GTM by identifier'
					}
				template = JINJA_ENVIRONMENT.get_template('identifier_gtm.html')
				return template, template_values

			elif typ == 'email_gtm':
				options = {'type': 'email_gtm', 'data': individu}
				data = [[element['emailAddress'],element['Account_Name'], nice_print_permissions(element['account_level_permissions']),element['container_name'], nice_print_permissions(element['container_level_permissions']),element['ReferentEmail'] , manage_expiration(element['YYYY'],element['MM']),element['YYYY'],element['MM']] for element in get_bigquery_data(options)]
				
				template_values = {
				'email':individu,
				'gadatalist': data,
				'screen_name':'GTM by email'
				}
				template = JINJA_ENVIRONMENT.get_template('email_gtm.html')				
				return template, template_values


		typ = self.request.get('type')

		account = str(self.request.get('getaccountid'))
		property = str(self.request.get('getpropertyid'))
		gtm_account = str(self.request.get('getaccountid'))
		gtm_container = str(self.request.get('getcontainerid'))
		individu = self.request.get('email').lower()
		user = users.get_current_user()
		if user:
			nickname = user.nickname()
			logout_url = users.create_logout_url('/')
		template, template_values = screen(typ)
		template_values['version'] = Context().version
		template_values['nickname'] = nickname
		template_values['logout_url'] = logout_url
	
		self.response.write(template.render(template_values))

class Upload_to_bigquery(webapp2.RequestHandler):
	def store_to_bigquery_(context, target):
		from google.cloud import bigquery

		context = Context().context()
		client = bigquery.Client(project = Context().project_id)
		dataset_id, table_id, target = context['global']['dataset_id'], context[target]['table_id_raw']['id'], target
		bucket_path = '{}/'.format(context[target]["location"])
		dataset_ref = client.dataset(dataset_id)
		job_config = bigquery.LoadJobConfig()
		job_config.create_disposition = 'CREATE_IF_NEEDED'
		job_config.schema = [bigquery.SchemaField(element, 'STRING')for element in context[target]['table_id_raw']['schema']]
		job_config.write_disposition = 'WRITE_TRUNCATE'
		job_config.skip_leading_rows=1
		uri = 'gs://{}.csv'.format(context[target]['location'])

		try:
			job_config.field_delimiter = ';'
			load_job = client.load_table_from_uri(uri, dataset_ref.table(table_id), job_config=job_config)
			load_job.result()
		except:
			job_config.field_delimiter = ','
			load_job = client.load_table_from_uri(uri, dataset_ref.table(table_id), job_config=job_config)
			load_job.result()

		job_config = bigquery.QueryJobConfig() 
		table_ref = client.dataset(dataset_id).table(target)
		job_config.destination = table_ref
		job_config.use_legacy_sql = True
		job_config.create_disposition = 'CREATE_IF_NEEDED'
		job_config.write_disposition = 'WRITE_APPEND'
		query = open_query(context[target]['table_id']['keygen'])
		query_job = client.query(query, job_config= job_config)
		results = query_job.result()

	def store_to_bigquery(self):
		from google.cloud import bigquery

		context = Context().context()
		client = bigquery.Client(project = Context().project_id)

		def load_raw():
			job_config = bigquery.LoadJobConfig()
			dataset_id, table_id = context['global']['dataset_id'], context['global']['table_id_raw']
			dataset_ref = client.dataset(dataset_id)
			job_config = bigquery.LoadJobConfig()
			job_config.create_disposition = 'CREATE_IF_NEEDED'
			job_config.schema = [bigquery.SchemaField(element, 'STRING')for element in ['Email','GAorGTMAccount_name','GAorGTM2','GAorGTMidentifier','GAorGTMpermissions','ReferentEmail','Status','YYYY','MM']]
			job_config.write_disposition = 'WRITE_TRUNCATE'
			job_config.skip_leading_rows=1
			uri = 'gs://{}.csv'.format(Context().raw_upload)
			try:
				job_config.field_delimiter = ';'
				load_job = client.load_table_from_uri(uri, dataset_ref.table(table_id), job_config=job_config)
				load_job.result()
			except:
				job_config.field_delimiter = ','
				load_job = client.load_table_from_uri(uri, dataset_ref.table(table_id), job_config=job_config)
				load_job.result()

		def load_upperlevel_custom():
			job_config = bigquery.QueryJobConfig()
			target_list = ['analytics','tagmanager']
			for target in target_list:
				dataset_id, table_id = context['global']['dataset_id'], context[target]['table_id']['id']
				dataset_ref = client.dataset(dataset_id) 
				table_ref = client.dataset(dataset_id).table(table_id)
				job_config.destination = table_ref
				job_config.use_legacy_sql = True
				job_config.create_disposition = 'CREATE_IF_NEEDED'
				job_config.write_disposition = 'WRITE_APPEND'
				query = open_query(context[target]['table_id']['keygen'])
				query_job = client.query(query, job_config= job_config)
				results = query_job.result()


		load_raw()
		#load_upperlevel_custom()

	def post(self):
		uploaded_file = self.request.POST.get("file")
		pageurl = self.request.POST.get('pageurl')
		uploaded_file_content = uploaded_file.file.read()
		context = Context().context()
		gcs_file = gcs.open('/{}.csv'.format(Context().raw_upload),'w')
		gcs_file.write(uploaded_file_content)
		gcs_file.close()
		self.store_to_bigquery()

		task_name ='customdata_handler_{}'.format(datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S"))
		task = taskqueue.add(name=task_name, url='/customdata_handler', target='update', method='GET')

		self.redirect(str(pageurl))

class EnqueueTaskHandler(webapp2.RequestHandler):

	def get(self):

		task_name ='refresh_{}'.format(datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S"))
		task = taskqueue.add(name=task_name, url='/refresh', target='update', method='GET')

		template_values = {
							'task_info': task.name,
							}

		template_values['version'] = 'update! this can take few minutes'

		template = JINJA_ENVIRONMENT.get_template('home.html')
		self.response.write(template.render(template_values))


class ReferentEmailPage(webapp2.RequestHandler):
	def get(self):
		user = users.get_current_user()
		if user:
			nickname = user.nickname()
			logout_url = users.create_logout_url('/')
		template_values = {
			'version' :  Context().version,
			'screen_name':'Referent Email Upload',
			'nickname' : nickname,
			'logout_url' : logout_url
		}
		template = JINJA_ENVIRONMENT.get_template('referent_email.html')
		self.response.write(template.render(template_values))
		
# [START app]
app = webapp2.WSGIApplication([
	('/', LandingPage),
	('/analytics',LandingPageAnalytics),
	('/to_bq', Upload_to_bigquery),
	('/enqueue', EnqueueTaskHandler),
	('/endpoint6', Endpoints),
	('/endpoint7',AnalyticsSummary),
	('/referent_email',ReferentEmailPage)
		], debug=True)
# [END app]
