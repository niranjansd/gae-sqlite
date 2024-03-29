# Copyright 2008 Jens Scheffler
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

from datastore_base import DatastoreSqlStub
from datastore_sqlite_stub import PRMHelper
from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_file_stub
from pysqlite2 import dbapi2 as sqlite


def setup_refstore(app_id='test'):
  """Sets up a clean "reference" store (file-based implementation."""
  stub = datastore_file_stub.DatastoreFileStub(app_id, None, None)
  apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', stub)

def setup_sqlite(name=None):
  """Sets up an in-memory sqlite instance 
     and connects the datastore to it.
  
  Args:
    name: the name of the instance to connect to, 
      None or empty string for in-memory
  
  Returns:
    a sqlite connection object pointing to the database.
  """
  if name:
    raise 'Not implemented yet'
  else:
    connection = sqlite.connect(':memory:')
    name = 'memory'
    get_connection = lambda: connection
    release_connection = lambda x: None    
    prm_helper = PRMHelper(get_connection, release_connection)
    stub = DatastoreSqlStub(prm_helper)
    apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', stub)
    return connection


def teardown_sqlite():
  """Used by unit-tests to disconnect and unregister 
     the sqlite stub."""
  proxy = apiproxy_stub_map.apiproxy
  proxy._APIProxyStubMap__stub_map.pop('datastore_v3')
  

def create_tabledef(connection, model_instance, prm_helper=None):
  """Create a SQL statement to populate a table 
     from a fully populated Model.
  
  Args:
    connection: a database connection that should be used
    model_instance: an instance of a model, 
      each field filled with a non-None value
    prm_helper: an object that contains logic 
      about how protocl-buffers
      should be mapped to a relational datastore
      
  Returns:
    a SQL statement that could be used to create a new table.
  """
  
  # Make sure the helper exists
  if not prm_helper:
    prm_helper = PRMHelper(None, None)
  
  # Convert the model into a map of propert key/value pairs
  entity = model_instance._populate_internal_entity()
  pb = entity._ToPb()
  as_dict = prm_helper.entityToDict(pb)
  
  # Delegate to the prm-helper
  return prm_helper.suggestMutation(
      connection, model_instance.kind(), as_dict)[0]
      
      
def create_tables(list_of_models, connection):
  """Creates one or more SQL tables 
     from a list of prepopulated models.
  
  Args:
  list_of_models: a list of models, each of a different kind
  connection: the SQLite connection that should be used
  """
  cursor = connection.cursor()
  ok = False
  try:
    for model in list_of_models:
      tabledef = create_tabledef(connection, model)
      cursor.execute(tabledef)
    ok = True
  finally:
    if ok:
      connection.commit()
    else:
      connection.rollback()
