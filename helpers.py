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

from datastore_sqlite_stub import DatastoreSqliteStub
from datastore_sqlite_stub import PRMHelper
from google.appengine.api import apiproxy_stub_map
from pysqlite2 import dbapi2 as sqlite


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
    stub = DatastoreSqliteStub(get_connection, release_connection)
    apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', stub)
    return connection


def teardown_sqlite():
  """Used by unit-tests to disconnect and unregister 
     the sqlite stub."""
  proxy = apiproxy_stub_map.apiproxy
  proxy._APIProxyStubMap__stub_map.pop('datastore_v3')
  

def create_tabledef(model_instance, prm_helper=None):
  """Create a SQL statement to populate a table 
     from a fully populated Model.
  
  Args:
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
    prm_helper = PRMHelper()
  
  # Convert the model into a map of propert key/value pairs
  entity = model_instance._populate_internal_entity()
  pb = entity._ToPb()
  as_dict = prm_helper.entityToDict(pb)
  
  # Translate each key/value pair into a SQL-ish type definition
  types = {str: 'TEXT', long: 'INTEGER', int: 'INTEGER'}
  def convert(value):
    key = type(value)
    assert key in types, 'Cannot convert type %s' % key
    return types[key]
  cols = ['%s %s' % (key, convert(val)) for key,val in \
          as_dict.items()]
  
  # Add two more columns for the primary key
  cols.append('pk_int INTEGER PRIMARY KEY')
  cols.append('pk_string TEXT')
  
  # Concatenate all type definitions into one create statement
  return 'CREATE TABLE %s (%s);' % (
      model_instance.kind(), ','.join(cols))
      
      
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
      tabledef = create_tabledef(model)
      cursor.execute(tabledef)
    ok = True
  finally:
    if ok:
      connection.commit()
    else:
      connection.rollback()
