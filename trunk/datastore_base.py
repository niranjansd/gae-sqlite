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

import google.appengine.datastore.entity_pb as entity_pb
from google.appengine.runtime import apiproxy_errors
from google.appengine.datastore import datastore_pb
import threading


class DatastoreSqlStub(object):
  """ Datastore stub implementation that uses a SQL connection.
  
      Database specific details (dependent on what rdbms is used
      are abstracted into a PRMHelper object (see
      datastore_sqlite_stub for an example). Content in this
      file should not depend on concrete database details.
  """
  
  def __init__(self, prm_helper):
    """Constructor.

    Args:
      prm_helper:
        a class that provides miscellaneous tools for 
        protocolbuffer-to-rdbms mapping
    """
    self.__next_tx_handle = 1
    self.__open_transactions = {}
    self.__tx_handle_lock = threading.Lock()
    self.__next_cursor = 1
    self.__cursor_lock = threading.Lock()
    self.__queries = {}
    self.__prm = prm_helper
    
    
  def _connect(
      self, transaction=None, may_update=False, delete=False):
    """Opens a new or returns an opened connection.
    
    Args:
      transaction: an optional transaction protocol buffer. If not 
        set, a new, temporary object is returned. If set to a 
        transaction without a handle, a new object will be created. 
        If set to a transaction, the appropriate element. If the 
        handle has no handle, that means a new cursor will be 
        created and the handle be set.
      may_update: if set to True (default is False), the newly 
        created cursor object will be remembered as a new transaction
        and its handle updated in the transaction object
      delete: if set to true (default is False), delete the cursor 
        from the internal data structure if it exists
        
    Returns:
      a cursor
    """
    if not transaction:
      return self.__prm.get_connection()
    self.__tx_handle_lock.acquire()
    try:
      if transaction.has_handle():
        handle = transaction.handle()
        if handle in self.__open_transactions:
          if delete:
            return self.__open_transactions.pop(handle)
          else:
            return self.__open_transactions.get(handle)
        else:
          raise apiproxy_errors.ApplicationError(
            datastore_pb.Error.BAD_REQUEST,
            'Transaction handle %d not found' % handle)
      else:
        if not may_update:
          return self.__prm.get_connection()
        handle = self.__next_tx_handle
        self.__next_tx_handle += 1
        transaction.set_handle(handle)
        cursor = self.__prm.get_connection()
        self.__open_transactions[handle] = cursor
    finally:
      self.__tx_handle_lock.release()

  def close(self):    
    """Teardown-method for unit tests, 
       closes the database connection."""
    self.self.__prm.release_connection(connection)
    
  def MakeSyncCall(self, service, call, request, response):
    """ Taken pretty much verbatim 
        from the original datastore_file_stub."""
    assert service == 'datastore_v3'
    explanation = []
    assert request.IsInitialized(explanation), explanation
    (getattr(self, "_Dynamic_" + call))(request, response)
    assert response.IsInitialized(explanation), explanation
    

  def _Dynamic_Put(self, put_request, put_response):
    
    # Fetch a cursor from the connection
    connection = self._connect(
        put_request.transaction(), False, False)
    cursor = connection.cursor()
    
    # Iterate theough the instances
    clones = []
    for entity in put_request.entity_list():
      # create a defensive copy called "clone"      
      clone = entity_pb.EntityProto()
      clone.CopyFrom(entity)
      clones.append(clone)
    
      # make sure that the clone has a valid key
      assert clone.has_key()
      assert clone.key().path().element_size() > 0
      tablename = clone.key().path().element_list()[0].type()
    
      # populate the key's app name with a value
      clone.mutable_key().set_app('sql-app')
      
      # Convert the protocol buffer into a dictionary
      values = self.__prm.entityToDict(clone)
      
      # If the instance has a primary key, delete the old row
      last_path = clone.key().path().element_list()[-1]
      if last_path.id() != 0:
        cursor.execute('DELETE FROM %s WHERE pk_int=%s' % 
                       (tablename, last_path.id()))
        values['pk_int'] = last_path.id()
      if last_path.has_name():
        cursor.execute('DELETE FROM %s WHERE pk_string=?' % 
                       tablename, [last_path.name()])
        values['pk_string'] = last_path.name()
        
      # Make sure that the database schema is current
      schema_mutations = self.__prm.suggestMutation(
          connection, tablename, values)
      for mutation in schema_mutations:
        cursor.execute(mutation)
      
      # Using the dictionary of values, create a SQL query
      # TODO: how handle list elements
      keyval_list = values.items()
      key_list = ','.join([first for first, second in keyval_list])
      value_list = [second for first, second in keyval_list]
      questionmarks = ','.join(['?' for value in value_list])
      if len(key_list):
        cursor.execute(
            'INSERT INTO %s (%s) VALUES (%s)' % 
                (tablename, key_list, questionmarks),
            value_list)
      else:
        cursor.execute('INSERT INTO %s (pk_string) VALUES(null)' % tablename)
      
      # Grab the primary key and update the result
      if last_path.id() == 0 and not last_path.has_name():
        last_path.set_id(cursor.lastrowid)
        
    # Do a database commit
    if not put_request.has_transaction():
      connection.commit()
      self.__prm.release_connection(connection)

    # Populate the response        
    put_response.key_list().extend([c.key() for c in clones])
    

  def _Dynamic_Get(self, get_request, get_response):
    # Fetch a cursor from the connection
    connection = self._connect(
        get_request.transaction(), False, False)
    cursor = connection.cursor()
    
    for key in get_request.key_list():
      
      # Populate an entity with a clone of the key
      entity = entity_pb.EntityProto()
      result_key = entity.mutable_key()
      result_key.CopyFrom(key)
      result_key.set_app('sql-app')
      key = result_key
      group = get_response.add_entity()
      
      # Build and execute the SQL query
      tablename = key.path().element_list()[0].type()
      last_path = key.path().element_list()[-1]
      if last_path.has_name():
        query = 'SELECT * FROM %s WHERE pk_string=?' % tablename
        param = str(last_path.name())
      else:
        query = 'SELECT * FROM %s WHERE pk_int=?' % tablename
        param = long(last_path.id())
      cursor.execute(query, [param])
      data = cursor.fetchone()
      if not data:
        continue
      
      # Populate the entity and store it in the response
      keyvals = self.__prm.rowToDict(cursor, data)
      self.__prm.dictToEntity(keyvals, entity)
      entity.mutable_entity_group().CopyFrom(key.path())
      group.mutable_entity().CopyFrom(entity)
      
      # Close the connection
      if not get_request.has_transaction():
        self.__prm.release_connection(connection)


  def _Dynamic_Delete(self, delete_request, delete_response):
    #TODO: find out what this is good for
    pass

  _Operator_NAMES = {
    1: "<",
    2: "<=",
    3: ">",
    4: ">=",
    5: "=",
    #6: "IN",
    #7: "EXISTS",
  }

  def _Dynamic_RunQuery(self, query, query_result):
    
    # Turn the filter-objects into a set of conditions
    def build_query_conditions(dictionary, filter, col_key, value):
      operator = filter.op()
      names = DatastoreSqlStub._Operator_NAMES
      if operator in names:
        key = '%s %s' % (col_key, names[operator])
        assert not key in dictionary,\
            'Multiple conditions not supported yet: %s' % key
        dictionary[key] = value
      elif operator == 7:
        key = '%s NOT NULL' % col_key
        assert not key in dictionary,\
            'Multiple conditions not supported yet: %s' % key
        dictionary[key] = None
      else:
        assert False, 'Unsupported operator: %s' % operator
    conditions = self.__prm.entityToDict(
        pb=query, 
        populate_dict=build_query_conditions, 
        get_list=lambda x:x.filter_list(), 
        unwrap_properties=lambda x:x.property_list())
    
    # Concatenate the conditions in a statement
    sqlquery = 'SELECT * FROM %s' % query.kind()
    params = {}
    count = 0
    for key, value in conditions.items():
      if count == 0:
        sqlquery = '%s WHERE %s' % (sqlquery, key)
      else:
        sqlquery = '%s AND %s' % (sqlquery, key)
      if value != None:
        params[str(count)] = value
        sqlquery = '%s :%s' % (sqlquery, count)
      count += 1
  
    # Open a connection and execute the rest of the
    # method in a try-finally block  
    connection = self._connect(None, False, False)
    try:
      
      # Now, look at the sort order
      order_conditions = []
      schema = self.__prm.getSchema(connection, query.kind())
      for entry in query.order_list():
        if schema and entry.property() in schema:
          for column in schema[entry.property()]:
            order = \
                'ASC'\
                if entry.direction() == \
                    datastore_pb.Query_Order.ASCENDING \
                else 'DESC'
            order_conditions.append('%s %s' % (column, order))
      if len(order_conditions) > 0:
        sqlquery = '%s ORDER BY %s' % (
            sqlquery, ', '.join(order_conditions))
    
      # Execute the query, if the table exists
      rows = []
      if schema:
        cursor = connection.cursor()
        cursor.execute(sqlquery, params)
        try:
          for i in range(query.offset()):
            cursor.next()
        except StopIteration:
          pass
        rows = cursor.fetchmany(max(0,min(1000, query.limit())))
    
      # Convert the rows into PBs
      results = []
      for data in rows:
      
        # Extract data from row and convert to entity
        entity = entity_pb.EntityProto()
        results.append(entity)
        keyvals = self.__prm.rowToDict(cursor, data, False)
        self.__prm.dictToEntity(keyvals, entity)
      
        # Extract primary key
        pk = self.__prm.pkFromRow(query.kind(), keyvals)
        key_pb = entity.mutable_key().CopyFrom(pk)
        
        # TODO: better support for entity groups?
        group = entity.mutable_entity_group()
        root = pk.path().element(0)
        group.add_element().CopyFrom(root)
        
      
      # The following code is taken from datastore_file_stub  
      self.__cursor_lock.acquire()
      cursor = self.__next_cursor
      self.__next_cursor += 1
      self.__cursor_lock.release()
      self.__queries[cursor] = (results, len(results))
      query_result.mutable_cursor().set_cursor(cursor)
      query_result.set_more_results(len(results) > 0)
    
    # Clean up resources at the end
    finally:
      self.__prm.release_connection(connection)


  def _Dynamic_Next(self, next_request, query_result):
    """Taken verbatim from datastore_file_stub.py."""
    cursor = next_request.cursor().cursor()

    try:
      results, orig_count = self.__queries[cursor]
    except KeyError:
      raise apiproxy_errors.ApplicationError(
          datastore_pb.Error.BAD_REQUEST,
          'Cursor %d not found' % cursor)

    count = next_request.count()
    for r in results[:count]:
      query_result.add_result().CopyFrom(r)
    del results[:count]

    query_result.set_more_results(len(results) > 0)


  def _Dynamic_Count(self, query, integer64proto):
    #TODO: find out what this is good for
    pass


  def _Dynamic_BeginTransaction(self, request, transaction):
    self._connect(transaction, True, False)


  def _Dynamic_Commit(self, transaction, transaction_response):
    connection = self._connect(transaction, False, True)
    connection.commit()
    self.__prm.release_connection(connection)
    

  def _Dynamic_Rollback(self, transaction, transaction_response):
    connection = self._connect(transaction, False, True)
    connection.rollback()
    self.__prm.release_connection(connection)


  def _Dynamic_GetSchema(self, app_str, schema):
    #TODO: find out what this is good for
    pass


  def _Dynamic_CreateIndex(self, index, id_response):
    #TODO: find out what this is good for
    pass


  def _Dynamic_GetIndices(self, app_str, composite_indices):
    #TODO: find out what this is good for
    pass


  def _Dynamic_UpdateIndex(self, index, void):
    #TODO: find out what this is good for
    pass


  def _Dynamic_DeleteIndex(self, index, void):
    #TODO: find out what this is good for
    pass

