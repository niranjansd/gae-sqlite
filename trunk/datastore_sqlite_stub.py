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
from google.appengine.api import datastore
from google.appengine.datastore import datastore_pb
import threading

class PRMHelper(object):
  """ProtocolBuffer-Relational-Mapping tool.
     Encapsulates helpers that deal with the mapping between
     protocol buffer and a relational database. Currently, this
     means SQLite, but it could be generalized in the future.
  """  
  
  def getSchema(self, connection, table_name):
    """For a given table name, return all property names defined.
    
       Args:
         connection: the sql connection that should be used to 
                     gather any metadata
         table_name: the name of the table/model to inspect
    
       Returns:
         a dictionary with the property names as keys and a list
         of column names (used to store them) as values
    """
    result = {}
    for column in connection.cursor().execute(
        "PRAGMA TABLE_INFO(%s)" % table_name).fetchall():
      key = column[1]
      i = key.find('_')
      if i < 1:
        continue
      p1 = key[0:i]
      if p1 == 'pk':
        continue
      p2 = key[i+1:]
      if not p2 in result:
        result[p2] = []
      result[p2].append(key)
    return result
      
  
  def rowToDict(self, cursor, row, remove_metadata=True):
    """Convert a given row from a cursor into a dictionary,
       using the column names as keys.
       
       Args:
         cursor: a database curspr that contains the metadata
         row: the row to work on
         remove_pk: if set to True (default), kick the rows out
           that are related to metadata (like primary keys)
    """
    keys = [metadata[0] for metadata in cursor.description]
    keyvals = dict([(keys[i], row[i]) for i in range(len(keys))])
    if remove_metadata:
      keyvals.pop('pk_string')
      keyvals.pop('pk_int')
    return keyvals
  
  def pkFromRow(self, kind, keyvals):
    """Converts a given dictionary of values into a primary key."""
    int_pk = keyvals.get('pk_int', None)
    string_pk = keyvals.get('pk_string', None)
    if int_pk == None and string_pk == None:
      return None
    if string_pk:
      return datastore.Key.from_path(kind, string_pk)
    return datastore.Key.from_path(kind, int_pk)._ToPb()


  def entityToDict(self, pb,
                   populate_dict=None,
                   get_list=lambda x:x.property_list(),
                   unwrap_properties=lambda x:[x]):
    """Converts an entity protocol buffer to a dictionary.
       This method will only deal with the properties of the entity,
       not its primary key.
      
       Args:
         pb: the entity protocol buffer
         populate_dict: a method that takes a dictionary,
           a property object(or whatever else get_list returns to
           iterate on), a column key and a value as parameters 
           and decides how to modify the dictionary. If not given, 
           the default behavior is to ignore the property and put the
           value into the map, using the column key as key.
           Implementation detail: if a property value maps to more
           than one row (like a geo-coordinate that could be split
           up in latitude and longitude), the method might be called
           more than once with the same property object
         get_list: a way to extract a list of properties from pb
           (or anything else iterable that returns something that
           the get_property method can turn into a property;
            default behavior is to call "property_list")
         unwrap_properties: takes an element from the get_list
           result and turns it into list of property pbs. 
           Default is lambda x:[x].
         
       Returns:
         a dictionary with appropriate kev/value pairs that can
         be stored in a SQLite database.
    """
    result = {}
    #TODO: what about raw properties?
    for item in get_list(pb):
      for property in unwrap_properties(item):
        #TODO: what about multiple properties?
        assert not (property.has_multiple() and property.multiple())
        if not property.has_value():
          continue
        property_name = property.name()
        value_pb = property.value()
        col_key = None
        property_value = None
        if value_pb.has_int64value():
          col_key = 'int64_' + property_name
          property_value = value_pb.int64value()
        elif value_pb.has_stringvalue():
          col_key = 'string_' + property_name
          property_value = value_pb.stringvalue()
        else:
          raise 'Not supported yet: %s' % value
        if not populate_dict:
          result[col_key] = property_value
        else:
          populate_dict(result, item, col_key, property_value)
    return result
  
  def dictToEntity(self, values, pb):
    """Transfers values from a dictionary into a protocol buffer
       This method will only deal with the properties of the entity,
       not its primary key.
      
       Args:
         values: a dctionary of values
         pb: the entity protocol buffer
    """
    for key, value in values.items():
      
      # Split up the key (like int64_name) into segments
      i = key.find('_')
      if i < 1:
        continue
      p1 = key[0:i]
      p2 = key[i+1:]
      
      # Case: type integer
      if p1 == 'int64':
        prop = pb.add_property()
        prop.set_name(p2)
        prop.set_multiple(False)
        prop.mutable_value().set_int64value(long(value))
        
      # Case: type string
      elif p1 == 'string':
        prop = pb.add_property()
        prop.set_name(p2)
        prop.set_multiple(False)
        prop.mutable_value().set_stringvalue(value)
    

class DatastoreSqliteStub(object):
  """ Datastore stub implementation that uses a sqlite instance."""
  
  def __init__(
      self, get_connection, release_connection, prm_helper=None):
    """Constructor.

    Args:
      get_connection: a parameterless function that provides a new
        connection from the pool
      release_connection: a function that accepts a connection 
        and puts it back into the pool
      prm_helper:
        a class that provides miscellaneous tools for 
        protocolbuffer-to-rdbms mapping
    """
    self._get_connection = get_connection
    self._release_connection = release_connection
    self.__next_tx_handle = 1
    self.__open_transactions = {}
    self.__tx_handle_lock = threading.Lock()
    self.__next_cursor = 1
    self.__cursor_lock = threading.Lock()
    self.__queries = {}
    if prm_helper:
      self.prm = prm_helper
    else:
      self.prm = PRMHelper()
    
    
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
      return self._get_connection()
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
          return self._get_connection()
        handle = self.__next_tx_handle
        self.__next_tx_handle += 1
        transaction.set_handle(handle)
        cursor = self._get_connection()
        self.__open_transactions[handle] = cursor
    finally:
      self.__tx_handle_lock.release()

  def close(self):    
    """Teardown-method for unit tests, 
       closes the database connection."""
    self.self._release_connection(connection)
    
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
      values = self.prm.entityToDict(clone)
      
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
        
      # Using the dictionary of values, create a SQL query
      # TODO: how handle list elements
      keyval_list = values.items()
      key_list = ','.join([first for first, second in keyval_list])
      value_list = [second for first, second in keyval_list]
      questionmarks = ','.join(['?' for value in value_list])
      cursor.execute(
          'INSERT INTO %s (%s) VALUES (%s)' % 
              (tablename, key_list, questionmarks),
          value_list)
      
      # Grab the primary key and update the result
      if last_path.id() == 0 and not last_path.has_name():
        last_path.set_id(cursor.lastrowid)
        
    # Do a database commit
    if not put_request.has_transaction():
      connection.commit()
      self._release_connection(connection)

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
      keyvals = self.prm.rowToDict(cursor, data)
      self.prm.dictToEntity(keyvals, entity)
      entity.mutable_entity_group().CopyFrom(key.path())
      group.mutable_entity().CopyFrom(entity)
      
      # Close the connection
      if not get_request.has_transaction():
        self._release_connection(connection)


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
      names = DatastoreSqliteStub._Operator_NAMES
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
    conditions = self.prm.entityToDict(
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
  
    # Open a connection and execute the reentitiesst of the
    # method in a try-finally block  
    connection = self._connect(None, False, False)
    try:
      
      # Now, look at the sort order
      order_conditions = []
      schema = self.prm.getSchema(connection, query.kind()) \
          if query.order_size() > 0 else None
      for entry in query.order_list():
        if entry.property() in schema:
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
    
      # Execute the query
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
        keyvals = self.prm.rowToDict(cursor, data, False)
        self.prm.dictToEntity(keyvals, entity)
      
        # Extract primary key
        pk = self.prm.pkFromRow(query.kind(), keyvals)
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
      self._release_connection(connection)


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
    self._release_connection(connection)
    

  def _Dynamic_Rollback(self, transaction, transaction_response):
    connection = self._connect(transaction, False, True)
    connection.rollback()
    self._release_connection(connection)


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

