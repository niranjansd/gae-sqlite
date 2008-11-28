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

from google.appengine.api import datastore


class PRMHelper(object):
  """ProtocolBuffer-Relational-Mapping tool.
     Encapsulates helpers that deal with the mapping between
     protocol buffer and a relational database. Currently, this
     means SQLite, but it could be generalized in the future.
  """  
  
  def __init__(
      self, get_connection, release_connection):
    """Constructor.

    Args:
      get_connection: a parameterless function that provides a new
        connection from the pool
      release_connection: a function that accepts a connection 
        and puts it back into the pool
    """
    self.get_connection = get_connection
    self.release_connection = release_connection
    self.__types = { str: 'TEXT', long: 'INTEGER', 
                    int: 'INTEGER', float: 'DOUBLE'}

  def __suggestRows(self, sample_dict):
    """Suggests a list of row definitions for this SQL dialect.
    
       Args:
         sample_dict: a dictionary of column names and sample
           entries (required to determine the sql column type
           that should be used.)
       Returns:
         a list of strings of form "column_name type" that
         could be used for a sql create table statement.
    """
    result = []
    for key, val in sample_dict.items():
      value_type = type(val)
      assert value_type in self.__types, ('Cannot '
          'convert type %s' % value_type)
      result.append('%s %s' % (key, self.__types[value_type]))
    return result
  
  def getSchema(self, connection, table_name):
    """For a given table name, return all property names defined.
    
       Args:
         connection: the sql connection that should be used to 
                     gather any metadata
         table_name: the name of the table/model to inspect
    
       Returns:
         a dictionary with the property names as keys and a list
         of column names (used to store them) as values. If the
         table does not exist, None will be returned
    """
    result = {}
    has_table = False
    for column in connection.cursor().execute(
        "PRAGMA TABLE_INFO(%s)" % table_name).fetchall():
      has_table = True
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
    return result if has_table else None
  
  def suggestMutation(self, connection, table_name, sample_dict, add_rowz=True):
    """Suggests an sql statement that mutates a db schema.
    
       Given a table name, this method will check if the table exists
       and if a given set of column names exist. If the table does
       not exist, it will return a CREATE TABLE statement that
       can be executed. If the table does exist but one or more
       column names do not exist, it will return a statement to
       modify the table instead.
    
       Args:
         connection: the sql connection that should be used to 
                     gather any metadata
         table_name: the name of the table that should be validated.
         sample_dict: a dictionary of column names and sample
           entries (required to determine the sql column type
           that should be used.)
         add_rowz: if set to true, it will create any "standard" columns
           (for primary keys and such) upon table creation. 
           True is default.
       Returns:
         a list sql statements that can be executed to perform the mutation
    """
    
    # Check if the table exists and if so, eliminate duplicate rows
    new_rows = dict(sample_dict)
    new_table = False
    current_schema = self.getSchema(connection, table_name)
    if current_schema is None:
      new_table = True
    else:      
      for columns in current_schema.values():
        for column in columns:
          new_rows.pop(column, None)
        new_rows.pop('pk_string', None)
        new_rows.pop('pk_int', None)
          
    # If there are no new rows, we are done
    if not new_table and not len(new_rows):
      return []
          
    # Translate each key/value pair into a SQL-ish type definition
    # and two more columns for the primary key, if necessary
    snippets = self.__suggestRows(new_rows)
    if new_table and add_rowz:  
      snippets.append('pk_int INTEGER PRIMARY KEY')
      snippets.append('pk_string TEXT')

    # Convert the snippets into a proper statement
    if new_table:
      return ['CREATE TABLE %s (%s);' %
          (table_name, ','.join(snippets))]
    else:
      return ['ALTER TABLE %s ADD COLUMN %s' % 
          (table_name, snippet) for snippet in snippets]
  
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
        elif value_pb.has_booleanvalue():
          col_key = 'boolean_' + property_name
          property_value = 1 if value_pb.booleanvalue() else 0
        elif value_pb.has_doublevalue():
          col_key = 'double_' + property_name
          property_value = value_pb.doublevalue()
        else:
          raise 'Not supported yet: %s' % value
          # TODO: support at least the following primitives:
          # point, user, reference
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

      # Case: type boolean
      elif p1 == 'boolean':
        prop = pb.add_property()
        prop.set_name(p2)
        prop.set_multiple(False)
        prop.mutable_value().set_booleanvalue(value != 0)

      # Case: type double
      elif p1 == 'double':
        prop = pb.add_property()
        prop.set_name(p2)
        prop.set_multiple(False)
        prop.mutable_value().set_doublevalue(value)
    