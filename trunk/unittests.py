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

from google.appengine.ext import db
import helpers
import unittest
import datastore_sqlite_stub
import os


class TestModel(db.Model):
  text = db.StringProperty(default='some text')
  number = db.IntegerProperty(default=42)


class UnitTests(unittest.TestCase):
  
  def setUp(self):
    """Set up in-memory connection."""
    os.environ['APPLICATION_ID'] = 'test'
    self.connection = helpers.setup_sqlite()
    
  def tearDown(self):
    """Cleanup after a unit test."""
    helpers.teardown_sqlite()
    
  def testCreateTabledef(self):
    """Checks if our TestModel can be converted 
       into a valid SQL table."""
    helpers.create_tables([TestModel()], self.connection)
    cursor = self.connection.cursor()
    cursor.execute(
      "INSERT INTO TestModel(string_text,int64_number) "
      "VALUES ('test', 13)")
    
  def testGetSchema(self):
    helpers.create_tables([TestModel()], self.connection)
    prm = datastore_sqlite_stub.PRMHelper()
    schema = prm.getSchema(self.connection, 'TestModel')
    assert schema
    self.assertEquals(['text', 'number'], schema.keys())
    self.assertEquals({
        'text': ['string_text'],
        'number': ['int64_number']}, schema)
    
  def testWriteSingle(self):
    """Writes a single model to the database retrieves it."""
    helpers.create_tables([TestModel()], self.connection)
    model = TestModel()    
    key = model.put()
    id = key._Key__reference.path().element_list()[-1].id()    
    cursor = self.connection.cursor()
    cursor.execute(
        'SELECT string_text, int64_number FROM TestModel '
        'WHERE pk_int=%s' % id)
    result = cursor.fetchone()
    self.assertEquals('some text', result[0])
    self.assertEquals(42, result[1])
    
  def testWriteDouble(self):
    """Writes a value into the database twice."""
    helpers.create_tables([TestModel()], self.connection)
    model = TestModel()    
    key = model.put()
    id = key._Key__reference.path().element_list()[-1].id()    
    key = model.put()
    id2 = key._Key__reference.path().element_list()[-1].id()
    self.assertEquals(id, id2)
    cursor = self.connection.cursor()
    cursor.execute(
        'SELECT COUNT(*) FROM TestModel WHERE pk_int=%s' % id)
    result = cursor.fetchone()
    self.assertEquals(1, result[0])
    
  def testGetSingleElement(self):
    """Gets a single model from the datastore."""
    helpers.create_tables([TestModel()], self.connection)
    model1 = TestModel(number=1)
    model2 = TestModel(number=2, text='#2')
    model3 = TestModel(number=3)
    key1 = model1.put()
    key2 = model2.put()
    key3 = model3.put()
    fetched = TestModel.get(key2)
    self.assertEquals(2, fetched.number)
    self.assertEquals('#2', fetched.text)

  def testGetSingleElementByCustomKey(self):
    """Gets a single model from the datastore with a string key."""
    helpers.create_tables([TestModel()], self.connection)
    model1 = TestModel(number=1)
    model2 = TestModel(key_name='custom', number=2, text='#2')
    model3 = TestModel(number=3)
    key1 = model1.put()
    key2 = model2.put()
    key3 = model3.put()
    fetched = TestModel.get_by_key_name('custom')
    self.assertEquals(2, fetched.number)
    self.assertEquals('#2', fetched.text)

    
  def testGetMultipleElements(self):
    """Gets a several models from the datastore."""
    helpers.create_tables([TestModel()], self.connection)
    model1 = TestModel(number=1)
    model2 = TestModel(number=2)
    model3 = TestModel(number=3)
    key1 = model1.put()
    key2 = model2.put()
    key3 = model3.put()
    fetched = TestModel.get([key2, key1])
    self.assertEquals(2, len(fetched))
    self.assertEquals(2, fetched[0].number)
    self.assertEquals(1, fetched[1].number)

  def testGetOrInsert(self):
    """tests if get_or_insert works correctly"""
    helpers.create_tables([TestModel()], self.connection)
    model1 = TestModel(number=1)
    model1.put()
    model2 = TestModel.get_or_insert('foo', number=13, text='t')
    self.assertEquals(13, model2.number)
    fetched = TestModel.get_by_key_name('foo')
    self.assertEquals(13, fetched.number)

  def testSimpleQuery(self):
    helpers.create_tables([TestModel()], self.connection)
    model = TestModel(text='t1', number=13)
    model.put()
    data = TestModel.gql(
        'WHERE text=:1 and number=:2 order by text desc', 
        't1', 13).fetch(5)
    self.assertEquals(1, len(data))
    self.assertEquals('t1', data[0].text)
    self.assertEquals(13, data[0].number)
    
    
if __name__ == '__main__':
    unittest.main()

