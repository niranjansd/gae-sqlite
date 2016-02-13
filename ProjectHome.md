This is a module that will allow you to run Google's App Engine development server with an SQLite backend. The concept is intended to be generic enough that other databases (mysql, SQL Server, Oracle...) can follow with only minimal modifications.

For additional information, check also http://blog.appenginefan.com/search/label/SQLite

## Installation ##

### Unit Tests ###

To run the unit tests, add the directory of your Google App Engine SDK to your PYTHONPATH and then run `unittests.py`.

```
$ PYTHONPATH=/home/jcgregorio/Desktop/googleappengine/
~/projects/gae-sqlite
$ python unittests.py 
.........
----------------------------------------------------------------------
Ran 9 tests in 0.029s

OK
~/projects/gae-sqlite
$ 
```

## Running ##

...