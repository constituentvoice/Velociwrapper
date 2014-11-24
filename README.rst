Velociwrapper
=============

Velociwrapper is a wrapper to create ORM like features around Elasticsearch indexes.
Velociwrapper is not a true ORM since Elasticsearch isn't a relational database

Getting Started
---------------

::
	
	# configuration
	import velociwrapper
	velociwrapper.config.dsn = ["localhost"]
	velociwrapper.config.default_index = 'my_elasticsearch_index'
	velociwrapper.config.results_per_page = 50
	
	from velociwrapper import VWBase, VWCollection
	from velociwrapper.es_type import String, Integer, DateTime
	from velociwrapper.mapper import Mapper # for creating or reindexing indexes

	# create models, similar to SQLAlchemy

	class User(VWBase):
		
		__index__ = 'user_index'  # each model can have a custom index. If omitted, uses the default
		__type__ = 'user' # each model most specify the type. Cooresponds to the doc_type in Elasticsearch

		username = String(analyzed=False) # keyword arguments can be used to affect the mapping 
		password = String(analyzed=False)
		email = String()
		permission_level = String('default_permission', analyzed=False) # ES Types can have default values
		name = '' # Velociwrapper will automatically convert python types to the appropriate type (but you can't specify mappings)
		created = DateTime() # defaults to current time
		address = {} # models can also have nested information

	# define a collection. You only need to specify the model
	class Users(VWCollection):
		__model__ = User

	
	if __name__ == '__main__':
		# create indexes
		Mapper().create_indicies() # creates all defined VWBase models

		# create a model
		user = User(
			username='johndoe',
			password=some_encrypt_method('password'),
			email='johndoe@example.com',
			permission_level='admin',
			name='John Doe',
			address={ 'street': '123 Some Street', 'city':'Somewhere','state':'TX','zip':'75000' }
			)
		
		# commit the info to the index
		user.commit()

		# (id is created automatically unless specified)
		
		# data is retrieved using a collection class

		# search for a user by id
		user_by_id = Users().get(user.id)

		# search by another field and return 1 
		user_by_username = Users().filter_by(username='johndoe').one()

		# search by multiple fields
		user_by_fields = Users().filter_by(username='johndoe', email='johndoe@example.com').one()

		# or chain search conditions together
		user_by_fields = Users().filter_by(username='johndoe').filter_by(email='johndoe@example.com').one()

		# specify boolean conditions. ( all() gets all related records for the page)
		users = Users().filter_by(username='johndoe', email='quazimoto@example.com', condition='or').all()

		# find out how many records match the criteria in the entire index
		user_count = Users().filter_by(username='johndoe', email='quazimoto@example.com', condition='or').count()

		# or using len()
		user_count = len(Users().filter_by(username='johndoe', email='quazimoto@example.com', condition='or'))

		# nested objects can automatically be searched as well
		users = Users().filter_by(city='Somewhere').all()

Velociwrapper can do many more things. Read on!

-----

Dear God, Why?
--------------

Like most things it started off as a useful tool and took on a life of its own.
We had a ton of code written around SQLAlchemy but wanted the power and convience of
ElasticSearch. We started off mapping results to objects and then added methods that make
writing most searches easier.

Configuration
-------------

*velociwrapper.config.dsn*

A list of nodes to connect to. Each node can be a string hostname or a dict with options. 
See http://elasticsearch-py.readthedocs.org/en/master/api.html#elasticsearch.Elasticsearch for valid values. 
(sets the value of the ``hosts`` parameter).  Defaults to ``localhost``.

*velociwrapper.config.connection_params*

A ``dict`` of additional parameters to pass to the client connection. 
See http://elasticsearch-py.readthedocs.org/en/master/api.html#elasticsearch.Elasticsearch
Defaults to ``{}``

*velociwrapper.config.default_index*

A string index to use if it is not specified in the model. Defaults to ``es_model``

*velociwrapper.config.bulk_chunk_size*

A few calls such as ``VWCollection.delete()``, ``VWCollection.commit()``, or  ``Mapper.reindex()`` can act on
large collections. The ``bulk_chunk_size`` tells Elasticsearch how many records to operate on at a time.
Defaults to 1000

*velociwrapper.config.results_per_page*

For performance reasons Elasticsearch will not return large numbers of documents in a single call. As such
return values are limited. This value is the default results but you can also pass the parameter to ``all()``
to change the result for a single value. Defaults to 50

*velociwrapper.config.strict_types*

Perform type checks when creating objects. When ``True`` velociwrapper will throw an exception if the value
you're setting doesn't match the attribute's assigned type.

Configuration using environment variables
-----------------------------------------

All configuration variables can be set via the environment. 

``VW_DSN`` maps to ``dsn``. Can be a comma separated string or JSON

``VW_CONNECTION_PARAMS`` maps to ``connection_params``. Must be JSON

``VW_DEFAULT_INDEX`` maps to ``default_index``.  String

``VW_BULK_CHUNK_SIZE`` maps to ``bulk_chunk_size``

``VW_RESULTS_PER_PAGE`` maps to ``results_per_page``

----

Types
------------------

Elasticsearch is extremely flexible when it comes to adding types but less forgiving about changing them. To
help with this we created a metaclass called ``ESType`` to define mappings used in Elasticsearch. The types are 
used when ``strict_types`` is on and both the mapping options and types are used when creating or reindexing the
indicies.  The mapping options are set in the metaclass, otherwise the types subclass normal Python types and 
are used the same way.

Using Velociwrapper's types is completely optional. If you define the models using normal Python types, everything
will work as expected. The biggest drawback is that Velociwrapper will not automatically be able to use filter
syntax on ``not_analyzed`` string fields.

All defaults in Velociwrapper's types are set to Elasticsearch's defaults:
http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/mapping-core-types.html

**Available Types**

*String([str],\*\*kwargs):*
	
Keyword nts:

- *store*
- *index*
- *doc_values*


