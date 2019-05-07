Velociwrapper
=============

Velociwrapper is a wrapper to create ORM like features around Elasticsearch indexes.
Velociwrapper is not a true ORM since Elasticsearch isn't a relational database

Installation
------------

::

    pip install Velociwrapper

Or download manually and run setup.py 

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
        Mapper().create_indices() # creates all defined VWBase models

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

**Configuration using environment variables**

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
indices.  The mapping options are set in the metaclass, otherwise the types subclass normal Python types and 
are used the same way.

Using Velociwrapper's types is completely optional. If you define the models using normal Python types, everything
will work as expected. The biggest drawback is that Velociwrapper will not automatically be able to use filter
syntax on ``not_analyzed`` string fields.

All defaults in Velociwrapper's types are set to Elasticsearch's defaults:
http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/mapping-core-types.html

In cases where the option begins with "_" Velociwrapper requires the underscore be appended rather than prepended.

**Available Types**

**String** *([str],\*\*kwargs)*
    
Keyword args:

- ``analyzed``
- ``norms``
- ``index_options``
- ``analyzer``
- ``index_analyzer``
- ``search_analyzer``
- ``ignore_above``
- ``position_offset_gap``
- ``value_``
- ``boost_``

The ``analyzed`` argument maps to ``index=analyzed|not_analyzed`` default is ``analyzed``

**Number** *([number], \*\*kwargs)*

Generic number type. Normally you should use the number type classes that derive from this. If ``type`` is omitted
defaults to ``float``

Keyword args:

- ``type``
- ``index_``
- ``precision_step``
- ``ignore_malformed``
- ``coerce``

The following types use the same arguments (except for type which is specified automatically)

- ``Float`` *([float], \*\*kwargs)*
- ``Integer`` *([int], \*\*kwargs)*
- ``Long`` *([float], \*\*kwargs)*
- ``Short`` *([float], \*\*kwargs)*
- ``Byte`` *([float], \*\*kwargs)*
- ``Tokencount`` *([number],\*\*kwargs)*

**Date** *([date|str] | [year int, month int, day int], \*\*kwargs)* and **DateTime** *([datetime|str] | [year int, month int, day int, [hour int, [minute int,[second int, [microsecond int]]]]], \*\*kwargs)*

Keyword args:

- ``format``
- ``precision_step``
- ``ignore_malformed``

**Array** - new in 1.0.8

Special type that specifies a list of items that are a single type. Accepts any keyword argument above. ``type_`` keyword specifies the type to be used. Default is string

**Binary** *()*

Experimental. Keyword arguments:

- ``compress``
- ``compress_threshold``

**IP** *([str])*

Keyword args:

- ``precision_step``

**GeoShape** / **GeoPoint**

Experimental. Will work as regular objects as well.

----

Type Functions
--------------

**create_es_type** *(value)*

Takes ``value`` and returns the equivalent Elasticsearch type. If an appropriate type cannot be determined then the value itself is returned.

----

Models
---------------

Create a model by defining the name of the model and extending ``VWBase`` (or a subclass of ``VWBase``).
Properties for the model should be statically defined. They can be ESTypes as described above or as regular
Python types. Values set in the model are defaults in each instance.

The ``__type__`` attribute is required and maps to the Elasticsearch ``doctype``. ``__index__`` is recommended
but if it is not present then the value of ``velociwrapper.config.default_index`` is used.

Example:

::

    class User(VWBase):
        __index__ = 'user_index'
        __type__ = 'user'
        username = String(analyzed=False)
        password = String(analyzed=False)
        email = String(analyzed=False)
        name = String()
        profile_image = String('default.jpg')


Or without using ESTypes:

::

    class User(VWBase):
        __index__ = 'user_index'
        __type__ = 'user'
        username = ''
        password = ''
        email = ''
        name = ''
        profile_image = ''

The added benefit of using ESTypes is specifying the mappings. This helps velociwrapper know what kind of searches to build
and can create the mappings for you, if you haven't specified them yourself.

Once models are created they must be committed to save into the Elasticsearch cluster

::

    u = User(
        username='jsmith', 
        password=crypt_method('password123'), 
        email='jsmith@example.com', 
        name='John Smith', 
        profile_image='jsmith.jpg'
        )

    u.commit()

The call to ``commit()`` generates an id for the document. If you want to explicitly set the id first, you can set the id attribute:

::

    u = User( ... )
    u.id = 'my-unique-id'
    u.commit()

*Be careful!*. IDs have to be unique across all types in your index. If your ID is not unique, the ID specified will be updated by
your new data. It is recommended to let Velociwrapper handle ID creation unless you're certain of what you're doing.

**Model API**

**collection** *()*

Returns a ``VWCollection`` for this model. If a custom subclass has been defined it will be returned. Otherwise a new collection will be created.

**commit** *()*

Commits the model to Elasticsearch. New models will be created as new documents. Existing models will be updated.

**delete** *()*

Deletes the cooresponding document from Elasticsearch. New operations cannot be performed on the model once it is marked
for delete.

**sync** *()*

Syncs the document in Elasticsearch to the model. Overwrites any uncommitted changes.

**to_dict** *()*

Converts the model to a dictionary. Very useful for outputting models to JSON web services. This method is intended to be overridden for
custom output.

**more_like_this** *()*

Performs a search to get documents that are "like" the current document. Returns a VWCollectionGen.

----

Collections
------------

Collections are used to search and return collections of models. Searches can be chained together to create complex queries of Elasticsearch
(much like SQLAlchemy). Currently collections are of one document type only. This may change in a future release.

Example:
    
::

    # all users named john
    users = Users().filter_by(name='John').all()

    # users named john who live in texas
    users = Users().filter_by(name='John', state='TX').all()

    # another way to write the same as above
    users = Users().filter_by(name='John').filter_by(state='TX').all()

By default chained criteria are joined with "AND" ("must" in most cases internally). But can be controlled:

::

    # users who live in texas or are named john:
    users = Users().filter_by(name='John', state='TX', condition='or').all()

For more complex queries see the ``raw()`` method and the QDSL module.

**Creating Collections**

Created a collection by calling ``Model().collection()``. If a subclass of the collection exists it will be created and returned
otherwise an base collection will be created for the model by calling ``VWCollection(baseobj=Model)``.  ``collection()`` is 
convienent because it allows collections and models to be defined in separate files without recursive import errors.

When creating a subclass for a collection, specify the model using the ``__model__`` property.

::

    class Users(VWCollection):
        __model__ = User

**Conditions**

Conditions in Elasticsearch are a little tricky. Internally the ``bool`` queries / filters are used. Instead of the traditional
``and``, ``or``, ``not``. Elasticsearch uses ``must``, ``should`` and ``must_not``. To make things a bit more interesting the
traditional boolean values exist as well and Elasticsearch recommends they be used is certain cases (such as geo filters) 
Velociwrapper converts ``and``, ``or``, ``not`` to the Elasticsearch equivalents except in the case of ``search_geo()``.

The ``must``, ``should``, ``must_not`` options can be used instead and will work. ``minimum_should_match`` is also available. If 
the explicit options are needed you can use ``explicit_and``, ``explicit_or``, and ``explicit_not``.

Conditions can become complex very quickly. Velociwrapper tries to take a "do what I mean" approach to chained conditions. First
the current filter is checked for a specific condition. If no condition exists then the *preceeding* condition is used. If there
is no preceeding condition, the condition is set to and/must by default.

Examples:

::

    # get users in named John or Stacy  
    users = Users().filter_by(name='John').filter_by(name='Stacy', condition='or').all()

    # equivalent because the second filter_by() will use the preceeding or condition:
    users = Users().filter_by(name='John', condition='or').filter_by(name='Stacy').all()

    # add another condition, such as state, might not always do what we expect. This would return anyone
    # who's name is stacy or john or lives in Texas
    users = Users().filter_by(name='John').filter_by(name='Stacy', condition='or').filter_by(state='TX').all()

    # (john or stacy) and state
    users = Users().filter_by(name='John').filter_by(name='Stacy', condition='or').filter_by(state='TX',condition='and').all()

Obviously order matters. For more complex queries the other option is to use the ``raw()`` method and the QDSL module (see below)

**API**

Methods marked chainable internally change the search query to affect the output on ``all()``, ``delete()``, and ``one()``. Chainable methods can be
called multiple times with different parameters.

**all** *(\*\*kwargs)*

Executes the current search and returns ``results_per_page`` results. (default 50). ``results_per_page`` is specified in ``velociwrapper.config.results_per_page``
but can also be specified by keyword arguments. 

If no search has been specified, Velociwrapper will call ``match_all``.

If no results are matched ``all()`` returns an empty VWCollectionGen.

Arguments:

- ``results_per_page`` *int*: number of results to return
- ``size`` *int*: same as results_per_page
- ``start`` *int*: Record count to start with

**clear_previous_search** *()*

Clear all search parameters and reset the object. Even after a call to an output method the search can be output again. This allows the collection to be reused.
Generally its better to create a new object.

**commit** *([callback=callable])*

Bulk commits a list of items specified on ``__init__()`` or if no items were specified will bulk commit against the items matched in the current search. (be careful! Calling something like Users().commit() will commit all users!)

The ``callback`` argument should be a callable. The raw item will be passed to it and it must return either a ``dict`` or a ``VWBase`` 
(model) object.  Note that velociwrapper does not call each model's ``commit()`` or ``to_dict()`` methods but rather issues the request
in bulk. Thus you cannot affect the behavior by overriding these methods. Use the ``callback`` to make changes or change the items before
passing them to the collection.

As of 2.0 it is also possible to register a callback to manipulate items in the commit. See "Callbacks".

**count** *()*

Returns the total number of documents matched (not that will be returned!) by the search. 

**delete** *(\*\*kwargs)*

Delete the records specified by the search query.

**delete_in** *(ids=list)*

Delete the records specified by a list of ids. Equivalent to:

::

    Users().filter_by(ids=list_of_ids).delete()

**exact** *(field=str, value=mixed)*

Chainable. Find records where ``field`` is the exact ``value``. String based fields **must** be specified as ``not_analyzed`` in the index. Otherwise results
may not be as expected.  ``exact()`` is more for completeness. ``filter_by()`` uses exact values when available. The only difference is ``exact()``
will warn if the field cannot be searched while ``filter_by()`` silently converts to a query.

Keyword arguments:

- ``boost`` *float*: An explicit boost value for this boolean query
- ``condition`` *str*: "and","or","not","explicit_and","explicit_or","explicit_not",
- ``minimum_should_match`` *int*: When executing a should (or) query, specify the number of options that should match to return the document. Default = 1
- ``with_explicit`` *str*: "and","or","not". Only used if explicit conditions exist and there's a question of how an additional condtion should be added to the query. 

**exists** *(field, [kwargs])*

Chainable. Find records if the specified field exists is the document.

Keyword arguments:

- ``boost`` *float*: An explicit boost value for this boolean query
- ``condition`` *str*: "and","or","not","explicit_and","explicit_or","explicit_not",
- ``minimum_should_match`` *int*: When executing a should (or) query, specify the number of options that should match to return the document. Default = 1
- ``with_explicit`` *str*: "and","or","not". Only used if explicit conditions exist and there's a question of how an additional condtion should be added to the query. 

**filter_by** *([condition], kwargs)*

Chainable. Filter or query elasticsearch for ``field="search"``. Automatically creates filters or queries based on field mappings. If the ``search`` parameter is a list, filter_by will create
an ``in()`` filter / query. ``condition`` can be set as the first argument or passed as a keyword argument.

Keyword arguments

- ``[field]`` *str*: A field in the document set to the value to try to find.
- ``id`` *value*: Explicitly search for particular id. 
- ``ids`` *list*: Explicitly search for using a list of ids. 
- ``boost`` *float*: An explicit boost value for this boolean query
- ``condition`` *str*: "and","or","not","explicit_and","explicit_or","explicit_not",
- ``minimum_should_match`` *int*: When executing a should (or) query, specify the number of options that should match to return the document. Default = 1
- ``with_explicit`` *str*: "and","or","not". Only used if explicit conditions exist and there's a question of how an additional condtion should be added to the query. 

**multi_match** *(fields=list,query=str,\*\*kwargs)*

Chainable. Search the list of fields for the value of query. Accepts standard kwargs arguments.

**get** *(id=value)*

Returns the single record specified by ``id`` or ``None`` if it does not exist.

**get_in** *(ids=list)*

Returns a list of records specified by the list of ids or an empty list if no ids exist. Note this method cannot be sorted. If sorting is needed it is better to call

::

    filter_by(ids=list).sort(...).all()

**get_like_this** *(id)*

Returns records like the document specified by id or an empty list if none exists. Note this method cannot be sorted.

**\_\_init\_\_** *([items=list],[\*\*kwargs])*

Create a collection. If ``items`` are specified they are stored internally to ``commit()`` in bulk. Stored items must be models (subclassing ``VWBase``) or ``dict``.

Keyword arguments:

- ``bulk_chunk_size`` *int*: override default chunk size for this collection
- ``results_per_page`` *int*

**\_\_len\_\_** *()*

Same as ``count()``. Allows for the entire collection to be passed to ``len()``

**missing** *(field=str,\*\*kwargs)*

Chainable. Finds records where the specified ``field`` is missing

Keyword arguments:

- ``boost`` *float*: An explicit boost value for this boolean query
- ``condition`` *str*: "and","or","not","explicit_and","explicit_or","explicit_not",
- ``minimum_should_match`` *int*: When executing a should (or) query, specify the number of options that should match to return the document. Default = 1
- ``with_explicit`` *str*: "and","or","not". Only used if explicit conditions exist and there's a question of how an additional condtion should be added to the query. 

**one** *()*

Executes the search and returns the first record only. Raises ``NoResultFound`` if the search did not match any documents.

**range** *(field=str, \*\*kwargs)*

Chainable. Filters the results by a range of values in ``field``. The keyword arguments coorespond to arguments used by the range filter
in Query DSL: http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-range-query.html

Other search keywords are available except for ``boost``. ``boost`` affects the range query itself. Keyword arguemtns are:

- ``gte`` *number or date*: greater than or equal
- ``gt`` *number or date*: greater than
- ``lte`` *number or date*: less than or equal
- ``lt`` *number or date*: less than
- ``boost`` *float*: boost value for the range query itself
- ``time_zone`` *str*: timezone offset. Only used if comparison is a date and doesn't contain a timezone offset already.
- ``condition`` *str*: "and","or","not","explicit_and","explicit_or","explicit_not",
- ``minimum_should_match`` *int*: When executing a should (or) query, specify the number of options that should match to return the document. Default = 1
- ``with_explicit`` *str*: "and","or","not". Only used if explicit conditions exist and there's a question of how an additional condtion should be added to the query. 

**raw** *(rawquery=dict)*

Execute a raw Query DSL query.  Chainable but all other search filters are ignored. Can still be used with ``sort()``.

***search** *(query=string)*

Execute a Lucene query against the server. Chainable.

**search_geo** *(field=str,distance=float,lat=float,lon=float,\*\*kwargs)*

Chainable. Filter the search based on distance from a geopoint.

- ``boost`` *float*: An explicit boost value for this boolean query
- ``condition`` *str*: "and","or","not","explicit_and","explicit_or","explicit_not",
- ``minimum_should_match`` *int*: When executing a should (or) query, specify the number of options that should match to return the document. Default = 1
- ``with_explicit`` *str*: "and","or","not". Only used if explicit conditions exist and there's a question of how an additional condtion should be added to the query. 

**sort** *(\*\*kwargs)*

Chainable (and can appear anywhere before an output method, including by having other filters chained to it). Arguments are ``field=asc|desc``. ``asc`` sorts the field
first to last. ``desc`` sorts the field last to first. ``asc`` is the default.

----

Additional Methods for ``VWCollectionGen``
------------------------------------------

``VWCollectionGen`` is returned by calls from ``VWCollection.all()`` and ``VWCollection.get_in()``. 

**results** *(self)*

Returns the underlying ElasticSearch results. Useful for getting meta information

Query Bodies with ``querybuilder.QueryBody`` 
--------------------------------------------

Underlying chainable methods is the ``querybuilder.QueryBody`` class. This class helps build simple query bodies
for Elasticsearch but attempts not to get too crazy. It stores an internal structure of the query and then
builds it into a ``dict`` that can be passed to the underlying Elasticsearch client.  The class is used internally
by ``VWCollection`` but you could use it directly to build queries to then pass to the ``raw()`` method.

``QueryBody`` only supports queries and filters. For other wrappers, such as constant_score, you'll need to manually
build the queries by hand or with the ``QDSL`` functions described below.

**QueryBody methods**

**chain** *(self, newpart=dict, \*\*kwargs)*

Chains a new part of the query into the existing query. Newpart must be a ``dict`` with additional query parameters
to pass to Elasticsearch. Note that ``newpart`` is not checked for correctness. 

Returns ``self`` so additional methods can be called. 

Keyword Arguments:

- ``type`` *string*: either "query" or "filter". If not specified checks ``newpart`` for one of these keywords. Otherwise uses "query"
- ``condition`` *string*: must|should|must_not|and|or|not. Defaults to "must". Specifies how this part of the query is treated in relation to the existing query
- ``with_explicit`` *string*: and|or|not. Included for legacy purposes. Overrides ``condition`` and is useful if a nested bool was manually created. Generally should not be used.

**is_filtered** *(self)*

Returns ``True`` if the current query body contains a filter.

**is_query** *(self)*

Returns ``True`` if the current query body contains a query other than ``match_all {}``

**build** *(self)*

Builds the current query into a representation understood by Elasticsearch. Returns ``dict``

----

QDSL and Building Raw Queries
-----------------------------

``velociwrapper.qdsl`` contains functions to help make writing QDSL easier.

**QDSL Functions**

**query** *(params=dict)*

Returns ``params`` wrapped by ``{ "query": params }``

**filter_** *(params=dict)*

Returns ``params`` wrapped by ``{ "filter": params }``.

Note the "_" appended to ``filter_`` to prevent confusion with Python's ``filter()``

**match** *(field=str,value=str|dict,\*\*kwargs)*

Returns ``{"match": { field: { "query": value } } }``

Additional keyword arguments should be Elasticsearch arguments on ``match``

**match_phrase** *(field=str,value=str|dict,\*\*kwargs)*

Equivalent to ``match(field,value,type="phrase")``

**match_phrase_prefix** *(field=str,value=str|dict,\*\*kwargs)*

Equivalent to ``match(field,value,type="phrase_prefix")``

**multi_match** *(query=str|dict, fields=list,\*\*kwargs)*

Returns ``{"multi_match": {"query": query, "fields": fields } }``

Additional keyword arguments should be Elasticsearch arguments on ``multi_match``

**bool_** *(\*args,\*\*kwargs)*

Args are any number of dicts containing "must", "should" or "must_not" keys. Note the appended "_" to prevent
confusion with Python's ``bool``.

Keyword arguments are Elasticsearch options for ``bool`` such as ``minimum_should_match``

Example:

::

    from velociwrapper.qdsl import bool_, must, must_not, match
    mybool = bool_(
        must( match('foo','some value') ), 
        must_not( match( 'bar', 'some other value' ) )
    )

Special Keyword arguments

- *__vw_set_current* *dict*: set a current ``bool`` dictionary that will be updated rather than creating a blank one.

**must** *(params=str|dict, value=str|dict|None,\*\*kwargs)*

Creates a ``must`` arguement for ``bool``. If params is a ``dict`` then it is passed on directly. If it is a string or value
is set then the params are treated as a field name and passed to ``term``.

Example:

::

    must( match('foo', 'some value' ) )
    # returns { "must": { "match": { "foo": {"query": "some value" } } } }

    must('foo', 'some value' ) )
    # returns { "must": { "term" { "foo": {"value": "some value" } } } }

**must_not** *(params=str|dict,value=str|dict|None,\*\*kwargs)*

Like ``must`` but uses "must_not"

**should** *(params=str|dict,value=str|dict|None,\*\*kwargs)*

Like ``must`` but uses "should"

**term** *(field=str, value=str,\*\*kwargs)*

Like ``match`` but for filters

**terms** *(field=str,value=list,\*\*kwargs)*

Like ``term`` but values are a list of strings to match in a field.

**boosting** *(\*args, \*\*kwargs)*

Similar to ``bool`` allows any number of dicts with the key ``positive`` or ``negative``. Keyword arguments are options
passed to ``boosting``

**positive** *(field,value)*

Returns ``{ "positive": { "term": { field: value } } }``

**negative** *(field,value)*

Returns ``{ "negative": {"term": { field:value } } }``

**common** *(field, value, \*\*kwargs)*

Returns ``{ "common": { field: { "query": value } } }``

Keyword arguments are passed as additional key values to the ``field`` dict.

**constant_score** *(\*args, \*\*kwargs)*

Arguments should be ``dict``. A single argument is wrapped directly by ``constant_score``. In the case of multiple arguments the function searches each
for ``query`` or ``filter`` keys to wrap in the output.

**filtered** *(\*args, \*\*kwargs)*

Arguments should be ``dict``. A single argument is wrapped directly by ``filtered``. In the case of multiple arguments the function searches each
for ``query`` or ``filter`` keys to wrap in the output.

Additional keyword arguments are set on the ``filtered`` dict.

**function_score** *(\*args, \*\*kwargs)*

Arguments should be ``dict``. A single argument is wrapped directly by ``function_score``. In the case of multiple arguments the function searches each
for ``query``, ``filter``, ``FUNCTION``, or ``functions`` keys to wrap in the output. No magic happens here to check the validity of the functions!

Keyword arguments are set on the ``function_score`` dict.

**fuzzy**

**ids**

**query_term**

**indices**

**match_all**

**more_like_this**

**nested**

**prefix**

**query_string**

**simple_query_string**

**range**

**regexp**

**span_term**

**span_first**

**span_multi**

**span_near**

**span_not**

**span_or**

**wildcard**

**and_**

**or_**

**not_**

**exists**

**geo_bounding_box**

**geo_distance**

**geo_range**

**geo_polygon**

**geo_shape**

**geohash_cell**

**has_child**

**has_parent**

**missing**

**script**

**type_**


----

Mapper
------

Use the mapper by importing it:

::

    from velociwrapper.mapper import Mapper

The Mapper class has utilities for managing the Elasticsearch index.

**Mapper API**

**get_index_map** *(\*\*kwargs)*

Searches for currently loaded VWBase models and returns the their indexes as defined by code, along with their mappings. The only keyword argument is ``index``, passed to specify 
a particular index or group of indexes (must be a ``str`` or ``list``).

**get_server_map** *(\*\*kwargs)*

*New in version 1.0.10*. Like *get_index_map()*, but returns the mapping as saved on the server.

**create_indices** *(\*\*kwargs)*

Creates indexes based on currently loaded VWBase models or for the index or indexes specified by the ``index`` keyword argument.

**get_index_for_alias** *(alias=str)*

Return the name of the index for the specified ``alias``. If ``alias`` is an index, then the same name will be returned.

**reindex** *(index=str,newindex=str,\*\*kwargs)*

Re-indexes the specified index to a new index. Useful for making model changes and then creating them in Elastic search

Keyword arguments

- ``alias_name`` *string*: specify a new alias name when re-mapping an alias. If omitted the previous alias name is used.
- ``remap_alias`` *bool*: Aliases the index under a new name. Useful for making on-the-fly changes

**describe** *(cls=class)*

Output the index mapping for a VWBase class.

----

Callbacks
---------

There are several events built-in to Velociwrapper on which you can register callbacks. 
Callbacks are registered at the class level so all instances will have the callback. You 
can also register multiple methods for the same event. Callbacks recieve the instance and 
a single (optional) argument. The argument is returned. In the case of multiple callbacks 
on an event, the callbacks are fired in the order they were registered. The return value 
from one method is passed to the next as the argument.

Example:

::

    from your_models import Document
    
    # check a user for entry in another database
    def doc_database_check( vwinst, argument=None ):
        if not doc_in_database(vwinst.id):
            insert_into_database( vwinst.id, vwinst.name, vwinst.content ) # or whatever
            return argument 

    Document.register_callback( 'after_commit', doc_database_check )

Callbacks are defined in the ``VWCallback`` class in base.py. ``VWCollection`` and ``VWBase`` 
derive from ``VWCallback``

**Callback API**

**register_callback** *(cls, callback_event=str, callback=callable)* *- classmethod*

Register a callback for the event *callback_event* on the collection or base class. This is a class method,
the callback becomes active on all instances.

**deregister_callback** *(cls, callback_event=str, callback=callable|str)* *- classmethod*

Deregister a callback for the event *callback_event* by its name or original function *callback*. Returns None
even if there was not a callback by the name or for the event.

**execute_callbacks** *(self, event=string, argument=None, \*\*kwargs)*

Executes the current instances callbacks for *event* in the order they were registered. Returns *argument*.
If no callback was registered for the event the method returns *None*

**Available Events**

*before_manual_create_model*

Executed when a model instance is created directly but not when the model is created as the result of a search.
*argument* is ignored, only the model's instance is passed. The event fires before mapping information is copied
from the class and before the id is created.  An example use is a custom ID creation method.

*after_manual_create_model*

Executed when a model instance is created directly but not when the model is created as the result of a search.
*argument* is ignored, only the model's instance is passed. The event fires after the class variables are copied
to the instance, *id* is created, and the *__index__* is set.

*on_delete*

Executed when ``commit()`` is called on a deleted instance. Fires just before the underlying DELETE to Elasticsearch.
Argument is ignored. Does not execute on a bulk delete call in ``VWCollection``.

*before_commit*

Executed before the INDEX call to Elasticsearch. The argument is ignored. Does not execute on bulk commit calls in
``VWCollection``.

*after_commit*

Executed after the INDEX call to Elasticsearch. The argument is ignored. Does not execute on bulk commit calls in
``VWCollection``.

*before_sync*

Executed before retrieveing the underlying document from Elasticsearch to sync the object. The argument is ignored. 

*after_sync*

Executed after variables from Elasticsearch have overwritten object attributes. The argument is ignored.

*before_query_build*

Executed just before a search query is created. The argument is the current ``QueryBody``.

*after_query_build*

Executes after the search query is created as a ``dict``. The argument is the ``dict`` to be passed to the Elasticsearch client.

*on_bulk_commit*

Executes for each item before being appended to a bulk commit operation. The argument is the item. The item can be a 
``dict`` source document or a ``VWBase`` object depending on what was passed to the collections items. (If the commit
criteria was a search query then ``VWBase`` objects are passed.

*before_auto_create_model*

Executes after a source document is retrieved from Elasticsearch but before the document is converted to a model
instance. Note this does not fire until accessed in the ``VWCollectionGen`` generator. The argument passed is the
source document

*after_auto_create_model*

Executes after the a source document result is converted to a model instance. Does not occur until the model 
instance is accessed in the generator. Due to the way the generator works the instance passed to the callback
is empty, while the argument is the newly created instance to manipulate.

**Creating New Events**

You can register your own events and fire them yourself.

::

    # register an event when your generic document is something specific
    def is_pdf(inst, argument=None, **kwargs):
        # do something
        return argument

    Document.register_callback( 'on_edit', is_pdf )

    # then somewhere in your code (maybe an edit function?)
    document_instance.execute_callbacks('on_edit')

----

AUTHOR
------

Chris Brown, Drew Goin and Boyd Hitt 

----

COPYRIGHT
---------

Copyright (c) 2015-2019 Constituent Voice LLC
