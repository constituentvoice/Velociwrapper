# register callbacks for Velociwrapper events

callbacks = {}

def register_callback( cbtype, callback ):
	if cbtype not in callbacks:
		callbacks[cbtype] = []
	
	if not callable(callback):
		raise ValueError( 'parameter 2 to register_callback() must be callable' )

	callbacks[cbtype].append( callback )

def execute_callbacks( cbtype, argument, **kwargs ):
	try:
		for cb in callbacks[cbtype]:
			argument = cb( argument, **kwargs )
	except KeyError:
		pass # no callbacks by this name. 

	return argument
