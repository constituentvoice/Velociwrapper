class relationship(object):
    def __init__(self,ref_model,**kwargs):
        self.ref_model_str = ref_model
        self.ref_model = None

        # reltype is one or many
        self.reltype = 'one'
        if kwargs.get('_type'):
            self.reltype = kwargs.get('_type')
            del kwargs['_type']
        
        self.params = kwargs
    
    def _find_related_class(self,name,cls=None):
        if not cls:
            cls = VWBase
        
        classes = {}
        for sc in cls.__subclasses__():
            if name == sc.__name__:
                return sc
            else:
                possible = self._find_related_class(name,sc)
                if possible and possible.__name__ == name:
                    return possible
        
        return None

    
    # returns the relationship lookup values for a given instance
    def get_relational_params(self,cur_inst):
        dict_params = {}
        for k,v in self.params.iteritems():
            dict_params[k] = getattr(cur_inst,k)

        return dict_params

    def get_reverse_params(self,cur_inst,new_obj):
        dict_params = {}

        for k,v in self.params.iteritems():
            # catching of attributeerror maybe unintended consequence
            if new_obj and isinstance(new_obj,VWBase):
                dict_params[k] = getattr(new_obj,v)
            else:
                dict_params[k] = None

    def execute(self, cur_inst):
        
        # first pass we'll need the reference model
        if not self.ref_model:
            self.ref_model = self._find_related_class(self.ref_model_str) 

        if not self.ref_model:
            raise AttributeError('Invalid relatonship. Could not find %s.' % self.ref_model_str )

        c = VWCollection(base_obj=self.ref_model)
        filter_params = {}
        possible_by_id = False
        for k,v in self.params.iteritems():
            column_value = getattr(cur_inst,k)

            # we can't do anything unless there's a value for the column
            # this will allow us to create blank classes properly
            if column_value:
                if v == 'id':
                    possible_by_id = column_value
                else:
                    if type(column_value) == list:
                        or_values = []
                        
                        # let's be a bit magical
                        for item in column_value:
                            if isinstance(item,dict) and item.get('id'):
                                or_values.append(v + "=" + "'" + item.get('id') + "'") # look for  dictionaries that have 
                            elif isinstance(item,basestring):
                                or_values.append(v + "=" + "'" + item + "'")
                            else:
                                raise AttributeError('Unable to parse relationship')


                    else:
                        filter_params[v] = getattr(cur_inst,k)

        value = None
        
        if not filter_params and possible_by_id:
            if type( possible_by_id ) == list:
                value = c.get_in(possible_by_id)
            else:
                value = c.get(possible_by_id)
        else:
            srch = c.filter_by(**filter_params)

            if self.reltype == 'one':
                try:
                    value = srch.one()
                except NoResultsFound:
                    pass
            else:
                value = srch.all()

        return value
    
