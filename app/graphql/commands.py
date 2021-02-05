# coding: utf-8
"""commands.
   """

class Command():
    def __init__(self, name, data_type="*", split=False, split_length=0, resolver=None, default=None):
        assert resolver

        self.name = name.lower()
        self.data_type = data_type
        self.split = split
        self.split_length = split_length
        self.resolver = resolver
        self.default = default

        self.__command_delimiter__ = '$'
        self.__list_delimiter__ = '|'
        self.__negation_mark__ = '!'

        self.__exception_too_many_items__ = 'Muitos itens na lista.'
        self.__exception_too_few_items__ = 'Poucos itens na lista.'

        self.__type_float__ = 'float64'
        self.__type_int__ = 'int64'
        self.__type_date__ = 'datetime64[ns]'
        self.__type_string__ = 'str'

    def get_condition(self, field, line, str_compatible, field_type):
        negation = False
        argument = line.lower()
        params = line.lower().split(self.__command_delimiter__)

        if len(params) == 1:
            command = self.default
        else:
            command = params[0]
            argument = line.lower()[len(command)+1:]

        if command[0] == self.__negation_mark__:
            command = command[1:]
            negation = True
        
        if not str_compatible:
            command = self.default if self.default else self.name

        if command != self.name:
            return None
        
        if self.split:
            argument = list(map(lambda x: x.strip(), argument.split(self.__list_delimiter__)))
            if self.split_length > 0:
                if len(argument) > self.split_length:
                    raise Exception(self.__exception_too_many_items__)
                
                if len(argument) < self.split_length:
                    raise Exception(self.__exception_too_few_items__)

        if field_type.lower() == self.__type_float__:
            argument = list(map(lambda x: float(x), argument)) if type(argument) == list else float(argument)

        elif field_type.lower() == self.__type_int__:
            argument = list(map(lambda x: int(x), argument)) if type(argument) == list else int(argument)

        elif field_type.lower() == self.__type_date__:
            argument = argument if type(argument) == list else f"'{argument}'"
        else:
            field = f'{field}.str.lower()'
            argument = argument if type(argument) == list else f"'{argument}'"
        
        return self.resolver(field, argument, negation) 

