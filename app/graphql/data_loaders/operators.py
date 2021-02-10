# coding: utf-8
"""commands.
   """

class Operator():
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

    def parser(self, line):
        line = line.lower() if line is not None else ''
        operator = self.default if self.default else self.name
        argument = line
        negation = False
        params = line.split(self.__command_delimiter__)
        if len(params) > 1:
            operator = params[0]
            argument = line[len(operator)+1:]

        if operator[0] == self.__negation_mark__:
            operator = operator[1:]
            negation = True
        
        return operator, argument, negation

    def splitter(self, argument):
        argument = list(map(lambda x: x.strip(), argument.split(self.__list_delimiter__)))
        if self.split_length > 0:
            if len(argument) > self.split_length:
                raise Exception(f'{self.name}: {self.__exception_too_many_items__}')
            
            if len(argument) < self.split_length:
                raise Exception(f'{self.name}: {self.__exception_too_few_items__}')

        return argument

    def get_condition(self, field, line, dtypes, parse_dates=[]):
        field_type = self.__type_date__ if field in parse_dates else dtypes[field]
        str_compatible = (field not in parse_dates and dtypes[field] not in ['float64', 'int64']) or self.data_type != 'str'

        operator, argument, negation = self.parser(line)
        
        if not str_compatible:
            operator = self.default if self.default else self.name

        if operator != self.name:
            return None

        if self.split:
            argument = self.splitter(argument)

        if field_type.lower() == self.__type_float__:
            argument = list(map(lambda x: float(x if x!='' else 0), argument)) if type(argument) == list else float(argument if argument!='' else 0)

        elif field_type.lower() == self.__type_int__:
            argument = list(map(lambda x: int(x if x!='' else 0), argument)) if type(argument) == list else int(argument if argument!='' else 0)

        elif field_type.lower() == self.__type_date__:
            argument = argument if type(argument) == list else f"'{argument}'"
        else:
            field = f'{field}.str.lower()'
            argument = argument if type(argument) == list else f"'{argument}'"
            
        return self.resolver(field, argument, negation) 

