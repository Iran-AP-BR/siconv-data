# coding: utf-8
"""commands.
   """
from flask import current_app as app


def parser(line, default):
    line = line.strip()
    line = line.lower() if line is not None else ''
    command = default
    argument = line
    negation = False
    params = line.split(app.config.get('GRAPHQL_COMMAND_DELIMITER'), 1)
    if len(params) > 1:
        command = params[0]
        argument = params[1]

    if command[0] == app.config.get('GRAPHQL_NEGATION_MARK'):
        command = command[1:]
        negation = True

    return command.strip(), argument.strip(), negation


class Command():
    def __init__(self, name, data_type="*", split=False, split_length=0, resolver=None, default=None):
        assert resolver

        self.name = name.lower()
        self.data_type = data_type
        self.split = split
        self.split_length = split_length
        self.resolver = resolver
        self.default = default

        self.__exception_too_many_items__ = 'Muitos itens na lista.'
        self.__exception_too_few_items__ = 'Poucos itens na lista.'

        self.__type_float__ = 'float64'
        self.__type_int__ = 'int64'
        self.__type_date__ = 'datetime64[ns]'
        self.__type_string__ = 'str'

    def splitter(self, argument):
        argument = list(map(lambda x: x.strip(), argument.split(
            app.config.get('GRAPHQL_LIST_DELIMITER'))))
        if self.split_length > 0:
            if len(argument) > self.split_length:
                raise Exception(
                    f'{self.name}: {self.__exception_too_many_items__}')

            if len(argument) < self.split_length:
                raise Exception(
                    f'{self.name}: {self.__exception_too_few_items__}')

        return argument

    def get_condition(self, field, line, dtypes, parse_dates=[]):
        line = line.strip()
        field_type = self.__type_date__ if field in parse_dates else dtypes[field]
        str_compatible = (field not in parse_dates and dtypes[field] not in [
                          'float64', 'int64']) or self.data_type != 'str'

        command, argument, negation = parser(
            line, self.default if self.default else self.name)

        if not str_compatible:
            command = self.default if self.default else self.name

        if command != self.name:
            return None

        if self.split:
            argument = self.splitter(argument)

        if field_type.lower() == self.__type_float__:
            argument = list(map(lambda x: float(x if x != '' else 0), argument)) if type(
                argument) == list else float(argument if argument != '' else 0)

        elif field_type.lower() == self.__type_int__:
            argument = list(map(lambda x: int(x if x != '' else 0), argument)) if type(
                argument) == list else int(argument if argument != '' else 0)

        elif field_type.lower() == self.__type_date__:
            argument = argument if type(argument) == list else f"'{argument}'"
        else:
            field = f'{field}.str.lower()'
            argument = argument if type(argument) == list else f"'{argument}'"

        return self.resolver(field, argument, negation)
