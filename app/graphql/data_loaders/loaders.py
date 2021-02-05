# coding: utf-8
"""loaders.
   """

import pandas as pd
from functools import reduce
from . import page_settings, pagination, load_data

def load_convenios(page=1, page_length=1, _emendas=None, parameters={}):
    dtypes = {'NR_CONVENIO': 'object',
            'DIA_ASSIN_CONV': 'object',
            'SIT_CONVENIO': 'object',
            'INSTRUMENTO_ATIVO': 'object',
            'DIA_PUBL_CONV': 'object',
            'DIA_INIC_VIGENC_CONV': 'object',
            'DIA_FIM_VIGENC_CONV': 'object',
            'DIA_LIMITE_PREST_CONTAS': 'object',
            'VL_GLOBAL_CONV': 'float64',
            'VL_REPASSE_CONV': 'float64',
            'VL_CONTRAPARTIDA_CONV': 'float64',
            'COD_ORGAO_SUP': 'object',
            'DESC_ORGAO_SUP': 'object',
            'NATUREZA_JURIDICA': 'object',
            'COD_ORGAO': 'object',
            'DESC_ORGAO': 'object',
            'MODALIDADE': 'object',
            'IDENTIF_PROPONENTE': 'object',
            'OBJETO_PROPOSTA': 'object'}

    parse_dates = ['DIA_ASSIN_CONV', 'DIA_PUBL_CONV', 'DIA_INIC_VIGENC_CONV', 'DIA_FIM_VIGENC_CONV', 'DIA_LIMITE_PREST_CONTAS']
    
    convenios = load_data('convenios', dtypes=dtypes, parse_dates=parse_dates)
    emendas_convenios = load_data('emendas_convenios')
    emendas = load_data('emendas')
    
    q = []
    if _emendas is not None:
        if _emendas is True:
            q += [(convenios['NR_CONVENIO'].isin(emendas_convenios['NR_CONVENIO'].to_list()))]
        elif _emendas is False:
            q += [(~convenios['NR_CONVENIO'].isin(emendas_convenios['NR_CONVENIO'].to_list()))]
        
    convenios = filter_constructor(parameters=parameters, data_frame=convenios, dtypes=dtypes, parse_dates=parse_dates, initial_conditions=q)

    items_count, page_count, idx_first, idx_last = page_settings(convenios, page, page_length)

    convenios = convenios[idx_first:idx_last]
    convenios_d = convenios.to_dict('records')

    for i in range(len(convenios_d)):
        ec = emendas_convenios[emendas_convenios['NR_CONVENIO'] == convenios_d[i]['NR_CONVENIO']]
        convenios_d[i]['EMENDAS'] = emendas[emendas['NR_EMENDA'].isin(ec['NR_EMENDA'].to_list())].to_dict('records')
    
    return convenios_d, pagination(page, page_length, page_count, items_count)

def load_emendas(page=1, page_length=1, parameters={}):
    emendas = load_data('emendas')
    emendas_convenios = load_data('emendas_convenios')
    convenios = load_data('convenios')

    emendas = filter_constructor(parameters=parameters, data_frame=emendas)

    items_count, page_count, idx_first, idx_last = page_settings(emendas, page, page_length)
    emendas = emendas[idx_first:idx_last]

    emendas_d = emendas.to_dict('records')
    for i in range(len(emendas_d)):
        ec = emendas_convenios[emendas_convenios['NR_EMENDA'] == emendas_d[i]['NR_EMENDA']]
        emendas_d[i]['CONVENIOS'] = convenios[convenios['NR_CONVENIO'].isin(ec['NR_CONVENIO'].to_list())].to_dict('records')

    return emendas_d, pagination(page, page_length, page_count, items_count)


def filter_constructor(parameters, data_frame, dtypes=None, parse_dates=[], initial_conditions=[]):
    assert type(initial_conditions) == list
    
    command_types = {
        'eq': {'type': '*', 'split': False, 'split_length': 0},
        'ct': {'type': 'str', 'split': False, 'split_length': 0},
        'sw': {'type': 'str', 'split': False, 'split_length': 0},
        'ew': {'type': 'str', 'split': False, 'split_length': 0},
        'gt': {'type': '*', 'split': False, 'split_length': 0},
        'lt': {'type': '*', 'split': False, 'split_length': 0},
        'in': {'type': '*', 'split': True, 'split_length': 0},
        'bt': {'type': '*', 'split': True, 'split_length': 2}
    }
   
    conditions = initial_conditions
    for key in parameters.keys():
        negation = False
        command = 'eq'
        value = parameters.get(key).lower()
        params = parameters.get(key).lower().split('$')
        if len(params) > 1:
            command = params[0]
            value = parameters.get(key).lower()[len(command)+1:]

        if command[0] == '!':
            command = command[1:]
            negation = True

        if command not in command_types.keys():
            raise Exception('Comando inválido.')

        if command_types[command]['type'] == 'str' and (key in parse_dates or dtypes[key] in ['float64', 'int64']):
            command = 'eq'
        
        if command_types[command]['split']:
            value = value.split('|')
            if command_types[command]['split_length'] > 0:
                if len(value) > command_types[command]['split_length']:
                    raise Exception('Muitos itens na lista.')
                
                if len(value) < command_types[command]['split_length']:
                    raise Exception('Poucos itens na lista.')

        if key in parse_dates:
            argument = data_frame[key]
        elif dtypes[key] == 'float64':
            argument = data_frame[key]
            value = list(map(lambda x: float(x), value)) if type(value) == list else float(value)

        elif dtypes[key] == 'int64':
            argument = data_frame[key]
            value = list(map(lambda x: int(x), value)) if type(value) == list else int(value)

        else:
            argument = data_frame[key].str.lower()

        if command == 'eq':
            conditions += [~(argument==value)] if negation else [(argument==value)]
        elif command == 'gt':
            conditions += [~(argument>value)] if negation else [(argument>value)]
        elif command == 'lt':
            conditions += [~(argument<value)] if negation else [(argument<value)]
        elif command == 'ct':
            conditions += [~(argument.str.contains(value))] if negation else [(argument.str.contains(value))]
        elif command == 'sw':
            conditions += [~(argument.str.startswith(value()))] if negation else [(argument.str.startswith(value()))]
        elif command == 'ew':
            conditions += [~(argument.str.endswith(value.lower()))] if negation else [(argument.str.endswith(value.lower()))]
        elif command == 'in':
            conditions += [~(argument.isin(value))] if negation else [(argument.isin(value))]
        elif command == 'bt':
            conditions += [~(argument.between(value[0], value[1]))] if negation else [(argument.between(value[0], value[1]))]
        else:
            raise Exception('Comando desconhecido.')
    
    if len(conditions) > 1:
        return data_frame[reduce(lambda x, y: x & y, conditions)]

    elif len(conditions) == 1:
        return data_frame[conditions[0]]
    
    return data_frame
