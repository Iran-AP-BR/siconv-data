# coding: utf-8
"""loaders.
   """

import pandas as pd
from functools import reduce
from . import page_settings, pagination, load_data

def load_convenios(page=1, page_length=1, _emendas=None, parameters={}):
    convenios = load_data('convenios')
    emendas_convenios = load_data('emendas_convenios')
    emendas = load_data('emendas')
    
    q = []
    if _emendas is not None:
        if _emendas is True:
            q += [(convenios['NR_CONVENIO'].isin(emendas_convenios['NR_CONVENIO'].to_list()))]
        elif _emendas is False:
            q += [(~convenios['NR_CONVENIO'].isin(emendas_convenios['NR_CONVENIO'].to_list()))]
        
    convenios = filter_constructor(parameters=parameters, data_frame=convenios, initial_conditions=q)

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


def filter_constructor(parameters, data_frame, initial_conditions=[]):
    assert type(initial_conditions) == list
    '''
    igual$
    !igual$
    contem$
    !contem$
    comeca com$
    !começa com$
    termina com$
    !termina com$
    maior que$
    !maior que$
    menor$
    !menor que$

    :: - separador
    
    = - igual
    != - diferente
    <=> - contém
    <!> - não contém
    ^ - começa com
    !^ - não começa com
    $ - termina com
    !$ - termina com

    > - maior
    >= - maior ou igual
    < - menor
    <= - menor ou igual
    '''
    conditions = initial_conditions
    for key in parameters.keys():
        command = parameters.get(key)[:3]
        value = parameters.get(key)[3:]
        if command == '===':
            conditions += [(data_frame[key].str.lower()==value.lower())]
        elif command == '!==':
            conditions += [(data_frame[key].str.lower()!=value.lower())]
        elif command == '<=>':
            conditions += [(data_frame[key].str.lower().str.contains(value.lower()))]
        elif command == '<!>':
            conditions += [~(data_frame[key].str.lower().str.contains(value.lower()))]
        elif command == '<<<':
            conditions += [(data_frame[key].str.lower().str.startswith(value.lower()))]
        elif command == '!<<':
            conditions += [~(data_frame[key].str.lower().str.startswith(value.lower()))]
        elif command == '>>>':
            conditions += [(data_frame[key].str.lower().str.endswith(value.lower()))]
        elif command == '!>>':
            conditions += [~(data_frame[key].str.lower().str.endswith(value.lower()))]
        else:
            conditions += [(data_frame[key].str.lower()==parameters.get(key).lower())]
    
    if len(conditions) > 1:
        return data_frame[reduce(lambda x, y: x & y, conditions)]

    elif len(conditions) == 1:
        return data_frame[conditions[0]]
    
    return data_frame
