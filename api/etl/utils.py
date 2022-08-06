# coding: utf-8

from dateutil.parser import parse as date_parse


def datetime_validation(txt, logger=None, dayfirst=True):
    try:
        dtime = date_parse(txt, dayfirst=dayfirst).date()
    
    except Exception as e:
        if logger:
            logger.error(str(e))
        return None

    return dtime

def feedback(logger, label='', value=''):
    label_length = 30
    value_length = 30
    label = label + ' ' + '-'*label_length
    value = '-'*value_length + ' ' + value
    logger.info(label[:label_length] + value[-value_length:])
