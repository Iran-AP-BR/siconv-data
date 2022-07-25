# coding: utf-8

class CSVUpToDateException(Exception):
    """Custom exception to be raised if data is up to date in csv files"""
    
    def __init__(self, expression='', message=''):
        self.expression = expression
        self.message = message

class CSVUnchangedException(Exception):
    """Custom exception to be raised if data was not changed in csv files"""
    
    def __init__(self, expression='', message=''):
        self.expression = expression
        self.message = message
