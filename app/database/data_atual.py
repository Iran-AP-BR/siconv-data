# coding: utf-8
"""data_atual table.
   """

from . import db


class DataAtual(db.Model):

    __tablename__ = 'data_atual'

    DATA_ATUAL = db.Column(db.DateTime, unique=True, nullable=False, primary_key=True)

    def __init__(self, **kwargs):

        self.DATA_ATUAL = kwargs.get('DATA')

    def __repr__(self):
        return "<DataAtual(DATA_ATUAL={self.DATA_ATUAL!r})>".format(self=self)

