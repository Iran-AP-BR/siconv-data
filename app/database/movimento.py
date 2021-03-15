# coding: utf-8
"""
   """

from . import db
from .associations import convenios_emendas_association
from uuid import uuid4

class Movimento(db.Model):

    __tablename__ = 'movimento'

    MOV_ID = db.Column(db.BigInteger, unique=True, nullable=False, primary_key=True, autoincrement=True)
    DATA = db.Column(db.DateTime, unique=False, nullable=True)
    VALOR = db.Column(db.Float, unique=False, nullable=True)
    TIPO = db.Column(db.String, unique=False, nullable=False)
    IDENTIF_FORNECEDOR = db.Column(db.String, unique=False, nullable=False)
    NOME_FORNECEDOR= db.Column(db.String, unique=False, nullable=False)

    NR_CONVENIO = db.Column(db.String, db.ForeignKey('convenios.NR_CONVENIO'), unique=False, nullable=False)

    def __init__(self, **kwargs):

        self.NR_CONVENIO = kwargs.get('NR_CONVENIO')
        self.DATA = kwargs.get('DATA')
        self.VALOR = kwargs.get('VALOR')
        self.TIPO = kwargs.get('TIPO')
        self.IDENTIF_FORNECEDOR = kwargs.get('IDENTIF_FORNECEDOR')
        self.NOME_FORNECEDOR = kwargs.get('NOME_FORNECEDOR')

    def __repr__(self):
        return "<Movimento(TIPO={self.TIPO!r})>".format(self=self)

