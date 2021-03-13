# coding: utf-8
"""
   """

from . import db
from .associations import convenios_emendas_association

class Emenda(db.Model):

    __tablename__ = 'emendas'

    NR_EMENDA = db.Column(db.String, unique=True, nullable=False, primary_key=True)
    NOME_PARLAMENTAR = db.Column(db.String, unique=False, nullable=False)
    TIPO_PARLAMENTAR = db.Column(db.String, unique=False, nullable=False)

    CONVENIOS = db.relationship("Convenio", secondary=convenios_emendas_association, back_populates="EMENDAS")

    def __init__(self, **kwargs):

        self.NR_EMENDA = kwargs.get('NR_EMENDA')
        self.NOME_PARLAMENTAR = kwargs.get('NOME_PARLAMENTAR')
        self.TIPO_PARLAMENTAR = kwargs.get('TIPO_PARLAMENTAR')        

    def __repr__(self):
        return "<Emenda(NR_EMENDA={self.NR_EMENDA!r})>".format(self=self)

