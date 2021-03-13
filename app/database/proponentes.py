# coding: utf-8
"""
   """

from . import db

class Proponente(db.Model):

    __tablename__ = 'proponentes'

    IDENTIF_PROPONENTE = db.Column(db.String, unique=True, nullable=False, primary_key=True)
    NM_PROPONENTE = db.Column(db.String, unique=False, nullable=True)
    UF_PROPONENTE = db.Column(db.String, unique=False, nullable=True)
    MUNIC_PROPONENTE = db.Column(db.String, unique=False, nullable=True)

    COD_MUNIC_IBGE = db.Column(db.String, db.ForeignKey('municipios.codigo_ibge'), unique=False, nullable=False)

    CONVENIOS = db.relationship("Convenio", backref="PROPONENTE")

    def __init__(self, **kwargs):

        self.IDENTIF_PROPONENTE = kwargs.get('IDENTIF_PROPONENTE')
        self.NM_PROPONENTE = kwargs.get('NM_PROPONENTE')
        self.UF_PROPONENTE = kwargs.get('UF_PROPONENTE')
        self.MUNIC_PROPONENTE = kwargs.get('MUNIC_PROPONENTE')
        self.COD_MUNIC_IBGE = kwargs.get('COD_MUNIC_IBGE')
        
    def __repr__(self):
        return "<Proponente(IDENTIF_PROPONENTE={self.IDENTIF_PROPONENTE!r})>".format(self=self)

