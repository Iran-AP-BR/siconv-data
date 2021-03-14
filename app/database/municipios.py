# coding: utf-8
"""
   """

from . import db

class Municipio(db.Model):

    __tablename__ = 'municipios'

    codigo_ibge = db.Column(db.String, unique=True, nullable=False, primary_key=True)
    nome_municipio = db.Column(db.String, unique=False, nullable=True)
    codigo_uf = db.Column(db.String, unique=False, nullable=True)
    uf = db.Column(db.String, unique=False, nullable=True)
    estado = db.Column(db.String, unique=False, nullable=True)
    latitude = db.Column(db.Float, unique=False, nullable=True)
    longitude = db.Column(db.Float, unique=False, nullable=True)

    PROPONENTES = db.relationship("Proponente", backref="MUNICIPIO", lazy='dynamic')

    def __init__(self, **kwargs):

        self.codigo_ibge = kwargs.get('codigo_ibge')
        self.nome_municipio = kwargs.get('nome_municipio')
        self.codigo_uf = kwargs.get('codigo_uf')
        self.uf = kwargs.get('uf')
        self.estado = kwargs.get('estado')
        self.latitude = kwargs.get('latitude')
        self.longitude = kwargs.get('longitude')

    def __repr__(self):
        return "<Municipio(codigo_ibge={self.codigo_ibge!r})>".format(self=self)

