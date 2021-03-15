# coding: utf-8
"""data_atual table.
   """

from . import db


class Situacao(db.Model):

    __tablename__ = 'situacoes'

    SIT_CONVENIO = db.Column(db.String, unique=True, nullable=False, primary_key=True)

    def __init__(self, **kwargs):

        self.SIT_CONVENIO = kwargs.get('SIT_CONVENIO')

    def __repr__(self):
        return "<Situacao(SIT_CONVENIO={self.SIT_CONVENIO!r})>".format(self=self)

class Natureza(db.Model):

    __tablename__ = 'naturezas'

    NATUREZA_JURIDICA = db.Column(db.String, unique=True, nullable=False, primary_key=True)

    def __init__(self, **kwargs):

        self.NATUREZA_JURIDICA = kwargs.get('NATUREZA_JURIDICA')

    def __repr__(self):
        return "<Natureza(NATUREZA_JURIDICA={self.NATUREZA_JURIDICA!r})>".format(self=self)

class Modalidade(db.Model):

    __tablename__ = 'modalidades'

    MODALIDADE = db.Column(db.String, unique=True, nullable=False, primary_key=True)

    def __init__(self, **kwargs):

        self.MODALIDADE = kwargs.get('MODALIDADE')

    def __repr__(self):
        return "<Natureza(MODALIDADE={self.MODALIDADE!r})>".format(self=self)

