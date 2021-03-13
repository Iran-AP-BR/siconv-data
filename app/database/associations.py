# coding: utf-8
"""Provides associations table for many-to-may relationships.
   """

from . import db

convenios_emendas_association = db.Table('convenios_emendas_association',
    db.Column('NR_CONVENIO', db.String, db.ForeignKey('convenios.NR_CONVENIO'), primary_key=True),
    db.Column('NR_EMENDA', db.String, db.ForeignKey('emendas.NR_EMENDA'), primary_key=True),
)
