# coding: utf-8
"""
   """

from . import db
from .associations import convenios_emendas_association
from .emendas import Emenda
from .movimento import Movimento

class Convenio(db.Model):

    __tablename__ = 'convenios'

    NR_CONVENIO = db.Column(db.String, unique=True, nullable=False, primary_key=True)
    DIA_ASSIN_CONV = db.Column(db.DateTime, unique=False, nullable=True)
    SIT_CONVENIO = db.Column(db.String, unique=False, nullable=True)
    INSTRUMENTO_ATIVO = db.Column(db.String, unique=False, nullable=True)
    DIA_PUBL_CONV = db.Column(db.DateTime, unique=False, nullable=True)
    DIA_INIC_VIGENC_CONV = db.Column(db.DateTime, unique=False, nullable=True)
    DIA_FIM_VIGENC_CONV = db.Column(db.DateTime, unique=False, nullable=True)
    DIA_LIMITE_PREST_CONTAS = db.Column(db.DateTime, unique=False, nullable=True)
    VL_GLOBAL_CONV = db.Column(db.Numeric(10, 2), unique=False, nullable=True)
    VL_REPASSE_CONV = db.Column(db.Numeric(10, 2), unique=False, nullable=True)
    VL_CONTRAPARTIDA_CONV = db.Column(db.Numeric(10, 2), unique=False, nullable=True)
    COD_ORGAO_SUP = db.Column(db.String, unique=False, nullable=True)
    DESC_ORGAO_SUP = db.Column(db.String, unique=False, nullable=True)
    NATUREZA_JURIDICA = db.Column(db.String, unique=False, nullable=True)
    COD_ORGAO = db.Column(db.String, unique=False, nullable=True)
    DESC_ORGAO = db.Column(db.String, unique=False, nullable=True)
    MODALIDADE = db.Column(db.String, unique=False, nullable=True)
    OBJETO_PROPOSTA = db.Column(db.String, unique=False, nullable=True)

    IDENTIF_PROPONENTE = db.Column(db.String, db.ForeignKey('proponentes.IDENTIF_PROPONENTE'), unique=False, nullable=False)

    EMENDAS = db.relationship("Emenda", secondary=convenios_emendas_association, back_populates="CONVENIOS", lazy='dynamic')
    MOVIMENTOS = db. relationship("Movimento", backref="CONVENIO")


    def __init__(self, **kwargs):

        self.NR_CONVENIO = kwargs.get('NR_CONVENIO')
        self.DIA_ASSIN_CONV = kwargs.get('DIA_ASSIN_CONV')
        self.SIT_CONVENIO = kwargs.get('SIT_CONVENIO')
        self.INSTRUMENTO_ATIVO = kwargs.get('INSTRUMENTO_ATIVO')
        self.DIA_PUBL_CONV = kwargs.get('DIA_PUBL_CONV')
        self.DIA_INIC_VIGENC_CONV = kwargs.get('DIA_INIC_VIGENC_CONV')
        self.DIA_FIM_VIGENC_CONV = kwargs.get('DIA_FIM_VIGENC_CONV')
        self.DIA_LIMITE_PREST_CONTAS = kwargs.get('DIA_LIMITE_PREST_CONTAS')
        self.VL_GLOBAL_CONV = kwargs.get('VL_GLOBAL_CONV')
        self.VL_REPASSE_CONV = kwargs.get('VL_REPASSE_CONV')
        self.VL_CONTRAPARTIDA_CONV = kwargs.get('VL_CONTRAPARTIDA_CONV')
        self.COD_ORGAO_SUP = kwargs.get('COD_ORGAO_SUP')
        self.DESC_ORGAO_SUP = kwargs.get('DESC_ORGAO_SUP')
        self.NATUREZA_JURIDICA = kwargs.get('NATUREZA_JURIDICA')
        self.COD_ORGAO = kwargs.get('COD_ORGAO')
        self.DESC_ORGAO = kwargs.get('DESC_ORGAO')
        self.MODALIDADE = kwargs.get('MODALIDADE')
        self.IDENTIF_PROPONENTE = kwargs.get('IDENTIF_PROPONENTE')
        self.OBJETO_PROPOSTA = kwargs.get('OBJETO_PROPOSTA')
        
    def __repr__(self):
        return "<Convenio(NR_CONVENIO={self.NR_CONVENIO!r})>".format(self=self)

