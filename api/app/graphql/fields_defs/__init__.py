# coding: utf-8
"""Queries.
   """

from .buscarConvenios import query as q1
from .buscarEmendas import query as q2
from .buscarEstados import query as q3
from .buscarFornecedores import query as q4
from .buscarLicitacoes import query as q5
from .buscarMovimento import query as q6
from .buscarMunicipios import query as q7
from .buscarParlamentares import query as q8
from .buscarProponentes import query as q9
from .buscarAtributos import query as q10

from .analytics import query as analytics

fields_defs = [q1, q2, q3, q4, q5, q6, q7, q8, q9, q10, analytics]