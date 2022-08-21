drop table if exists `atributos`;

create table `atributos` (
   select * from (
    select distinct "SIT_CONVENIO" as ATRIBUTO, SIT_CONVENIO as VALOR from convenios where not SIT_CONVENIO is null
	union all
	select distinct "NATUREZA_JURIDICA", NATUREZA_JURIDICA from convenios where not NATUREZA_JURIDICA is null
	union all
	select distinct "MODALIDADE_TRANSFERENCIA", MODALIDADE from convenios where not MODALIDADE is null
	union all
	select distinct "TIPO_PARLAMENTAR", TIPO_PARLAMENTAR from emendas where not TIPO_PARLAMENTAR is null 
	union all
	select distinct "MODALIDADE_COMPRA", MODALIDADE_COMPRA from licitacoes where not MODALIDADE_COMPRA is null
	union all
	select distinct "TIPO_LICITACAO", TIPO_LICITACAO from licitacoes where not TIPO_LICITACAO is null
	union all
	select distinct "FORMA_LICITACAO", FORMA_LICITACAO from licitacoes where not FORMA_LICITACAO is null
	union all
	select distinct "STATUS_LICITACAO", STATUS_LICITACAO from licitacoes where not STATUS_LICITACAO is null
    ) attr
)

