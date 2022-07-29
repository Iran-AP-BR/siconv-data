CREATE DATABASE IF NOT EXISTS `siconvdata` 
CHARACTER SET `utf8mb4`
COLLATE `utf8mb4_general_ci`;

use siconvdata;
drop table if exists calendario;
drop table if exists convenios;
drop table if exists data_atual;
drop table if exists emendas;
drop table if exists emendas_convenios;
drop table if exists fornecedores;
drop table if exists movimento;
drop table if exists municipios;
drop table if exists proponentes;
drop table if exists licitacoes;

CREATE TABLE `calendario` (
  `DATA` date NOT NULL,
  `ANO` int NOT NULL,
  `MES_NUMERO` smallint NOT NULL,
  `MES_NOME` varchar(10) NOT NULL,
  `ANO_MES_NUMERO` int NOT NULL,
  `MES_ANO` varchar(7) NOT NULL,
  `MES_NOME_ANO` varchar(15) NOT NULL,
  `TRIMESTRE_NUMERO` smallint NOT NULL,
  `TRIMESTRE_NOME` varchar(12) NOT NULL,
  `ANO_TRIMESTRE_NUMERO` int NOT NULL,
  `TRIMESTRE_ANO` varchar(7) NOT NULL,
  `TRIMESTRE_NOME_ANO` varchar(17) NOT NULL,
  `SEMANA_NUMERO` smallint NOT NULL,
  `SEMANA_NOME` varchar(10) NOT NULL,
  `ANO_SEMANA_NUMERO` int NOT NULL,
  `SEMANA_ANO` varchar(7) NOT NULL,
  `SEMANA_NOME_ANO` varchar(15) NOT NULL,
  `DIA_DA_SEMANA` varchar(13) NOT NULL,
  PRIMARY KEY (`DATA`),
  KEY `idx_calendario_ano` (`ANO`),
  KEY `idx_calendario_ano_mes_numero` (`ANO_MES_NUMERO`),
  KEY `idx_calendario_ano_trimestre_numero` (`ANO_TRIMESTRE_NUMERO`),
  KEY `idx_calendario_ano_semana_numero` (`ANO_SEMANA_NUMERO`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `convenios` (
  `NR_CONVENIO` int NOT NULL,
  `DIA_ASSIN_CONV` date NOT NULL,
  `SIT_CONVENIO` varchar(80) NOT NULL,
  `INSTRUMENTO_ATIVO` boolean NOT NULL,
  `DIA_PUBL_CONV` date DEFAULT NULL,
  `DIA_INIC_VIGENC_CONV` date NOT NULL,
  `DIA_FIM_VIGENC_CONV` date NOT NULL,
  `DIA_LIMITE_PREST_CONTAS` date DEFAULT NULL,
  `VL_GLOBAL_CONV` decimal(18,2) NOT NULL,
  `VL_REPASSE_CONV` decimal(18,2) NOT NULL,
  `VL_CONTRAPARTIDA_CONV` decimal(18,2) NOT NULL,
  `COD_ORGAO_SUP` int NOT NULL,
  `DESC_ORGAO_SUP` varchar(80) DEFAULT NULL,
  `NATUREZA_JURIDICA` varchar(60) NOT NULL,
  `COD_ORGAO` int NOT NULL,
  `DESC_ORGAO` varchar(80) NOT NULL,
  `MODALIDADE` varchar(20) NOT NULL,
  `IDENTIF_PROPONENTE` varchar(14) NOT NULL,
  `OBJETO_PROPOSTA` text,
  `VALOR_EMENDA_CONVENIO` decimal(18, 2) NOT NULL,
  `COM_EMENDAS` char(3) NOT NULL,
  `INSUCESSO` float NOT NULL,
  PRIMARY KEY (`NR_CONVENIO`),
  KEY `idx_convenios_proponente` (`IDENTIF_PROPONENTE`),
  KEY `idx_convenios_dia_inic_vigenc_conv` (`DIA_INIC_VIGENC_CONV`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `data_atual` (
  `DATA_ATUAL` date NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `emendas` (
  `NR_EMENDA` bigint NOT NULL,
  `NOME_PARLAMENTAR` varchar(60) NOT NULL,
  `TIPO_PARLAMENTAR` varchar(20) NOT NULL,
  `VALOR_EMENDA` decimal(18,2) NOT NULL,
  PRIMARY KEY (`NR_EMENDA`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `emendas_convenios` (
  `NR_EMENDA` bigint NOT NULL,
  `NR_CONVENIO` int NOT NULL,
  `VALOR_REPASSE_EMENDA` decimal(18,2) NOT NULL,
  KEY `idx_emendas_convenios_nr_convenio` (`NR_CONVENIO`),
  KEY `idx_nr_convenio_nr_emenda` (`NR_EMENDA`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `fornecedores` (
  `FORNECEDOR_ID` int NOT NULL,
  `IDENTIF_FORNECEDOR` varchar(40) NOT NULL,
  `NOME_FORNECEDOR` varchar(150) NOT NULL,
  PRIMARY KEY (`FORNECEDOR_ID`),
  KEY `idx_fornecedores_identif_nome` (`IDENTIF_FORNECEDOR`,`NOME_FORNECEDOR`(60))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `movimento` (
  `NR_CONVENIO` int NOT NULL, 
  `DATA_MOV` date NOT NULL,
  `VALOR_MOV` decimal(18,2) NOT NULL,
  `MOV_ID` bigint NOT NULL,
  `TIPO_MOV` char(1) NOT NULL, 
  `FORNECEDOR_ID` int NOT NULL,
  KEY `idx_movimento_convenio` (`NR_CONVENIO`),
  KEY `idx_movimento_fornecedor_id` (`FORNECEDOR_ID`),
  KEY `idx_movimento_data` (`DATA_MOV`),
  KEY `idx_movimento_tipo` (`TIPO_MOV`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  PARTITION BY HASH (year(`DATA_MOV`))
  PARTITIONS 20;

CREATE TABLE `municipios` (
  `CODIGO_IBGE` bigint NOT NULL,
  `NOME_MUNICIPIO` varchar(60) NOT NULL,
  `UF` char(2) NOT NULL,
  `REGIAO` varchar(12) NOT NULL,
  `REGIAO_ABREVIADA` char(2) NOT NULL,
  `NOME_ESTADO` varchar(30) NOT NULL,
  `LATITUDE` float,
  `LONGITUDE` float,
  `CAPITAL` boolean NOT NULL,
  PRIMARY KEY (`CODIGO_IBGE`),
  KEY `idx_municipios_uf` (`UF`(2))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `proponentes` (
  `IDENTIF_PROPONENTE` varchar(14) NOT NULL,
  `NM_PROPONENTE` varchar(150) NOT NULL,
  `CODIGO_IBGE` bigint NOT NULL,
  PRIMARY KEY (`IDENTIF_PROPONENTE`),
  KEY `idx_proponentes_codigo_ibge` (`CODIGO_IBGE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `licitacoes` (
  `ID_LICITACAO` INTEGER NOT NULL, 
  `NR_CONVENIO` INTEGER NOT NULL, 
  `MODALIDADE_COMPRA` VARCHAR(50) NOT NULL, 
  `TIPO_LICITACAO` VARCHAR(20) NOT NULL, 
  `FORMA_LICITACAO` VARCHAR(20) NOT NULL, 
  `REGISTRO_PRECOS` BOOLEAN, 
  `LICITACAO_INTERNACIONAL` BOOLEAN,
  `STATUS_LICITACAO` VARCHAR(15) NOT NULL, 
  `VALOR_LICITACAO` DECIMAL(18, 2) NOT NULL,
  PRIMARY KEY (`ID_LICITACAO`),
  KEY `idx_licitacoes_nr_convenio` (`NR_CONVENIO`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
