# -*- coding: utf-8 -*-

from sklearn.metrics import precision_score, f1_score, recall_score, accuracy_score, classification_report
from sklearn.preprocessing import OneHotEncoder, PowerTransformer
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, cross_validate, cross_val_predict, StratifiedKFold

import pandas as pd
import pickle
import numpy as np
from .text_transformer import TextTransformer

class MLModel(object):
    
    def __init__(self, transformers, principal_components_analysis, classifier, dimensionality_reduction_metrics, cross_validation_metrics, validation_metrics):
        
        self.transformers = transformers
        self.principal_components_analysis = principal_components_analysis
        self.classifier = classifier
        self.validation_metrics = validation_metrics
        self.cross_validation_metrics = cross_validation_metrics
        self.dimensionality_reduction_metrics = dimensionality_reduction_metrics
        
        
class Metrics(object):
    
    def __init__(self):
        
        self.precision_score = None
        self.recall_score = None
        self.f1_score = None
        self.accuracy_score = None
        self.classification_report = None
        
    def json(self):
        
        metrics_json = self.__dict__.copy()
        metrics_json.pop('classification_report')

        return metrics_json
        
class DimensionalityReductionMetrics(object):
    
    def __init__(self):
        
        self.original_dimensinality = None
        self.reduced_dimensinality = None
        self.explained_variance_ratio = None
        
    def json(self):
        
        metrics_json = self.__dict__.copy()

        return metrics_json     
        
class RiskAnalyzer(object):
    
    def __init__(self, chunk_size=None, cross_validation_folds=10, ylabel_name = 'INSUCESSO',
                       raw_dataset_path='./etl/datasets/convenios.txt.gz', 
                       train_dataset_path='./etl/datasets/convenios_train.tsv.gz', 
                       test_dataset_path='./etl/datasets/convenios_test.tsv.gz'):
        
        self.cross_validation_folds = cross_validation_folds
        self.chunk_size = chunk_size
        self.raw_dataset_path = raw_dataset_path
        self.ylabel_name = ylabel_name
        self.pca_components = 700
        
        self.__model_filename_path__ = './etl/trained_model/model.pickle'
        self.__train_dataset_path__ = train_dataset_path
        self.__test_dataset_path__ = test_dataset_path
        self.__accented__ = './etl/datasets/accented.txt.gz'
        self.__stop_words__ = './etl/datasets/stopwords.txt.gz'
        
        self.__model_object__ = self.__load_model__()

        self.__ibge__ = None
        self.__principais_parlamentares__ = None
        self.__principais_fornecedores__ = None
        
    def make_train_test_bases(self, **tables):
    
        assert not tables or set(tables.keys()) == {'convenios', 'emendas', 'emendas_convenios', 
        'fornecedores', 'movimento'}, '''Named arguments, if provided, must be: convenios, emendas, emendas_convenios, fornecedores, movimento'''

        if tables:
            print('transforming tables into dataset ... ', end='')
            data = self.__transform_dataset__(**tables, ylabel=True)
            data = data.drop(['NR_CONVENIO'], axis=1)
            print('ok')
        else:
            print('loading dataset ... ', end='')
            data = pd.read_csv(self.raw_dataset_path, compression='gzip', sep='\t', encoding='utf-8')
            data = data.drop(['NR_CONVENIO'], axis=1)
            print('ok')

        print('balancing ... ', end='')
        data = data.sample(frac=1).reset_index(drop=True)
        q0 = len(data[data[self.ylabel_name]==0])
        q1 = len(data[data[self.ylabel_name]==1])
        q = q0 if q0<q1 else q1

        X = pd.concat([data[data[self.ylabel_name]==0].iloc[0:q], data[data[self.ylabel_name]==1].iloc[0:q]], sort=False)
        y = X[[self.ylabel_name]]
        X = pd.concat([data[data[self.ylabel_name]==0].iloc[0:q], data[data[self.ylabel_name]==1].iloc[0:q]], sort=False)
        rest = data[~data.index.isin(X.index)]
        print('ok')

        print('spliting ... ', end='')
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=0)
        print('ok')
        
        print('saving train and test datasets ... ', end='')
        X_train.to_csv(self.__train_dataset_path__, compression='gzip', sep='\t', encoding='utf-8', index=False)
        X_test.to_csv(self.__test_dataset_path__, compression='gzip', sep='\t', encoding='utf-8', index=False)
        print('ok')
        
    def train(self):
        
        print('loading bases ... ', end='')
        X_train, y_train = self.__load_bases__(type='train')
        print('ok')
        
        print('making transformers ... ', end='')
        self.__model_object__.transformers = self.__make_transformers__(X_train)
        print('ok')
        
        print('preparing data ... ', end='')
        X_train = self.__data_preparation__(X_train)
        print('ok')
        
        print('reducing dimensionality with principal components analysis ... ', end='')
        X_train = self.__dimensionality_reduction(X_train, y_train)
        print('ok')
        
        print('training model ... ', end='')
        self.__model_object__.classifier = RandomForestClassifier(criterion='gini', n_estimators=200, max_features='sqrt').fit(X_train, y_train[self.ylabel_name])
        print('ok')
                       
        print('scoring cross-validation ... ', end='')
        self.__cross_validation_test__(X_train, y_train)
        print('ok')

        print('testing model ... ', end='')
        self.__validation_test__()
        print('ok')

        print('saving model ... ', end='')
        self.__save_model__()
        print('ok')
         
    def metrics(self, type='validation'):
        
        assert type in ['validation', 'cross-validation', 'dimensionality'], "type must be 'validation' or 'cross-validation' or 'dimensionality'"
        
        if type=='validation':
            return self.__model_object__.validation_metrics
        elif type=='cross-validation':
            return self.__model_object__.cross_validation_metrics
        else:
            return self.__model_object__.dimensionality_reduction_metrics
        
    def predict(self, X_conv, proba=False, scale=False, append=False):
        dataframe_type = True
        if not isinstance(X_conv, pd.DataFrame):
            X= pd.DataFrame(X_conv)
            dataframe_type = False
        else:
            X = X_conv.copy()
        
        nr_convenio = None
        if 'NR_CONVENIO' in X.columns:
            nr_convenio = X.pop('NR_CONVENIO').to_frame()
            
        X = self.__data_preparation__(X)
        X = self.__model_object__.principal_components_analysis.transform(X)
        if proba:
            predictions = self.__model_object__.classifier.predict_proba(X)[:, 1]
            if scale:
                predictions = np.array(list(map(lambda value: self.__sigmoid__(value), predictions)))
        else:
            predictions = self.__model_object__.classifier.predict(X)
            
        predictions = pd.Series(predictions, name=self.ylabel_name)
        
        if append:
            predictions = pd.DataFrame(predictions)
            predictions = pd.concat([X, predictions], axis=1, sort=False, ignore_index=False)

            if isinstance(nr_convenio, pd.DataFrame):
                predictions = pd.concat([nr_convenio, predictions], axis=1, sort=False, ignore_index=False)
        
        if not dataframe_type:
            predictions = predictions.to_dict()
            
        return predictions 
    
    def run(self, convenios, proponentes, emendas, emendas_convenios, fornecedores, movimento, append=False):
        
        convenios_ = self.__transform_dataset__(convenios, proponentes, emendas, emendas_convenios, fornecedores, movimento)
        
        return self.predict(convenios_, proba=True, scale=True, append=append)
    
    def __cross_validation_test__(self, X_train=None, y_train=None):
        
        cv = StratifiedKFold(n_splits = self.cross_validation_folds, shuffle = True, random_state=200)
        cv_scores = cross_validate(self.__model_object__.classifier, X_train, y_train[self.ylabel_name], cv=cv, 
                                   scoring=['precision', 'recall', 'f1', 'accuracy'])
        
        p = cross_val_predict(self.__model_object__.classifier, X_train, y_train[self.ylabel_name], cv=cv, method='predict')

        cross_validation = Metrics()
        cross_validation.precision_score = cv_scores['test_precision']
        cross_validation.recall_score = cv_scores['test_recall']
        cross_validation.f1_score = cv_scores['test_f1']
        cross_validation.accuracy_score = cv_scores['test_accuracy']
        cross_validation.classification_report = classification_report(y_train[self.ylabel_name], p)
        
        self.__model_object__.cross_validation_metrics = cross_validation
        
    def __validation_test__(self):
        
        X_test, y_test = self.__load_bases__(type='test')
        
        X_test = self.__data_preparation__(X_test)
        X_test = self.__model_object__.principal_components_analysis.transform(X_test)

        p = self.__model_object__.classifier.predict(X_test)
        
        validation = Metrics()
        validation.precision_score = precision_score(y_test, p)
        validation.recall_score = recall_score(y_test, p)
        validation.f1_score = f1_score(y_test, p)
        validation.accuracy_score = accuracy_score(y_test, p)
        validation.classification_report = classification_report(y_test[self.ylabel_name], p)
        
        self.__model_object__.validation_metrics = validation
    
    def __make_transformers__(self, X):
        
        accented = pd.read_csv(self.__accented__, compression='gzip', sep=';', encoding='utf-8')
        stop_words = pd.read_csv(self.__stop_words__, compression='gzip', encoding='utf-8', header=None)[0].tolist()
        data = X.copy()
        text_clusterer = TextTransformer(n_clusters=50, stop_words=stop_words, accented=accented).fit(data['OBJETO_PROPOSTA'])
        data['OBJETO_PROPOSTA'] = text_clusterer.predict(data['OBJETO_PROPOSTA'])
        data['OBJETO_PROPOSTA'] = data['OBJETO_PROPOSTA'].astype('int64')

        data_categorical_parlamentar = data.pop('PRINCIPAL_PARLAMENTAR').to_frame()
        data_categorical_fornecedor = data.pop('PRINCIPAL_FORNECEDOR').to_frame()

        data_categorical_object = data.select_dtypes(include=['object'])
        data_categorical_int = data.select_dtypes(include=['int64'])
        data_value = data.select_dtypes(include='float64')

        transformers = {}
        transformers['TEXT_CLUSTERER'] = text_clusterer
        transformers['VALUE'] = PowerTransformer().fit(data_value)
        transformers['CATEGORICAL_OBJECT'] = OneHotEncoder(handle_unknown='ignore').fit(data_categorical_object)
        transformers['CATEGORICAL_INT'] = OneHotEncoder(handle_unknown='infrequent_if_exist', max_categories=500).fit(data_categorical_int)
        transformers['CATEGORICAL_PARLAMENTAR'] = OneHotEncoder(handle_unknown='infrequent_if_exist', max_categories=500).fit(data_categorical_parlamentar)
        transformers['CATEGORICAL_FORNECEDOR'] = OneHotEncoder(handle_unknown='infrequent_if_exist', max_categories=500).fit(data_categorical_fornecedor)

        return transformers
    
    def __data_preparation__(self, X):
        
        data = X.copy()
        transformers = self.__model_object__.transformers
        data['OBJETO_PROPOSTA'] = transformers['TEXT_CLUSTERER'].predict(data['OBJETO_PROPOSTA'])
        data['OBJETO_PROPOSTA'] = data['OBJETO_PROPOSTA'].astype('int64')

        data_categorical_parlamentar = data.pop('PRINCIPAL_PARLAMENTAR').to_frame()
        data_categorical_fornecedor = data.pop('PRINCIPAL_FORNECEDOR').to_frame()

        data_categorical_object = data.select_dtypes(include=['object'])
        data_categorical_int = data.select_dtypes(include=['int64'])
        data_value = data.select_dtypes(include='float64')

        value_codes = transformers['VALUE'].transform(data_value)
        value_feature_names = transformers['VALUE'].feature_names_in_
        data_value = pd.DataFrame(value_codes, columns=value_feature_names).astype('float64')

        categorical_object_codes = transformers['CATEGORICAL_OBJECT'].transform(data_categorical_object).toarray()
        categorical_object_feature_names= transformers['CATEGORICAL_OBJECT'].get_feature_names_out()
        data_categorical_object = pd.DataFrame(categorical_object_codes, columns=categorical_object_feature_names).astype('float64')

        categorical_int_codes = transformers['CATEGORICAL_INT'].transform(data_categorical_int).toarray()
        categorical_int_feature_names= transformers['CATEGORICAL_INT'].get_feature_names_out()
        data_categorical_int = pd.DataFrame(categorical_int_codes, columns=categorical_int_feature_names).astype('float64')

        parlamentar_codes = transformers['CATEGORICAL_PARLAMENTAR'].transform(data_categorical_parlamentar).toarray()
        parlamentar_feature_names= transformers['CATEGORICAL_PARLAMENTAR'].get_feature_names_out()
        data_categorical_parlamentar = pd.DataFrame(parlamentar_codes, columns=parlamentar_feature_names).astype('float64')

        fornecedor_codes = transformers['CATEGORICAL_FORNECEDOR'].transform(data_categorical_fornecedor).toarray()
        fornecedor_feature_names= transformers['CATEGORICAL_FORNECEDOR'].get_feature_names_out()
        data_categorical_fornecedor = pd.DataFrame(fornecedor_codes, columns=fornecedor_feature_names).astype('float64')

        return pd.concat([data_value, data_categorical_object, data_categorical_int,
                          data_categorical_parlamentar, data_categorical_fornecedor], axis=1, sort=False)

    def __dimensionality_reduction(self, X_train, y_train):
        
        dimensionality_metrics = DimensionalityReductionMetrics()
        dimensionality_metrics.original_dimensinality = len(X_train.columns)
        self.__model_object__.principal_components_analysis = PCA(n_components=self.pca_components)
        X_train = self.__model_object__.principal_components_analysis.fit_transform(X_train, y_train)
        dimensionality_metrics.reduced_dimensinality = len(X_train[0])
        dimensionality_metrics.explained_variance_ratio = sum(self.__model_object__.principal_components_analysis.explained_variance_ratio_)
        self.__model_object__.dimensionality_reduction_metrics = dimensionality_metrics
        return X_train
    
    def __load_bases__(self, type='both'):
        
        assert type in ['train', 'test', 'both']
        
        result = []
        if type.lower() in ['train', 'both']:
            train_base = pd.read_csv(self.__train_dataset_path__, compression='gzip', sep='\t', encoding='utf-8')
            X_train = train_base.drop([self.ylabel_name], axis=1)
            y_train = train_base[[self.ylabel_name]]
            result += [X_train, y_train]

        if type.lower() in ['test', 'both']:
            test_base = pd.read_csv(self.__test_dataset_path__, compression='gzip', sep='\t', encoding='utf-8')
            X_test = test_base.drop([self.ylabel_name], axis=1)
            y_test = test_base[[self.ylabel_name]]
            result += [X_test, y_test]

        return result

    def __sigmoid__(self, value):
        
        middle_value = 0.5
        slope_factor = 2
        return 1/(1+np.e**(-slope_factor*(value-middle_value)))
            
    def __save_model__(self):
        
        with open(self.__model_filename_path__, 'wb') as fd:
            pickle.dump(self.__model_object__, fd)
    
    def __load_model__(self):
        
        with open(self.__model_filename_path__, 'rb') as fd:
            model_object = pickle.load(fd)
        return model_object
    
    def __transform_dataset__(self, convenios, proponentes, emendas, emendas_convenios, fornecedores, movimento, ylabel=False):

        self.__ibge__ = proponentes[['IDENTIF_PROPONENTE', 'CODIGO_IBGE']].copy()
        
        selected_columns = ['VL_REPASSE_CONV', 'VL_CONTRAPARTIDA_CONV', 'VALOR_EMENDA_CONVENIO',
               'OBJETO_PROPOSTA', 'COD_ORGAO', 'COD_ORGAO_SUP', 'NATUREZA_JURIDICA',
               'MODALIDADE', 'IDENTIF_PROPONENTE', 'COM_EMENDAS']
        
        features_columns = ['NR_CONVENIO', *selected_columns]
        if ylabel:
            features_columns += [self.ylabel_name]
            
        convenios_ = convenios[features_columns].copy()
        
        self.__principais_parlamentares__ = self.__get_principais_parlamentares__(emendas=emendas, emendas_convenios=emendas_convenios, convenios_list=convenios_['NR_CONVENIO'].to_list())
        self.__principais_fornecedores__ = self.__get_principais_fornecedores__(movimento=movimento, fornecedores=fornecedores, convenios_list=convenios_['NR_CONVENIO'].to_list())

        dataset = pd.merge(convenios_, self.__ibge__, how='inner', on=['IDENTIF_PROPONENTE'], left_index=False, right_index=False)

        dataset = pd.merge(dataset, self.__principais_parlamentares__, how='left', on=['NR_CONVENIO'], left_index=False, right_index=False)

        dataset = pd.merge(dataset, self.__principais_fornecedores__, how='left', on=['NR_CONVENIO'], left_index=False, right_index=False)

        dataset = dataset.fillna('NAO APLICAVEL')
        
        Xdtypes = {'VL_REPASSE_CONV': 'float64', 'VL_CONTRAPARTIDA_CONV': 'float64', 
                   'VALOR_EMENDA_CONVENIO': 'float64', 'OBJETO_PROPOSTA': 'object', 
                   'COD_ORGAO': 'int64', 'COD_ORGAO_SUP': 'int64', 'NATUREZA_JURIDICA': 'object', 
                   'MODALIDADE': 'object', 'IDENTIF_PROPONENTE': 'int64', 'COM_EMENDAS': 'int64',
                   'CODIGO_IBGE': 'int64', 'PRINCIPAL_PARLAMENTAR': 'object', 'PRINCIPAL_FORNECEDOR': 'object'}
        
        return dataset.astype(Xdtypes)
    
    def __get_principais_parlamentares__(self, emendas, emendas_convenios, convenios_list):
        
        convenios_repasses_emendas = emendas_convenios.loc[emendas_convenios['NR_CONVENIO'].isin(convenios_list)].copy()
        convenios_repasses_emendas['rank'] = convenios_repasses_emendas.groupby(by=['NR_CONVENIO'])['VALOR_REPASSE_EMENDA'].rank(ascending=False, method='min')
        convenios_repasses_emendas = convenios_repasses_emendas.loc[convenios_repasses_emendas['rank']==1, ['NR_CONVENIO', 'NR_EMENDA']]
        convenios_parlamentares = pd.merge(convenios_repasses_emendas, emendas, on=['NR_EMENDA'], left_index=False, right_index=False)
        convenios_parlamentares = convenios_parlamentares[['NR_CONVENIO', 'NOME_PARLAMENTAR']]
        convenios_parlamentares.columns = ['NR_CONVENIO', 'PRINCIPAL_PARLAMENTAR']
        convenios_parlamentares = convenios_parlamentares.groupby(by=['NR_CONVENIO']).max().reset_index()
        
        return convenios_parlamentares

    def __get_principais_fornecedores__(self, movimento, fornecedores, convenios_list):
        
        movimento_exec = movimento.loc[movimento['NR_CONVENIO'].isin(convenios_list)].copy()
        movimento_exec = movimento_exec[movimento_exec['TIPO_MOV']=='P']
        convenios_fornecedores = movimento_exec[['NR_CONVENIO', 'FORNECEDOR_ID', 'VALOR_MOV']].groupby(by=['NR_CONVENIO', 'FORNECEDOR_ID']).sum().reset_index().copy()
        convenios_fornecedores['rank'] = convenios_fornecedores.groupby(by=['NR_CONVENIO'])['VALOR_MOV'].rank(ascending=False, method='min')
        convenios_fornecedores = convenios_fornecedores.loc[convenios_fornecedores['rank']==1, ['NR_CONVENIO', 'FORNECEDOR_ID']]
        convenios_fornecedores = pd.merge(convenios_fornecedores, fornecedores, on=['FORNECEDOR_ID'], left_index=False, right_index=False)
        convenios_fornecedores = convenios_fornecedores.sort_values(['NR_CONVENIO', 'IDENTIF_FORNECEDOR'], ascending=False)
        convenios_fornecedores = convenios_fornecedores[['NR_CONVENIO', 'IDENTIF_FORNECEDOR']]
        convenios_fornecedores.columns = ['NR_CONVENIO', 'PRINCIPAL_FORNECEDOR']
        convenios_fornecedores = convenios_fornecedores.groupby(by=['NR_CONVENIO']).max().reset_index()
        
        return convenios_fornecedores
    
