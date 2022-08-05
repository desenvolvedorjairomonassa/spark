#!/usr/bin/env python
# coding: utf-8

#libraries
import os
import sys
import json
import pyspark.sql.functions as f
from pyspark.sql.functions import when, col
from pyspark import SparkContext, SparkConf, SQLContext, HiveContext
from collections import namedtuple

conf = SparkConf().set("spark.driver.memory", "8G")\
    .set("spark.executor.cores", "5")\
    .set("spark.executor.instances", "5")\
    .set("spark.executor.memory", "10G")\
    .set("spark.sql.files.maxPartitionBytes", "128MB")\
    .set("spark.sql.adaptive.enabled", "true")\
    .set("spark.sql.shuffle.partitions", "50")\
    .set("spark.yarn.executor.memoryOverhead", "2")\
    .set("spark.yarn.driver.memoryOverhead", "1G")

sc = SparkContext(conf=conf)
sql = SQLContext(sc)
hc = HiveContext(sc)


dt_mes_atual = sys.argv[1]
db_trusted = sys.argv[2]
db_refined = sys.argv[3]

canais_varejo = hc.read.table(db_trusted+'.siebel_varejo_sap')\
               .withColumn('ds_canal_final',upper(col('ds_canal_final')))\
               .where( col("ds_ano_mes") == dt_mes_atual)

canais_empresarial = hc.read.table(db_trusted+'.canais_empresariais')\
               .withColumn('ds_canal_final',upper(col('ds_canal_final')))  

zona_competicao = hc.read.table(db_trusted+'.municipios_brasil')\
				   .withColumn('ds_municipio',upper(col('ds_municipio')))\
				   .withColumn('ds_filial',upper(col('ds_filial')))\
				   .select('ds_municipio','ds_filial','ds_municipio_tratado','ds_zona_competicao_tratada')

venda_liquida = hc.read.table('vendas_siebel_varejo')\
             .withColumn('indbd',lit('VLL'))\
		       .withColumn('codigo_sap_original',f.regexp_replace(col('codigo_sap_original'), r'^[0]*',''))\
           .withColumn('mes',col("dt_abertura").substr(1,6))\
           .withColumn('dt_processo',col("dt_abertura"))\
           .withColumn('descricao_canal_original',upper(col('descricao_canal_original')))\
           .withColumn('descricao_canal',upper(col('descricao_canal')))\
           .withColumn('municipio_instalacao',upper(col('municipio_instalacao')))\
           .join(zona_competicao,[(col('municipio_instalacao')== zona_competicao.ds_municipio),col('estado_instalacao') ==zona_competicao.ds_filial],'left')\
           .select('indbd','mes','codigo_sap_original','flg_mig_rentab','descricao_canal','descricao_canal_bov_original','grupo_unidade','dt_processo','estado_instalacao','municipio_instalacao','evento','ds_municipio_tratado','ds_zona_competicao_tratada','codigo_sap_original','flg_venda_valida')\
                 .where( (col("mes") == dt_mes_atual) & (col("evento").isin("VENDA","PROMOCAO",")) & \
                   ( col("flg_descom") == 'N') & (col("GRUPO").isin("EMPRESARIAL","VAREJO")) )


