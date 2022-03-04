# Importando modulos builtin
import time
import os

# Importando modulos do PySpark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *

# Importando classes
from Conexao import ConexaoPostgres

# Importando funcoes
from funcoes import *


# Criando sessao Spark
spark = SparkSession.builder.appName('job_2').getOrCreate()


# Limpando saida
#os.system("clear")
print("\nIniciando Job 2\n---------")


# Leitura dos Parquets
try:
    print("\n\tLendo Parquets...")
    inicio = time.time()
    df_autuacao = spark.read.parquet('gs://1_parquets_diego/autuacao')
    df_beneficiada = spark.read.parquet('gs://1_parquets_diego/beneficiada')
    df_municipio = spark.read.parquet('gs://1_parquets_diego/municipio')
    df_pib = spark.read.parquet('gs://1_parquets_diego/pib')
    df_arrecadacao = spark.read.parquet('gs://1_parquets_diego/arrecadacao')
    df_distribuicao = spark.read.parquet('gs://1_parquets_diego/distribuicao')
    df_barragens = spark.read.parquet('gs://1_parquets_diego/barragens')
except Exception as e:
    print(str(e))
    gravar_log(f"job_2.py - Falha ao ler Parquets")
else:
    fim = time.time()
    print(f"\t\t...processo concluido em {(float((fim - inicio)%60))} seg.\n\n")
    gravar_log(f"job_2.py - Parquets lidos em {(float((fim - inicio)%60))}")


# Inserindo dados no banco
try:
    total = 0
    print("\n\tInserindo dados no banco...")
    conexao = ConexaoPostgres("34.134.0.208","mineracao","postgres","novasenha123")
    total = total + df_arrecadacao.count()
    conexao.inserir_dados(df_arrecadacao, "arrecadacao")
    total = total + df_autuacao.count()
    conexao.inserir_dados(df_autuacao, "autuacao")
    total = total + df_barragens.count()
    conexao.inserir_dados(df_barragens, "barragens")
    total = total + df_beneficiada.count()
    conexao.inserir_dados(df_beneficiada, "beneficiada")
    total = total + df_distribuicao.count()
    conexao.inserir_dados(df_distribuicao, "distribuicao")
    total = total + df_municipio.count()
    conexao.inserir_dados(df_municipio, "municipio")
    total = total + df_pib.count()    
    conexao.inserir_dados(df_pib, "pib")
except Exception as e:
    print(str(e))
    gravar_log(f"job_2.py - Falha ao inserir registros no PG")
else:
    fim = time.time()
    print(f"\t\t...processo concluido em {(float((fim - inicio)%60))} seg.\n\n")
    gravar_log(f"job_2.py - {total} registros inseridos no PG em {(float((fim - inicio)%60))}")