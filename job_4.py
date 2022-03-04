# Importando modulos builtin
import time
import os

# Importando modulos do PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Importando classes
from Parquet import Parquet
from Conexao import ConexaoCassandra
from PySparkDF import PySparkDF

# Importando funcoes
from funcoes import *


# Criando a sessao Spark
spark = SparkSession\
    .builder\
    .appName("job_4")\
    .config("spark.cassandra.connection.host","10.140.0.2") \
    .config("spark.cassandra.connection.port","9042") \
    .config("spark.cassandra.output.concurrent.writes", 500) \
    .config("spark.cassandra.output.throughputMBPerSec", "128") \
    .config("spark.cassandra.connection.keepAliveMS", "30000") \
    .config("spark.cassandra.output.batch.size.bytes", "4096") \
    .config("spark.cassandra.connection.timeoutMS", 15000).getOrCreate()
    

# Limpando saida
os.system("clear")
print("\nIniciando Job 4\n---------")

# Leitura dos Parquets
try:
    print("\n\tLendo Parquets...")
    inicio = time.time()
    df_autuacao = Parquet('gs://2_parquets_diego/autuacao').ler_parquet(spark)
    df_beneficiada = Parquet('gs://2_parquets_diego/beneficiada').ler_parquet(spark)
    df_pib = Parquet('gs://2_parquets_diego/pib').ler_parquet(spark)
    df_arrecadacao = Parquet('gs://2_parquets_diego/arrecadacao').ler_parquet(spark)
    df_distribuicao = Parquet('gs://2_parquets_diego/distribuicao').ler_parquet(spark)
    df_barragens = Parquet('gs://2_parquets_diego/barragens').ler_parquet(spark)
    df_municipio = Parquet('gs://2_parquets_diego/municipio').ler_parquet(spark)   
except Exception as e:
    print(str(e))
else:
    fim = time.time()
    print(f"\t\t...processo concluido em {(float((fim - inicio)%60))} seg.\n\n")
    gravar_log(f"job_4.py - Parquets lidos em {(float((fim - inicio)%60))}")
    

# ----------------------------------------------


# Analise exploratoria para arrecadacao
# Quantidade de colunas e linhas
#print((df_arrecadacao.count(), len(df_arrecadacao.columns)))
# Schema do Dataframe
#df_arrecadacao.printSchema()
# Primeiros 5 registros
#df_arrecadacao.show(5, truncate=False)  
# Quantidade de campos nulos por coluna
#df_arrecadacao.select([count(when(isnan(c),c)).alias(c)for c in df_arrecadacao.columns]).show()  

# Selecionando campos relevantes para arrecadacao
df_arrecadacao = df_arrecadacao.select(col('ano'),\
	col('mes'),\
	col('ano_do_processo'),\
	col('tipo_pf_pj'),\
	col('substancia'),\
	col('uf'),\
	col('municipio'),\
	col('quantidade_comercializada').cast(FloatType()),\
	col('unidade_medida'),\
    col('valor_recolhido').cast(FloatType()) 
  )

# Tratamento dos campos
arrecadacao = PySparkDF(df_arrecadacao)
df_arrecadacao = arrecadacao.trata_separador("valor_recolhido")
df_arrecadacao = arrecadacao.trata_separador("quantidade_comercializada")
df_arrecadacao = arrecadacao.adiciona_uuid("id_arrecadacao")


# ----------------------------------------------


# Analise exploratoria para autuacao
# Quantidade de colunas e linhas
#print((df_autuacao.count(), len(df_autuacao.columns)))
# Schema do Dataframe
#df_autuacao.printSchema()
# Primeiros 5 registros
#df_autuacao.show(5, truncate=False)  
# Quantidade de campos nulos por coluna
#df_autuacao.select([count(when(isnan(c),c)).alias(c)for c in df_autuacao.columns]).show()  

# Selecionando campos relevantes para analise de autuacao
df_autuacao = df_autuacao.select(col('processo_cobranca'),\
    col('tipo_pf_pj'),\
    col('nome_titular'),\
    col('substancia'),\
    col('municipio'),\
    col('uf'),\
    col('valor').cast(FloatType()))

# Tratamento dos campos
autuacao = PySparkDF(df_autuacao)
df_autuacao = autuacao.trata_separador("valor")
df_autuacao = autuacao.adiciona_uuid("id_autuacao")


# ----------------------------------------------


# Analise exploratoria para barragens
# Quantidade de colunas e linhas
#print((df_barragens.count(), len(df_barragens.columns)))
# Schema do Dataframe
#df_barragens.printSchema()
# Primeiros 5 registros
#df_barragens.show(5, truncate=False)  
# Quantidade de campos nulos por coluna
#df_barragens.select([count(when(isnan(c),c)).alias(c)for c in df_barragens.columns]).show()  

# Selecionando campos relevantes para analise de barragens
df_barragens = df_barragens.select(col('nome'),\
    col('empreendedor'),\
    col('uf'),\
    col('municipio'),\
    col('latitude'),\
    col('longitude'),\
    col('situacao_operacional'),\
    col('desde'),\
    col('vida_util_prevista_anos'),\
    col('minerio_principal'),\
    col('pessoas_possivelmente_afetadas_rompimento'),\
    col('impacto_ambiental')
   )


# Tratamento dos campos
barragens = PySparkDF(df_barragens)
df_barragens = barragens.adiciona_uuid("id_barragens")


# ----------------------------------------------


# Analise exploratoria para beneficiada
# Quantidade de colunas e linhas
#print((df_beneficiada.count(), len(df_beneficiada.columns)))
# Schema do Dataframe
#df_beneficiada.printSchema()
# Primeiros 5 registros
#df_beneficiada.show(5, truncate=False)  
# Quantidade de campos nulos por coluna
#df_beneficiada.select([count(when(isnan(c),c)).alias(c)for c in df_beneficiada.columns]).show()  

# Selecionando campos relevantes para analise de beneficiada
df_beneficiada = df_beneficiada.select(col('ano_base'),\
	col('uf'),\
	col('substancia_mineral'),\
	col('quantidade_producao'),\
	col('unidade_de_medida_producao'),\
	col('quantidade_contido'),\
	col('unidade_de_medida_contido'),\
    col('quantidade_venda'),\
    col('unidade_de_medida_venda'),\
    col('valor_venda_rs').cast(FloatType())
     )

# Tratamento dos campos
beneficiada = PySparkDF(df_beneficiada)
df_beneficiada = beneficiada.trata_separador("valor_venda_rs")
df_beneficiada = beneficiada.trata_separador("quantidade_venda")
df_beneficiada = beneficiada.trata_separador("unidade_de_medida_producao")
df_beneficiada = beneficiada.adiciona_uuid("id_beneficiada")


# ----------------------------------------------


# Analise exploratoria para distribuicao
# Quantidade de colunas e linhas
#print((df_distribuicao.count(), len(df_distribuicao.columns)))
# Schema do Dataframe
#df_distribuicao.printSchema()
# Primeiros 5 registros
#df_distribuicao.show(5, truncate=False)  
# Quantidade de campos nulos por coluna
#df_distribuicao.select([count(when(isnan(c),c)).alias(c)for c in df_distribuicao.columns]).show()  

# Selecionando campos relevantes para analise de distribuicao
df_distribuicao = df_distribuicao.select(col('ano'),\
	col('mes'),\
	col('ente'),\
	col('sigla_estado'),\
	col('nome_ente'),\
    col('tipo_distribuicao'),\
	col('substancia'),\
    col('valor').cast(FloatType())
    )

# Tratamento dos campos
distribuicao = PySparkDF(df_distribuicao)
df_distribuicao = distribuicao.trata_separador("valor")
df_distribuicao = distribuicao.adiciona_uuid("id_distribuicao")


# ----------------------------------------------


# Analise exploratoria para municipio
# Quantidade de colunas e linhas
#print((df_municipio.count(), len(df_municipio.columns)))
# Schema do Dataframe
#df_municipio.printSchema()
# Primeiros 5 registros
#df_municipio.show(5, truncate=False)  
# Quantidade de campos nulos por coluna
#df_municipio.select([count(when(isnan(c),c)).alias(c)for c in df_municipio.columns]).show()  

# Selecionando campos relevantes para analise de municipio
df_municipio = df_municipio.select(col('uf'),\
	col('cod_uf'),\
	col('cod_munic'),\
	col('nome_do_municipio'),\
	col('populacao_estimada').cast(FloatType())
    )

# Tratamento dos campos
municipio = PySparkDF(df_municipio)
df_municipio = municipio.adiciona_uuid("id_municipio")


# ----------------------------------------------


# Analise exploratoria para pib
# Quantidade de colunas e linhas
#print((df_pib.count(), len(df_pib.columns)))
# Schema do Dataframe
#df_pib.printSchema()
# Primeiros 5 registros
#df_pib.show(5, truncate=False)  
# Quantidade de campos nulos por coluna
#df_pib.select([count(when(isnan(c),c)).alias(c)for c in df_pib.columns]).show()  

# Selecionando campos relevantes para analise de pib
df_pib = df_pib.select(col('ano'),\
	col('id_municipio').cast(IntegerType()),\
	col('pib').cast(FloatType())
    )

# Tratamento dos campos
pib = PySparkDF(df_pib)
df_pib = pib.adiciona_uuid("id_pib")


# ----------------------------------------------


# Gravando em Parquets pos tratamento
try:
    print("\nGravando parquets pos tratamento...")
    inicio = time.time()
    parquet = Parquet("gs://3_parquets_diego/arrecadacao").grava_parquet(df_arrecadacao)
    parquet = Parquet("gs://3_parquets_diego/autuacao").grava_parquet(df_autuacao)
    parquet = Parquet("gs://3_parquets_diego/barragens").grava_parquet(df_barragens)
    parquet = Parquet("gs://3_parquets_diego/beneficiada").grava_parquet(df_beneficiada)
    parquet = Parquet("gs://3_parquets_diego/distribuicao").grava_parquet(df_distribuicao)
    parquet = Parquet("gs://3_parquets_diego/municipio").grava_parquet(df_municipio)
    parquet = Parquet("gs://3_parquets_diego/pib").grava_parquet(df_pib)
except Exception as e:
    print(str(e))
    gravar_log(f"job_4.py - Falha ao gravar parquet pos tratamento")
else:
    fim = time.time()
    print(f"\t\t...processo concluido em {(float((fim - inicio)%60))} seg.\n\n")
    gravar_log(f"job_4.py - Parquets tratados salvos em {(float((fim - inicio)%60))}")


# Populando Cassandra
try:
    conexao = ConexaoCassandra("mineracao4")
    conexao.inserir_dados(df_arrecadacao, "arrecadacao")
    conexao.inserir_dados(df_autuacao, "autuacao")
    conexao.inserir_dados(df_barragens, "barragens")
    conexao.inserir_dados(df_beneficiada, "beneficiada")
    conexao.inserir_dados(df_distribuicao, "distribuicao")
    conexao.inserir_dados(df_municipio, "municipio")
    conexao.inserir_dados(df_pib, "pib")
except Exception as e:
    print(str(e))
    gravar_log(f"job_4.py - Falha ao inserir registros no cassandra")
else:
    fim = time.time()
    print(f"\t\t...processo concluido em {(float((fim - inicio)%60))} seg.\n\n")
    gravar_log(f"job_4.py - Populando Cassandra em {(float((fim - inicio)%60))}")