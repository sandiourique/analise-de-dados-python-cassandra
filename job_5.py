# Importando modulos builtin
import time
import datetime as dt

# Importando modulos do PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas

# Importando classes
from Conexao import ConexaoCassandra
from PySparkDF import PySparkDF

# Importando funcoes
from funcoes import *


# Criando a sessao Spark
spark = SparkSession\
    .builder\
    .appName("job_5")\
    .config("spark.cassandra.connection.host","10.140.0.2") \
    .config("spark.cassandra.connection.port","9042") \
    .config("spark.cassandra.output.batch.size.bytes", "4096").getOrCreate()


# Leitura Cassandra
try:
    print("\nLeitura Cassandra...")
    inicio = time.time()
    conexao = ConexaoCassandra("mineracao4")
    df_arrecadacao = conexao.ler_dados(spark,"arrecadacao")
    df_autuacao = conexao.ler_dados(spark,"autuacao")
    df_barragens = conexao.ler_dados(spark,"barragens")
    df_beneficiada = conexao.ler_dados(spark,"beneficiada")
    df_distribuicao = conexao.ler_dados(spark,"distribuicao")
    df_municipio = conexao.ler_dados(spark,"municipio")
    df_pib = conexao.ler_dados(spark,"pib")
except Exception as e:
    print(str(e))
    gravar_log(f"job_5.py - Falha ao ler registros no cassandra")
else:
    fim = time.time()
    print(f"\t\t...processo concluido em {(float((fim - inicio)%60))} seg.\n\n")
    gravar_log(f"job_5.py - Leitura Cassandra em {(float((fim - inicio)%60))}")


df_barragens_rafael = df_barragens
df_municipio_rafael = df_municipio
df_beneficiada_rafael = df_beneficiada

# # --------------------------------------------------------------------------
# # Geracao de csvs para analise final (serao utilizados pelo streamlit)

#Análise Diego
# 10 mais autuados com quantidade de autuacoes
df_autuacao.createOrReplaceTempView("view_autuacao")
query_1 = "SELECT nome_titular, count(nome_titular) AS qtd FROM view_autuacao GROUP BY nome_titular ORDER BY qtd DESC LIMIT 10"
df_autuacao_analise_1_ = spark.sql(query_1)
df_autuacao_analise_1_ = df_autuacao_analise_1_.toPandas()
df_autuacao_analise_1_.to_csv(r'gs://4_analises_diego/diego_autuacao_analise_1.csv')


# # mapa de barragens no brasil
df_barragens.createOrReplaceTempView("view_barragens")
query_2 = "SELECT latitude AS lat, longitude AS lon FROM view_barragens WHERE situacao_operacional = 'Em Operação'"
df_autuacao_analise_2_ = spark.sql(query_2)
df_autuacao_analise_2_ = df_autuacao_analise_2_.toPandas()
df_autuacao_analise_2_.to_csv(r'gs://4_analises_diego/diego_autuacao_analise_2.csv')


# serie historica para distribuicao de valores relativos a FERRO
df_distribuicao.createOrReplaceTempView("view_distribuicao")
query_3 = "SELECT ano, mes, valor, substancia, sigla_estado AS uf FROM view_distribuicao WHERE ano = '2020' AND substancia = 'FERRO'"
df_autuacao_diego_analise_3_ = spark.sql(query_3)
df_autuacao_diego_analise_3_ = df_autuacao_diego_analise_3_.toPandas()
df_autuacao_diego_analise_3_ = df_autuacao_diego_analise_3_.groupby(["substancia","mes"], as_index=False)["valor"].sum()
df_autuacao_diego_analise_3_.to_csv(r'gs://4_analises_diego/diego_distribuicao_analise_4.csv')


# --------------------------------------------------------------------------
#Tratamento barragens

#Análises - Rayssa e Sandi

df_barragens = df_barragens.select(
    col('empreendedor'),\
    col('uf'),\
    col('minerio_principal'),\
    col('situacao_operacional')
    )


df_barragens_ferro = df_barragens.filter("minerio_principal in ('Minério de Ferro')")
df_barragens_ferro = df_barragens_ferro.filter("situacao_operacional in ('Em Operação')")
# df_barragens_ferro.show()

df_barragens_ferro_analise = df_barragens_ferro.toPandas()
df_estados = df_barragens_ferro_analise['uf'].value_counts()
df_estados.to_csv(r'gs://4_analises_diego/sandi_rayssa_estados_ferro.csv')
print(df_estados)


df_barragens_ferro_analise_2 = df_barragens_ferro.toPandas()
df_empreendedor = df_barragens_ferro_analise_2['empreendedor'].value_counts()
df_empreendedor.to_csv(r'gs://4_analises_diego/sandi_rayssa_empreendedor.csv')
print(df_empreendedor)




# # # -----------------------------

df_beneficiada = df_beneficiada.filter("substancia_mineral in ('Ferro')")

df_beneficiada_total_vendas_ferro = df_beneficiada.select(
    col('ano_base').alias('Ano'),\
    col('substancia_mineral'),\
    col('uf'),\
    col('quantidade_venda').alias("Quantidade de Vendas"),\
    col('unidade_de_medida_venda').alias("Unidade de Medida da Venda"),\
    col('valor_venda_rs'),\
   )

#exportando csv
df_beneficiada_total_vendas_ferro_1 = df_beneficiada_total_vendas_ferro.toPandas()
df_beneficiada_total_vendas_ferro_1.to_csv(r'gs://4_analises_diego/analise_ferro_venda_brasil.csv')



# Beneficiada Produção
# Analise beneficiada1 - quantidade de produção quantidade produção e unidade de medida
df_beneficiada = df_beneficiada.filter("substancia_mineral in ('Ferro')")
df_beneficiada  = df_beneficiada.filter("UF in ('MG')")

df_beneficiada_producao = df_beneficiada.select(
    col('ano_base').alias('Ano'),\
    col('substancia_mineral'),\
    col('uf'),\
    col('quantidade_producao').alias("Produção"),\
    col('unidade_de_medida_producao').alias("Unidade de Medida"),\
   )

df_beneficiada_producao.show()

df_beneficiada_producao_1 = df_beneficiada_producao.select(
    col('Ano'),\
    col('Produção')
    )

#exportando csv
df_beneficiada_analise_1 = df_beneficiada_producao_1.toPandas()
df_beneficiada_analise_1.to_csv(r'gs://4_analises_diego/sandi_rayssa_beneficiada_analise_producao.csv')


df_beneficiada_venda = df_beneficiada.select(
    col('ano_base').alias('Ano'),\
    col('substancia_mineral'),\
    col('uf'),\
    col('quantidade_venda').alias("Quantidade de Vendas"),\
    col('unidade_de_medida_venda').alias("Unidade de Medida da Venda"),\
    col('valor_venda_rs'),\
   )

df_beneficiada_venda.show()

df_beneficiada_venda_1 = df_beneficiada_venda.select(
    col('Ano'),\
    col('Quantidade de Vendas')
    )

# # exportando csv
df_beneficiada_analise_venda_1 = df_beneficiada_venda_1.toPandas()
df_beneficiada_analise_venda_1.to_csv(r'gs://4_analises_diego/sandi_rayssa_beneficiada_analise_venda_1.csv')


df_beneficiada_venda_2 = df_beneficiada_venda.select(
    col('Ano'),\
    col('valor_venda_rs').alias("Valor Vendido em Reais")
    )

# exportando csv
df_beneficiada_analise_venda_2 = df_beneficiada_venda_2.toPandas()
df_beneficiada_analise_venda_2.to_csv(r'gs://4_analises_diego/sandi_rayssa_beneficiada_analise_venda_2.csv')



## Análises Rafael

''' ----------------------------------------- Análises Rafa ----------------------------------------- '''

df_barragens_rafael.createOrReplaceTempView("view_analise_1")

# - Quais as Mineradoras que Produzem Carvão, e qual o impacto ambiental elas causam? (CORRIGIR O LIKE PARA CARVÃO)
query_r_1 = "SELECT empreendedor,nome,uf, minerio_principal, impacto_ambiental FROM view_analise_1 WHERE minerio_principal LIKE '%Carvão%';"
df_analise_rafa_1 = spark.sql(query_r_1)
df_analise_rafa_1 = df_analise_rafa_1.toPandas()
df_analise_rafa_1.to_csv(r'gs://4_analises_diego/rafael_analise_1.csv')

df_barragens_rafael.createOrReplaceTempView("view_analise_2")

# - 2 Destaque para o Impacto Ambiental da Copelmi Mineração LTDA.
query_r_2 = "SELECT empreendedor, nome, minerio_principal, impacto_ambiental FROM view_analise_2 WHERE empreendedor LIKE '%Copelmi%';"
df_analise_rafa_2 = spark.sql(query_r_2)
df_analise_rafa_2 = df_analise_rafa_2.toPandas()
df_analise_rafa_2.to_csv(r'gs://4_analises_diego/2_analise_rafa.csv')

df_barragens_rafael.createOrReplaceTempView("view_analise_3_0")

#- 3.0 Qual o impacto da Mineração de Ferro da Vale 
query_r_3_0 = "SELECT nome, empreendedor, minerio_principal, impacto_ambiental FROM view_analise_3_0 WHERE minerio_principal LIKE '%Ferro%' AND empreendedor LIKE '%VALE%';"
df_analise_rafa_3_0 = spark.sql(query_r_3_0)
df_analise_rafa_3_0 = df_analise_rafa_3_0.toPandas()
df_analise_rafa_3_0.to_csv(r'gs://4_analises_diego/3_0_analise_rafa.csv')

df_municipio_rafael.createOrReplaceTempView("view_analise_3_1")

# -- 3.1 Com base na Mina de Forquilha I", qual a população do Município onde ela se localiza?
query_r_3_1 = "SELECT nome_do_municipio, uf, populacao_estimada FROM view_analise_3_1 WHERE nome_do_municipio LIKE '%Ouro Preto';"
df_analise_rafa_3_1 = spark.sql(query_r_3_1)
df_analise_rafa_3_1 = df_analise_rafa_3_1.toPandas()
df_analise_rafa_3_1.to_csv(r'gs://4_analises_diego/3_1_analise_rafa.csv')


df_barragens_rafael.createOrReplaceTempView("view_analise_4")

# -- 4 Mostre a situação operacional das Minas de Ferro da USIMINAS
query_r_4 = "SELECT situacao_operacional, desde, nome, empreendedor, minerio_principal FROM view_analise_4 WHERE empreendedor LIKE '%USIMINAS%' AND minerio_principal LIKE '%Ferro%';"
df_analise_rafa_4 = spark.sql(query_r_4)
df_analise_rafa_4 = df_analise_rafa_4.toPandas()
df_analise_rafa_4.to_csv(r'gs://4_analises_diego/4_analise_rafa.csv')

df_barragens_rafael.createOrReplaceTempView("view_analise_4_1")

# -- 4.1 Mostre a data em que a USIMINAS desativou uma de suas Minas de Ferro
query_r_4_1 = "SELECT situacao_operacional,desde,empreendedor, minerio_principal, pessoas_possivelmente_afetadas_rompimento FROM view_analise_4_1 WHERE empreendedor LIKE '%USIMINAS%' AND minerio_principal LIKE '%Ferro%' AND desde LIKE '%2021%';"
df_analise_rafa_4_1 = spark.sql(query_r_4_1)
df_analise_rafa_4_1 = df_analise_rafa_4_1.toPandas()
df_analise_rafa_4_1.to_csv(r'gs://4_analises_diego/4_1_analise_rafa.csv')

df_barragens_rafael.createOrReplaceTempView("view_analise_5")

# -- 5 Mostre a quantidade de barragens ativas no Brasil
query_r_5 = "SELECT COUNT(*) situacao_operacional FROM view_analise_5 WHERE situacao_operacional = 'Em Operação';"
df_analise_rafa_5 = spark.sql(query_r_5)
df_analise_rafa_5 = df_analise_rafa_5.toPandas()
df_analise_rafa_5.to_csv(r'gs://4_analises_diego/5_analise_rafa.csv')

df_beneficiada_rafael.createOrReplaceTempView("view_analise_6")
# -- 6 Preço do Carvão no Estado da Maior do Brasil do Mundo entre 2010 - 2020
query_r_6 = "SELECT ano_base,uf,substancia_mineral, valor_venda_rs FROM view_analise_6 WHERE substancia_mineral LIKE '%Carvão%' and uf LIKE 'RS';"
df_analise_rafa_6 = spark.sql(query_r_6)
df_analise_rafa_6 = df_analise_rafa_6.toPandas()
df_analise_rafa_6.to_csv(r'gs://4_analises_diego/6_analise_rafa.csv')


df_beneficiada_rafael.createOrReplaceTempView("view_analise_7")

# -- 7 Preço do Ferro no Brasil entre 2010 - 2020
query_r_7 = "SELECT ano_base,uf,substancia_mineral, valor_venda_rs FROM view_analise_7 WHERE substancia_mineral LIKE '%Ferro%';;"
df_analise_rafa_7 = spark.sql(query_r_7)
df_analise_rafa_7 = df_analise_rafa_7.toPandas()
df_analise_rafa_7.to_csv(r'gs://4_analises_diego/7_analise_rafa.csv')

gravar_log(f"job_5.py - CSVs para analise gerados com sucesso")