# Importando modulos builtin
import time
import datetime as dt
import uuid, os, re

# Importando modulos do PySpark
import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Importacao de Classe para tratamento de excecoes
from Erros import ErroTypeDataframe          


# Classe Parquet
class Parquet:
    """Classe para a manipulacao de arquivos do tipo Parquet.

    Atraves desta classe :class:`Parquet`, podemos realizar gravacao e
    leitura de Parquets.
    Para utilizar a classe :class:`Parquet`, a implementacao ocorre da
    seguinte forma:

    .. autoattribute:: builder
    :annotation:

    Exemplo do metodo construtor
    ----------------------------
    >>> objeto = Parquet(caminho_do_parquet)

    Metodo para gravacao de dados em Parquets
    -----------------------------------------
    >>> objeto.grava_parquet(PySparkDataFrame)

    Metodo para ler dados de Parquets
    ---------------------------------
    >>> objeto.ler_parquet(sessao_spark)
    
    Retorna PySparkDataFrame

    """
    
    def __init__(self, caminho):
        self.caminho = caminho
        
    def grava_parquet(self, df):
        if str(type(df)) == "<class 'pyspark.sql.dataframe.DataFrame'>":
            df.write.mode("overwrite").parquet(self.caminho)
        else:
            raise ErroTypeDataframe()
            
    def ler_parquet(self, spark):
        try: 
            df = spark.read.parquet(self.caminho)
        except Exception as e:
            print(str(e))
        return df