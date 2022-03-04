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


# Classe PySparkDF
class PySparkDF:   
    """Classe para manipulacao e tratamento de objetos do formato PySparkDataFrame.

    Atraves desta classe :class:`PySparkDF`, podemos realizar manipulacao
    e tratamento de objetos do formato PySparkDataFrame
    Para utilizar a classe :class:`PySparkDF`, a implementacao ocorre da
    seguinte forma:

    .. autoattribute:: builder
    :annotation:

    Exemplo do metodo construtor
    ----------------------------
    >>> objeto = PySparkDF(PySparkDF)

    Metodo para tratar separador de campos numerico
    -----------------------------------------------
    >>> objeto.trata_separador(campo)
    
    Retorna PySparkDataFrame

    Metodo para adicionar campo UUID
    --------------------------------
    >>> objeto.adiciona_uuid(campo)
    
    Retorna PySparkDataFrame

    """
    
    
# Importacao de Classe para tratamento de excecoes
from Erros import ErroTypeDataframe


# Classe PySparkDF
class PySparkDF:   
    
    def __init__(self, df):
        if str(type(df)) == "<class 'pyspark.sql.dataframe.DataFrame'>":
            self.df = df
        else:
            raise ErroTypeDataframe()

    def trata_separador(self, campo):
        try: 
            self.df = self.df.withColumn(campo, regexp_replace(campo, "," , "."))
        except Exception as e:
            print(str(e))
        return self.df
    
    def adiciona_uuid(self, campo):
        try: 
            self.df = self.df.withColumn(campo, expr("uuid()"))
        except Exception as e:
            print(str(e))
        return self.df