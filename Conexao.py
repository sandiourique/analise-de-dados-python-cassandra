# Classe para Conexao com Banco
class ConexaoPostgres:
    """Classe para realizar conexao com banco Postgres

    Atraves desta classe :class:`ConexaoPostgres`, podemos realizar a conexao com
    o banco de dados Postgres, consultar dados e inserir dados.
    Para utilizar a classe :class:`ConexaoPostgres`, a implementacao ocorre da
    seguinte forma:

    .. autoattribute:: builder
    :annotation:

    Exemplo do metodo construtor
    ----------------------------
    >>> objeto = ConexaoPostgres(host, nome_banco, usuario, senha)

    Metodo para inserir dados em uma tabela
    ---------------------------------------
    >>> objeto.inserir_dados(PySparkDataFrame, nome_tabela)

    Metodo para ler dados em uma tabela
    -----------------------------------
    >>> objeto.ler_dados(sessao_spark, tabela_origem)
    
    Retorna PySparkDataFrame

    """
    
class ConexaoPostgres:
    
    def __init__(self, host, banco, usuario, senha):
        self.host, self.banco, self.usuario, self.senha = host, banco, usuario, senha
        
    def inserir_dados(self,df,tabela):
        try: 
            df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{self.host}:5432/{self.banco}") \
                .option("numPartitions", "4") \
                .option("fetchsize", "5000") \
                .option("dbtable", tabela) \
                .option("driver", "org.postgresql.Driver") \
                .option("user", self.usuario) \
                .option("password", self.senha) \
                .mode('append')\
                .save()
        except Exception as e:
            print(str(e))
   
    def ler_dados(self,spark,tabela):
        try: 
            df = spark.read \
                .format("jdbc") \
                .option("numPartitions", "4") \
                .option("fetchsize", "50000") \
                .option("url", f"jdbc:postgresql://{self.host}:5432/{self.banco}") \
                .option("dbtable", tabela) \
                .option("driver", "org.postgresql.Driver") \
                .option("user", self.usuario) \
                .option("password", self.senha) \
                .load()
        except Exception as e:
            print(str(e))
        else:
            return df
        
        
# Classe para Conexao com Banco
class ConexaoCassandra:
    """Classe para realizar conexao com banco Cassandra

    Atraves desta classe :class:`ConexaoCassandra`, podemos realizar a conexao com
    o banco de dados Cassandra, consultar dados e inserir dados.
    Para utilizar a classe :class:`ConexaoCassandra`, a implementacao ocorre da
    seguinte forma:

    .. autoattribute:: builder
    :annotation:

    Exemplo do metodo construtor
    ----------------------------
    >>> objeto = ConexaoCassandra(host, nome_keyspace)

    Metodo para inserir dados em uma familia
    ----------------------------------------
    >>> objeto.inserir_dados(PySparkDataFrame, nome_familia)

    Metodo para ler dados em uma familia
    ------------------------------------
    >>> objeto.ler_dados(sessao_spark, familia_origem)
    
    Retorna PySparkDataFrame

    """
    
    # Metodo construtor da classe
    def __init__(self, keyspace):
        self.keyspace = keyspace
        
    def inserir_dados(self, df, familia):
        try: 
            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .option("keyspace",self.keyspace) \
                .option("table", familia) \
                .mode('append') \
                .save()
        except Exception as e:
            print("Erro ao inserir no Cassandra" + str(e))
            
    def ler_dados(self, spark, familia):
        try: 
            df = spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .option("keyspace",self.keyspace) \
                .option("table", familia) \
                .load()
        except Exception as e:
            print(str(e))
        else:
            return df