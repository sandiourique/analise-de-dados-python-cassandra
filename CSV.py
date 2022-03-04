# Classe CSV
class CSV:
    """Classe para a manipulacao de arquivos do tipo CSV.

    Atraves desta classe :class:`CSV`, podemos realizar leitura de CSVs.
    Para utilizar a classe :class:`CSV`, a implementacao ocorre da
    seguinte forma:

    .. autoattribute:: builder
    :annotation:

    Exemplo do metodo construtor
    ----------------------------
    >>> objeto = CSV(delimitador, encoding, caminho_do_arquivo, schema)

    Metodo para ler dados de CSV
    ----------------------------
    >>> objeto.ler_csv(sessao_spark)

    Retorna PySparkDataFrame

    """
    
        
    def __init__(self, delimiter, encoding, caminho, schema):
        self.delimiter, self.encoding, self.caminho, self.schema = delimiter, encoding, caminho, schema
        
    def ler_csv(self, spark):
        try: 
            df = spark.read.format("csv").option("header", "true")\
            .option("interSchema", "false")\
            .option("delimiter", self.delimiter)\
            .option("encoding", self.encoding)\
            .load(self.caminho, schema=self.schema)
        except Exception as e:
            print(str(e))
        return df