# Classe para tratamento de Exception
class ErroTypeDataframe(Exception):
    """Classe para tratar excecoes

    Atraves desta classe :class:`ErroTypeDataframe`, podemos tratar excecoes levantadas
    no momento de manipular PySpark DataFrames.
    A classe compara o tipo do atributo e levanta excecao caso o tipo seja diferente do
    esperado.

    """
    
    def __init__(self, message="Type error PySpark DataFrame"):
        self.message = message
        super().__init__(self.message)