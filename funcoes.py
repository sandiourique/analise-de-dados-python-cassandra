# Importando modulos builtin
import datetime as dt
import unittest

def gravar_log(conteudo):
    """Função para gravar no arquivo de log system.log 
    """
    try:
        linha = f"{str(dt.datetime.now())} {conteudo}\n"
        with open('system.log', 'a') as f:
            f.writelines(''.join(linha))
    except Exception as e:
        print(str(e))
    else:
        return True

import unittest

class TesteFuncoes(unittest.TestCase):
    def teste_grava_log(self):
        self.assertEqual(gravar_log("Testando log"), True)