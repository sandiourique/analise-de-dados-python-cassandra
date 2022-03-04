# Importando modulos builtin
import time
import os

# Importando modulos do PySpark
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Importando classes
from Parquet import Parquet
from CSV import CSV

# Importando funcoes
from funcoes import *


# Criando sessao Spark
spark = SparkSession.builder.appName('job_1').getOrCreate()


# Limpando saida
os.system("clear")
print("\nIniciando Job 1\n---------")


# Definindo Schema para Dataset arrecadacao
schema_arrecadacao = StructType([
    StructField('ano',  StringType(), True),
    StructField('mes',  StringType(), True),
    StructField('processo',  StringType(), True),
    StructField('ano_do_processo',  StringType(), True),
    StructField('tipo_pf_pj',  StringType(), True),
    StructField('cpf_cnpj',  StringType(), True),
    StructField('substancia',  StringType(), True),
    StructField('uf',  StringType(), True),
    StructField('municipio',  StringType(), True),
    StructField('quantidade_comercializada',  StringType(), True),
    StructField('unidade_medida',  StringType(), True),
    StructField('valor_recolhido',  StringType(), True)
]) 


# Definindo Schema para Dataset autuacao
schema_autuacao = StructType([
    StructField('processo_cobranca',  StringType(), True),
    StructField('ano_publicacao',  StringType(), True),
    StructField('mes_publicacao',  StringType(), True),
    StructField('tipo_pf_pj',  StringType(), True),
    StructField('cpf_cnpj',  StringType(), True),
    StructField('nome_titular',  StringType(), True),
    StructField('numero_auto',  StringType(), True),
    StructField('processo_minerario',  StringType(), True),
    StructField('substancia',  StringType(), True),
    StructField('municipio',  StringType(), True),
    StructField('uf',  StringType(), True),
    StructField('valor',  StringType(), True)
])


# Definindo Schema para Dataset barragens
schema_barragens = StructType([
    StructField('id_barragem',  StringType(), True),
    StructField('nome',  StringType(), True),
    StructField('empreendedor',  StringType(), True),
    StructField('cpf_cnpj',  StringType(), True),
    StructField('uf',  StringType(), True),
    StructField('municipio',  StringType(), True),
    StructField('latitude',  StringType(), True),
    StructField('longitude',  StringType(), True),
    StructField('posicionamento',  StringType(), True),
    StructField('categoria_de_risco',  StringType(), True),
    StructField('dano_potencial_associado',  StringType(), True),
    StructField('classe',  StringType(), True),
    StructField('necessita_de_paebm',  StringType(), True),
    StructField('inserido_na_pnsb',  StringType(), True),
    StructField('nivel_de_emergencia',  StringType(), True),
    StructField('status_da_dce_atual',  StringType(), True),
    StructField('tipo_de_barragem',  StringType(), True),
    StructField('possui_outra_estrutura_mineracao_interna',  StringType(), True),
    StructField('quantidade_diques_internos',  StringType(), True),
    StructField('quantidade_diques_selantes',  StringType(), True),
    StructField('possui_bud',  StringType(), True),
    StructField('bud_opera_pos_rompimento',  StringType(), True),
    StructField('nome_da_bud',  StringType(), True),
    StructField('uf_bud',  StringType(), True),
    StructField('municipio_bud',  StringType(), True),
    StructField('situacao_operacional_da_bud',  StringType(), True),
    StructField('desde_bud',  StringType(), True),
    StructField('vida_util_prevista_da_bud_anos',  StringType(), True),
    StructField('previsao_termino_construcao_bud',  StringType(), True),
    StructField('bud_anm_ou_servidao',  StringType(), True),
    StructField('processos_associados_bud',  StringType(), True),
    StructField('posicionamento_bud',  StringType(), True),
    StructField('latitude_bud',  StringType(), True),
    StructField('longitude_bud',  StringType(), True),
    StructField('altura_maxima_projeto_da_bud_m',  StringType(), True),
    StructField('comprimento_crista_projeto_bud_m',  StringType(), True),
    StructField('volume_do_projeto_da_bud_m',  StringType(), True),
    StructField('descarga_maxima_do_vertedouro_da_dud_m_seg',  StringType(), True),
    StructField('existe_documento',  StringType(), True),
    StructField('existe_manual',  StringType(), True),
    StructField('passou_por_auditoria',  StringType(), True),
    StructField('garante_a_reducao_mancha',  StringType(), True),
    StructField('tipo_de_bud_material_de_construcao',  StringType(), True),
    StructField('tipo_de_fundacao_da_bud',  StringType(), True),
    StructField('vazao_de_projeto_da_bud',  StringType(), True),
    StructField('metodo_construtivo_da_bud',  StringType(), True),
    StructField('tipo_de_auscultacao_da_bud',  StringType(), True),
    StructField('situacao_operacional',  StringType(), True),
    StructField('desde',  StringType(), True),
    StructField('vida_util_prevista_anos',  StringType(), True),
    StructField('estrutura_objetivo_de_contencao',  StringType(), True),
    StructField('bm_dentro_anm_ou_servidao',  StringType(), True),
    StructField('bm_alimentado_usina',  StringType(), True),
    StructField('usinas',  StringType(), True),
    StructField('minerio_principal',  StringType(), True),
    StructField('processo_de_beneficiamento',  StringType(), True),
    StructField('produtos_quimicos_utilizados',  StringType(), True),
    StructField('armazena_rejeitos_com_cianeto',  StringType(), True),
    StructField('teor_do_minerio_principal_rejeito',  StringType(), True),
    StructField('outras_substancias_presentes',  StringType(), True),
    StructField('altura_maxima_do_projeto_licenciado_m',  StringType(), True),
    StructField('altura_maxima_atual_m',  StringType(), True),
    StructField('comprimento_da_crista_do_projeto_m',  StringType(), True),
    StructField('comprimento_atual_da_crista_m',  StringType(), True),
    StructField('descarga_maxima_do_vertedouro_m_seg',  StringType(), True),
    StructField('area_do_reservatorio_m',  StringType(), True),
    StructField('tipo_de_barragem_ao_material_construcao',  StringType(), True),
    StructField('tipo_de_fundacao',  StringType(), True),
    StructField('vazao_de_projeto',  StringType(), True),
    StructField('metodo_construtivo_da_barragem',  StringType(), True),
    StructField('tipo_de_alteamento',  StringType(), True),
    StructField('tipo_de_auscultacao',  StringType(), True),
    StructField('bm_possui_manta_impermeabilizante',  StringType(), True),
    StructField('data_da_ultima_vistoria',  StringType(), True),
    StructField('confiabilidade_das_estruturas',  StringType(), True),
    StructField('percolacao',  StringType(), True),
    StructField('deformacoes_e_recalque',  StringType(), True),
    StructField('deteriorizacao_dos_taludes_paramentos',  StringType(), True),
    StructField('documentacao_de_projeto',  StringType(), True),
    StructField('qualificacao_tecnica_profissionais',  StringType(), True),
    StructField('manuais_seguranca_e_monitoramento',  StringType(), True),
    StructField('pae_exigido_fiscalizador',  StringType(), True),
    StructField('copias_do_paebm_entregues',  StringType(), True),
    StructField('relatorios_de_inspecao',  StringType(), True),
    StructField('volume_projeto_licenciado_reservatorio_m',  StringType(), True),
    StructField('volume_atual_do_reservatorio_m',  StringType(), True),
    StructField('existencia_de_populacao_a_jusante',  StringType(), True),
    StructField('pessoas_possivelmente_afetadas_rompimento',  StringType(), True),
    StructField('impacto_ambiental',  StringType(), True),
    StructField('impacto_socio_economico',  StringType(), True),
    StructField('data_da_finalizacao_da_dce',  StringType(), True),
    StructField('motivo_de_envio',  StringType(), True),
    StructField('rt_declaracao',  StringType(), True),
    StructField('rt_empreendimento',  StringType(), True),
])


# Definindo Schema para Dataset beneficiada
schema_beneficiada = StructType([
    StructField('ano_base',  StringType(), True),
    StructField('uf',  StringType(), True),
    StructField('classe_substancia',  StringType(), True),
    StructField('substancia_mineral',  StringType(), True),
    StructField('quantidade_producao',  StringType(), True),
    StructField('unidade_de_medida_producao',  StringType(), True),
    StructField('quantidade_contido',  StringType(), True),
    StructField('unidade_de_medida_contido',  StringType(), True),
    StructField('indicacao_contido',  StringType(), True),
    StructField('quantidade_venda',  StringType(), True),
    StructField('unidade_de_medida_venda',  StringType(), True),
    StructField('valor_venda_rs',  StringType(), True),
    StructField('consumo_utilizacao_usina',  StringType(), True),
    StructField('unidade_de_medida_consumo',  StringType(), True),
    StructField('valor_consumo',  StringType(), True),
    StructField('transferencia_para_transformacao',  StringType(), True),
    StructField('unidade_medida_transferencia_para_transformacao',  StringType(), True),
    StructField('valor_transferencia_para_transformacao',  StringType(), True),
])


# Definindo Schema para Dataset distribuicao
schema_distribuicao = StructType([
    StructField('numero_distribuicao',  StringType(), True),
    StructField('ano',  StringType(), True),
    StructField('mes',  StringType(), True),
    StructField('ente',  StringType(), True),
    StructField('sigla_estado',  StringType(), True),
    StructField('nome_ente',  StringType(), True),
    StructField('tipo_distribuicao',  StringType(), True),
    StructField('substancia',  StringType(), True),
    StructField('tipo_afetamento',  StringType(), True),
    StructField('valor',  StringType(), True)
])


# Definindo Schema para Dataset municipio
schema_municipio = StructType([
    StructField('uf',  StringType(), True),
    StructField('cod_uf',  StringType(), True),
    StructField('cod_munic',  StringType(), True),
    StructField('nome_do_municipio',  StringType(), True),
    StructField('populacao_estimada',  StringType(), True)
])


# Definindo Schema para Dataset pib
schema_pib = StructType([
    StructField('ano',  StringType(), True),
    StructField('id_municipio',  StringType(), True),
    StructField('pib',  StringType(), True),
    StructField('impostos_liquidos', StringType(), True),
    StructField('va',  StringType(), True),
    StructField('va_agropecuaria',  StringType(), True),
    StructField('va_industria',  StringType(), True),
    StructField('va_servicos',  StringType(), True),
    StructField('va_adespss',  StringType(), True),
])


# Lendo arquivos CSVs
try:
    print("\n\tLendo CSVs...")
    inicio = time.time()
    df_arrecadacao = CSV(";", "ISO-8859-1", "gs://0_csvs_diego/arrecadacao.csv", schema_arrecadacao).ler_csv(spark)
    df_autuacao = CSV(";", "ISO-8859-1", "gs://0_csvs_diego/autuacao.csv", schema_autuacao).ler_csv(spark)
    df_barragens = CSV(";", "ISO-8859-1", "gs://0_csvs_diego/barragens.csv", schema_barragens).ler_csv(spark)
    df_beneficiada = CSV(",", "ISO-8859-1", "gs://0_csvs_diego/beneficiada.csv", schema_beneficiada).ler_csv(spark)
    df_distribuicao = CSV(";", "ISO-8859-1", "gs://0_csvs_diego/distribuicao.csv", schema_distribuicao).ler_csv(spark)
    df_municipio = CSV(";", "ISO-8859-1", "gs://0_csvs_diego/municipio.csv", schema_municipio).ler_csv(spark)
    df_pib = CSV(",", "ISO-8859-1", "gs://0_csvs_diego/pib.csv", schema_pib).ler_csv(spark)
except Exception as e:
    print(str(e))
    gravar_log(f"job_1.py - Falha ao ler CSVs")
else:
    fim = time.time()
    print(f"\t\t...processo concluido em {(float((fim - inicio)%60))} seg.\n\n")
    gravar_log(f"job_1.py - CSVs lidos em {(float((fim - inicio)%60))}")


# Gravando em Parquets
try:
    print("\n\tGravando parquets...")
    inicio = time.time()
    parquet = Parquet("gs://1_parquets_diego/arrecadacao").grava_parquet(df_arrecadacao)
    parquet = Parquet("gs://1_parquets_diego/autuacao").grava_parquet(df_autuacao)
    parquet = Parquet("gs://1_parquets_diego/barragens").grava_parquet(df_barragens)
    parquet = Parquet("gs://1_parquets_diego/beneficiada").grava_parquet(df_beneficiada)
    parquet = Parquet("gs://1_parquets_diego/distribuicao").grava_parquet(df_distribuicao)
    parquet = Parquet("gs://1_parquets_diego/municipio").grava_parquet(df_municipio)
    parquet = Parquet("gs://1_parquets_diego/pib").grava_parquet(df_pib)
except Exception as e:
    print(str(e))
    gravar_log(f"job_1.py - Falha ao salvar Parquets")
else:
    fim = time.time()
    print(f"\t\t...processo concluido em {(float((fim - inicio)%60))} seg.\n\n")
    gravar_log(f"job_1.py - Parquets salvos em {(float((fim - inicio)%60))}")