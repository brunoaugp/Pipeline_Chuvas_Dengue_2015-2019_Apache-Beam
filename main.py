import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)


colunas_dengue = [
                'id',
                'data_iniSE',
                'casos',
                'ibge_code',
                'cidade',
                'uf',
                'cep',
                'latitude',
                'longitude'
]

# ----------------------------------------------------------------------------

# funcao para receber cada linha do arquivo e salvar a string como lista (lembrando que o BEam faz a leitura do arquivo linha a linha)
def texto_para_lista(line,delimiter='|'):
    return line.split(delimiter)

# funcao para transformar 2 listas em dicionário 
def lista_para_dicionario(line, colunas = colunas_dengue):
    return dict(zip(colunas,line))

# funcao para receber um dicionario e retornar o mesmo dicionario com um novo campo de data apenas com ANO-MES 
def trata_data(line,col = "data_iniSE"):
    line["ano_mes"] = '-'.join(line[col].split('-')[:2])
    return line

# funcao para receber um dicionario e retornar uma tupla com (UF, {})
def chave_uf(line):
    return (line['uf'],line)

# funcao que recebe uma tupla com chave e lista com dicionarios (UF, [{},{}]): vindo do groupby do pipeline e retorna uma tupla ('RS-2014-12',8)
def casos_dengue(line):
    uf, registros = line #descompactando tupla de entrada
    for registro in registros:
        if bool(re.search(r'\d',registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}",float(registro['casos']))
        else: 
            yield (f"{uf}-{registro['ano_mes']}",0.0)


# funcao que recebe lista e transforma em tupla do modo ('UF-ANO-MES', chuva em mm)
def chave_uf_ano_mes(line):
    data,mm,uf = line
    ano_mes = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{ano_mes}'
    if float(mm) < 0.0 or mm == None:
        mm = 0.0
    else:
        mm = float(mm)
    return (chave,mm)

# funcao que recebe uma tupla ('chave',mm) e retorna a mesma tupla com mm arredondado
def arredonda_valores(line):
    chave,mm = line
    return (chave,round(mm,2))


# funcao que recebe tupla (key,{chave:[],chave:[]}) e retorna a mesma tupla, retirando as linha que tiverem campos vazios
def filtra_campos_vazios(line):
    chave,dados = line
    if len(dados['chuvas'])== 0 or len(dados['dengue'])==0:
        return False
    else:
        return True

# funcao que recebe uma tupla (key, {'chave':[valor],'chave':[valor]})
def descompactar_elemento(line):
    chave,dados=line
    uf,ano,mes = chave.split('-')
    chuva = float((dados['chuvas'][0]))
    dengue = float((dados['dengue'][0]))

    return(uf,int(ano),int(mes),chuva,dengue)

# recebe um tupla e retorna um string delimitada
def preparando_csv(line,delimiter =';'):
    uf,ano,mes,chuva,dengue = line

    mes = str(mes).zfill(2)
    return f"{uf}{delimiter}{ano}{delimiter}{mes}{delimiter}{chuva}{delimiter}{dengue}"


# ----------------------------------------------------------------------------

# PIPELINES
dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> ReadFromText('./alura-apachebeam-basedados/casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_para_lista)
    | "De lista para dicionario" >> beam.Map(lista_para_dicionario)
    | "Criando campo ANO-MES" >> beam.Map(trata_data)
    | "Criando chave pelo estado" >> beam.Map(chave_uf)
    | "Agupando dados pelo estado (UF,[{},{},{}])" >> beam.GroupByKey()
    | "Descompactando casos de dengue" >> beam.FlatMap(casos_dengue)
    | "Soma dos casos pela chave" >> beam.CombinePerKey(sum)
    #| "Mostrando resultados" >> beam.Map(print)
)

chuvas = (
    pipeline
    | "Leitura do dataset de chuvas" >> ReadFromText('./alura-apachebeam-basedados/chuvas.csv', skip_header_lines=1)
    | "De texto para lista - chuvas" >> beam.Map(texto_para_lista, delimiter = ',')
    | "Criando chave UF-ANO-MES - chuvas" >> beam.Map(chave_uf_ano_mes)
    | "Soma dos mm de chuva agrupadoss pela chave - chuvas" >> beam.CombinePerKey(sum)
    | "Arredondamento dos valores acumulados de chuva" >> beam.Map(arredonda_valores)
    #| "Mostrando resultados - chuvas" >> beam.Map(print)
)

# juntando os dois resultados
resultado = (
    #(dengue,chuvas)
    #| "Juntando dados das 2 pcollection, empilhando" >> beam.Flatten()
    #| "Agupando dados por chave" >> beam.GroupByKey()
    ({'chuvas':chuvas,'dengue':dengue})
    | "Mesclando pcollections, fazendo empilhamento e agrupamento simultâneo" >> beam.CoGroupByKey()
    | "Filtrando (removendo) dados vazio" >> beam.Filter(filtra_campos_vazios)
    | "Descompactando elementos da tupla" >> beam.Map(descompactar_elemento)
    | "Preparando csv (passando tupla para string)" >> beam.Map(preparando_csv)
    #| "Mostrando resultados juntos" >> beam.Map(print)
)

header="UF;ANO;MES;CHUVA;CASOS_DENGUE"

resultado | "Criando arquivo csv" >> WriteToText('Resultados/resultado', file_name_suffix='.csv', header=header)

pipeline.run()