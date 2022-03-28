#Ler e baixar os arquivos

from zipfile import ZipFile
import decimal as D
import os
import requests
import csv
import pandas as pd
from pyunpack import Archive



# Baixar os arquivos pela url do site: https://portal.inmet.gov.br/dadoshistoricos


def baixar_arquivos(url, endereco):
    # faz requisição de resposta ao servidor
    resposta = requests.get(url)
    # Ver se está OK a resposta, prosseguir e exibr onde o donwload foi finalizado
    if resposta.status_code == requests.codes.OK:
        with open(endereco, 'wb') as arq_2020:
            arq_2020.write(resposta.content)
        print("download finalizado e salvo em: {}".format(endereco))
    else:
        resposta.raise_for_status()


if __name__ == "__main__":
    # Baixar Tudo colocar BASE_URL = 'https://portal.inmet.gov.br/uploads/dadoshistoricos/{}.zip'
    BASE_URL = 'https://portal.inmet.gov.br/uploads/dadoshistoricos/{}.zip'
    SAIDA_DIR = 'download'

    for i in range(2020,2023):

        # Modulo "os" fornece uma maneira de usar funcionalidades que são dependentes de sistema operacional
        nomes_arq = os.path.join(SAIDA_DIR, 'inmet_{}.zip'.format(i))
        baixar_arquivos(BASE_URL.format(i), nomes_arq)


# Extrair os arquivos na pasta inmet_ano (Em vários formatos de compressão podemos usar o pacote pyunpack)
Arq_zip2020 = ZipFile(r'C:\Users\user\Desktop\Pos_BIG_DATA\download\inmet_2020\inmet_2020.zip', 'r')
Arq_zip2020.extractall(r'C:\Users\user\Desktop\Pos_BIG_DATA\download\inmet_2020')
Arq_zip2020.close()

Arq_zip2021 = ZipFile(r'C:\Users\user\Desktop\Pos_BIG_DATA\download\inmet_2021\inmet_2021.zip', 'r')
Arq_zip2021.extractall(r'C:\Users\user\Desktop\Pos_BIG_DATA\download\inmet_2021')
Arq_zip2021.close()

Arq_zip2022 = ZipFile(r'C:\Users\user\Desktop\Pos_BIG_DATA\download\inmet_2022\inmet_2022.zip', 'r')
Arq_zip2022.extractall(r'C:\Users\user\Desktop\Pos_BIG_DATA\download\inmet_2022')
Arq_zip2022.close()

# Não conseguimos abrir de primeira porque está truncado
# Para resolver isso basta colocar r antes da sua string normal, ela converte a string normal em string bruta.


# Converter csv para arquivo parquet

file_path = r'C:\Users\user\Desktop\Pos_BIG_DATA\download\inmet_{ano}.zip'
destination_path = r'C:\Users\user\Desktop\Pos_BIG_DATA\download\inmet_{ano}.parquet.gz'



# imprimirá o diretório de trabalho atual junto com todos os arquivos nele
#cwd = os.getcwd()  # Get the current working directory (cwd)
#files = os.listdir(cwd)  # Get all the files in that directory
#print("Files in %r: %s" % (cwd, files))

def tratamento_df_proprties(df_properties):
    df_prop = df_properties.T
    df_prop.columns = df_prop.iloc[0]
    df_prop = df_prop.iloc[1].to_frame().T
    df_prop.reset_index(drop = True)[list(df_prop.columns)[:-1]]
    return df_prop[list(df_prop.columns)[:-1]]

def redefinicao_de_tipo_df_propriedades(df_prop):
    for i in list(df_prop.columns)[:4]:
        df_prop = df_prop.assign(
            **{
                i: lambda x: x[i].astype('category')
            }
        )
    for i in list(df_prop.columns)[4:6]:
        df_prop = df_prop.assign(
            **{
                i: lambda x: x[i].apply(
                    lambda y: float(str(y).replace(',', '.'))
                )
            }
        )
    for i in list(df_prop.columns)[6:7]:
        df_prop = df_prop.assign(
            **{
                i: lambda x: x[i].apply(
                    lambda y: D.Decimal(str(y).replace(',', '.'))
                )
            }
        )
    return df_prop

def redefinicao_de_tipo_df_dados(df_dados):
    decimal_convert = list(df_dados.columns[2:19])

    for field in decimal_convert:
        df_dados = df_dados.assign(
            **{
                field: lambda x: x[field].apply(
                    lambda y: D.Decimal(str(y).replace(',', '.'))
                )
            }
        )

    df_dados = df_dados.assign(
        **{
            'Data': lambda x: pd.to_datetime(x[list(df_dados)[0]])
        }
    )

    return df_dados

from datetime import datetime
def estacao(dt):
    
    dia = dt.day
    mes = dt.month
    ano = dt.year
    
    data = datetime(ano, mes, dia)
    if mes <= 2 or mes == 3 and dia < 20:
        ano -= 1
    estacoes = [
        {'estacao': 'outono', 'inicio': datetime(ano, 3, 20), 'fim': datetime(ano, 6, 20)}
        , {'estacao': 'inverno', 'inicio': datetime(ano, 6, 21), 'fim': datetime(ano, 9, 21)}
        , {'estacao': 'primavera', 'inicio': datetime(ano, 9, 22), 'fim': datetime(ano, 12, 20)}
        , {'estacao': 'verão', 'inicio': datetime(ano, 12, 21), 'fim': datetime(ano+1, 3, 19)}
    ]
    est = None
    for i in estacoes:
        if data >= i['inicio'] and data <= i['fim']:
            est = i
    d = data
    i = est['inicio']
    f = est['fim']
    
    if est['estacao'] in ['inverno', 'verão']:
        if abs(i - d).days > int(abs(i - f).days/2):
            dif = abs(i - f).days+1 - abs(i - d).days
        else:
            dif = abs(i - d).days + 1
    else:
        dif = abs(i - d).days + 1
        
    #return {'estacao': est['estacao'], 'dias': dif, 'ano':data.year , 'mes': data.month}
    return pd.Series([est['estacao'], dif, data.year, data.month], index=['estacao_ano', 'dias', 'ano', 'mes'])

def carga_zip(file_path, destination_path):
    for f in ZipFile(file_path).namelist():
        if f[-4:].lower() == '.csv':
            # dados da estação meteriologica
            df_properties = pd.read_csv(ZipFile(file_path).open(f), nrows=8, sep=';', encoding='iso-8859-1', header=None)

            # dados coletados
            df_dados = pd.read_csv(
                ZipFile(file_path).open(f)
                , skiprows=8
                , sep=';'
                , encoding='iso-8859-1'
            )
            df_prop = redefinicao_de_tipo_df_propriedades(tratamento_df_proprties(df_properties))
            df_dados = redefinicao_de_tipo_df_dados(df_dados)
    
    df_final = pd.concat([
        df_dados[list(df_dados.columns)[:-1]] # dados originais
        , df_dados['Data'].apply(estacao) # dados sobre a data
        , pd.concat([df_prop]*df_dados.shape[0], ignore_index=True) # dados sobre a estação meteriologica
    ], axis=1)
    
    df_final.to_parquet(destination_path, compression='gzip')

for ano in range(2020, 2023):
    carga_zip(file_path.format(ano=ano), destination_path.format(ano=ano))

#pd.read_parquet(r'C:\Users\user\Desktop\Pos_BIG_DATA\download\inmet_{ano}.parquet.gz').head().T

# Extrair os arquivos na pasta parquet_ano (Em vários formatos de compressão podemos usar o pacote pyunpack)
#INPUT_DIR_2020 = r'C:\Users\user\Desktop\Pos_BIG_DATA\download\parquet_2020\inmet_2020.parquet.gz' 
            
#OUTPUT_DIR_2020 = r'C:\Users\user\Desktop\Pos_BIG_DATA\download\parquet_2020'

Archive(r'C:\Users\user\Desktop\Pos_BIG_DATA\download\parquet_2020\inmet_2020.parquet.gz').extractall(r'C:\Users\user\Desktop\Pos_BIG_DATA\download\parquet_2020')

