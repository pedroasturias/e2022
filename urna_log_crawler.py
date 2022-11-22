import os, zipfile, py7zr
import pickle
import re, codecs
from pyUFbr.baseuf import ufbr
import urllib.request
import sys
import pandas as pd

COMPRESSED_LOG_DIRECTORY = './tmp'
DATA_DIRECTORY = './data'
MODELO_DE_URNA_PICKLE = os.path.join(DATA_DIRECTORY, 'modelo_de_urna.pickle')

# Arquives devem ser baixados na URL abaixo e colocados no diretorio COMPRESSED_LOG_DIRECTORY
    # https://dadosabertos.tse.jus.br/dataset/resultados-2022-arquivos-transmitidos-para-totalizacao
FILE_PATTERN_ARQUIVO_TRANSM = re.compile('bu_imgbu_logjez_rdv_vscmr_2022_(\d)t_([A-Z]{2}).zip')
ZIPPED_LOG_FILENAME_PATTERN = re.compile('.*(\d{5})(\d{4})(\d{4}).logjez')
#
ID_SECAO = 'ID_SECAO'
CD_MUNICIPIO = 'CD_MUNICIPIO'
NR_ZONA = 'NR_ZONA'
NR_SECAO = 'NR_SECAO'
MODELO_URNA_TEMPLATE = 'MODELO_URNA_%sT'
SE_UE2020 = 'SE_UE2020'


MODELO_URNA_PATTERN = re.compile('Modelo de Urna: (\w+)')
CD_MUNICIPIO_PATTERN = re.compile('Município: (\w+)')
NR_ZONA_PATTERN = re.compile('Zona Eleitoral: (\w+)')
NR_SECAO_PATTERN = re.compile('ão Eleitoral: (\w+)')

LISTA_TURNO_UFS = [
    (1, 'AC'),
    (1, 'AL'),
    (1, 'AM'),
    (1, 'AP'),
    (1, 'BA'),
    (1, 'CE'),
    (1, 'DF'),
    (1, 'ES'),
    (1, 'GO'),
    (1, 'MA'),
    (1, 'MG'),
    (1, 'MS'),
    (1, 'MT'),
    (1, 'PA'),
    (1, 'PB'),
    (1, 'PE'),
    (1, 'PI'),
    (1, 'PR'),
    (1, 'RJ'),
    (1, 'RN'),
    (1, 'RO'),
    (1, 'RR'),
    (1, 'RS'),
    (1, 'SC'),
    (1, 'SE'),
    (1, 'SP'),
    (1, 'TO'),
    (2, 'AC'),
    (2, 'AL'),
    (2, 'AM'),
    (2, 'AP'),
    (2, 'BA'),
    (2, 'CE'),
    (2, 'DF'),
    (2, 'ES'),
    (2, 'GO'),
    (2, 'MA'),
    (2, 'MG'),
    (2, 'MS'),
    (2, 'MT'),
    (2, 'PA'),
    (2, 'PB'),
    (2, 'PE'),
    (2, 'PI'),
    (2, 'PR'),
    (2, 'RJ'),
    (2, 'RN'),
    (2, 'RO'),
    (2, 'RR'),
    (2, 'RS'),
    (2, 'SC'),
    (2, 'SE'),
    (2, 'SP'),
    (2, 'TO')
]


def GetModeloUrnaFromLogFile(file_path):

    with codecs.open(file_path, encoding='iso-8859-15') as f:
        content = f.read()
        content = content.encode('UTF-8').decode()

        return MODELO_URNA_PATTERN.search(content).groups()[0]

def LoadDataDict():
    with open(MODELO_DE_URNA_PICKLE, 'rb') as f:
        return pickle.load(f)

def DumpDataDict(data):
    with open(MODELO_DE_URNA_PICKLE, 'wb') as f:
        return pickle.dump(data, f)


def LoadModeloUrnasDataFrame():
    with open(MODELO_DE_URNA_PICKLE, 'rb') as file:
        data = pickle.load(file)

    df = pd.DataFrame(
        data.values(),
        index=data.keys(),
    )
    df.index.names = [ID_SECAO]

    df['MODELO_URNA'] = df.MODELO_URNA_1T.combine_first(df.MODELO_URNA_2T)
    df['SE_UE2020'] = [i == 'UE2020' for i in df.MODELO_URNA]
    df = df.drop(['ID_SECAO', 'MODELO_URNA_1T', 'MODELO_URNA_2T', 'CD_MUNICIPIO', 'NR_ZONA', 'NR_SECAO'], axis=1)

    return df

def Main():
    plain_log_file_path = os.path.join(COMPRESSED_LOG_DIRECTORY, 'logd.dat')

    try:
        data = LoadDataDict()
    except FileNotFoundError:
        data = {}


    for turno, uf in LISTA_TURNO_UFS:

        url = 'https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes2022/arqurnatot/bu_imgbu_logjez_rdv_vscmr_2022_%dt_%s.zip' %(turno, uf)
        filename = os.path.join(COMPRESSED_LOG_DIRECTORY, url.split('/')[-1])

        print('Downloading ', url)
        urllib.request.urlretrieve(url, filename)

        print('Processando %s' % filename)

        zip_file = zipfile.ZipFile(filename)

        for zip_internal_file in zip_file.filelist:
            if zip_internal_file.filename.endswith('logjez'):
                zipped_log_file = zip_file.extract(zip_internal_file, path=COMPRESSED_LOG_DIRECTORY)
                zipped_log_file_ref = py7zr.SevenZipFile(zipped_log_file, mode='r')
                all_files = zipped_log_file_ref.getnames()
                zipped_log_file_ref.extractall(COMPRESSED_LOG_DIRECTORY)
                zipped_log_file_ref.close()

                cd_municipio, nr_zona, nr_secao = \
                    [int(i) for i in ZIPPED_LOG_FILENAME_PATTERN.match(zipped_log_file).groups()]

                os.remove(zipped_log_file)

                section_id = '%s_%s_%s_%s' % (
                    uf,
                    str(cd_municipio),
                    str(nr_zona),
                    str(nr_secao)
                )

                print('\t Processando %s' %section_id)

                if section_id in data:
                    section_data = data[section_id]
                    if MODELO_URNA_TEMPLATE % turno in section_data:
                        continue  # Tal dado já foi lido
                else:
                    section_data = {
                        ID_SECAO: section_id,
                        CD_MUNICIPIO: cd_municipio,
                        NR_ZONA: nr_zona,
                        NR_SECAO: nr_secao
                    }

                try:
                    log_modelo_urna = GetModeloUrnaFromLogFile(plain_log_file_path)
                except:
                    log_modelo_urna = 'None'

                if log_modelo_urna == 'UE2020':
                    section_data[SE_UE2020] = True
                elif log_modelo_urna in ['UE2010', 'UE2015', 'UE2009', 'UE2011', 'UE2013']:
                    section_data[SE_UE2020] = False
                else:
                    print('Erro: parsing modelo urna', section_data)

                section_data[MODELO_URNA_TEMPLATE % turno] = log_modelo_urna

                data[section_id] = section_data

                for log_file in all_files:
                    os.remove(os.path.join(COMPRESSED_LOG_DIRECTORY, log_file))

        zip_file.close()
        os.remove(filename)

        DumpDataDict(data)

if __name__ == '__main__':
    Main()