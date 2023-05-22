from pathlib import Path
from typing import Any, List, Tuple, Union, Set
from mysql.connector import Error

import mysql.connector
import pandas as pd
import planilhas

# Conexão com o banco de dados MySQL


def conectar():
    try:
        con = mysql.connector.connect(
            host='localhost', database='etl', user='root', password='my-secret-pw')
        if con.is_connected():
            db_info = con.get_server_info()
            print("Conectado ao servidor MySQL versão ", db_info)
            cursor = con.cursor()
            cursor.execute("select database();")
            linha = cursor.fetchone()
            print("Conectado ao banco de dados ", linha)
    except Error as erro:
        print("Erro de Conexão")
    return con


def export_xlsx_to_mysql(xlsx_files: List[Path], sheet_names: Set[str] = None):
    """Exporta planilhas Excel para Parquet.

    O nome final do arquivo será `{path}/{file_name}/{safe_sheet_name}/dados.parquet`.

    Args:
        xlsx_files (List[Path]): Lista de arquivos XLSX a processar.
        path (Union[str, Path]): Diretório base onde salvar os CSV.
        sheet_names (Set[str], optional): Lista de planilhas (abas) a considerar. Defaults to None.
    """
    if sheet_names is None:
        sheet_names = (None,)
    for xlsx_file in xlsx_files:
        file_name = xlsx_file.stem.lower()
        # file_name = file_name[9:]
        for sheet_name in sheet_names:
            df, final_sheet_name = transform_xlsx_dataframe(xlsx_file,
                                                            data_startrow=13,
                                                            header_startrow=12, header_nrows=0, header_rows_ffill=0,
                                                            sheet_name=sheet_name,)
            # print(df)
            print(file_name)
            # df = df.iloc[497]
            df = df.drop(df.index[497:])
            df.to_excel('C:/projeto_git/etl-python/arquivo.xlsx', index=False)
            dados_excel = pd.read_excel(
                'C:/projeto_git/etl-python/arquivo.xlsx')
            dados_excel = dados_excel.fillna(0)
            print(dados_excel)
            # Abrir a conexão com o banco de dados
            conn = conectar()
            nome_tabela = 'dados_ssp'
            cursor = conn.cursor()
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {nome_tabela} (Municipios VARCHAR(255), Homicidio_Doloso FLOAT, Total_de_vitimas_de_Homicidio_Doloso FLOAT, Latrocinio FLOAT, Furtos FLOAT, Abigeato FLOAT, Furto_de_Veiculo FLOAT, Roubos FLOAT, Roubo_de_Veiculo FLOAT, Estelionato FLOAT, Delitos_Relacionados_a_Armas_e_Municoes FLOAT, Entorpecentes_Posse FLOAT, Entorpecentes_Trafico FLOAT, Vitimas_de_Latrocinio FLOAT, Vitimas_de_Lesao_Corp_Seg_Morte FLOAT, Total_de_Vitimas_de_CVLI FLOAT)")

            # Inserir dados na tabela
            # Colunas do arquivo Excel
            for index, row in dados_excel.iterrows():
                valores = (str(row['Municipios']), float(row['Homicidio_Doloso']), float(row['Total_de_vitimas_de_Homicidio_Doloso']), float(row['Latrocinio']), float(row['Furtos']), float(row['Abigeato']), float(row['Furto_de_Veiculo']), float(row['Roubos']), float(row['Roubo_de_Veiculo']), float(row['Estelionato']), float(
                    row['Delitos_Relacionados_a_Armas_e_Municoes']), float(row['Entorpecentes_Posse']), float(row['Entorpecentes_Trafico']), float(row['Vitimas_de_Latrocinio']), float(row['Vitimas_de_Lesao_Corp_Seg_Morte']), float(row['Total_de_Vitimas_de_CVLI']))
                cursor.execute(
                    f"INSERT INTO {nome_tabela} (Municipios, Homicidio_Doloso, Total_de_vitimas_de_Homicidio_Doloso, Latrocinio, Furtos, Abigeato, Furto_de_Veiculo, Roubos, Roubo_de_Veiculo, Estelionato, Delitos_Relacionados_a_Armas_e_Municoes, Entorpecentes_Posse, Entorpecentes_Trafico, Vitimas_de_Latrocinio, Vitimas_de_Lesao_Corp_Seg_Morte, Total_de_Vitimas_de_CVLI) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", valores)

            # Confirmar as alterações
            conn.commit()

            # Fechar a conexão com o banco de dados
            conn.close()

            print('FIM')


def transform_xlsx_dataframe(
        xlsx_file: Path, data_startrow=9, data_startcolumn=None,
        header_startrow=6, header_nrows=3, header_rows_ffill=0,
        sheet_name: str = None, strftime='%d/%m/%Y, %H:%M:%S',) -> Tuple[pd.DataFrame, str]:
    """Transforma uma planiha dentro de um "workbook" Excel em um DataFrame pandas com colunas normalizadas.

    Args:
        xlsx_file (Path): Caminho do arquivo Excel.
        data_startrow (int, optional): Linha em que começam os dados. Defaults to 9.
        header_startrow (int, optional): Linha em que começa o cabeçalho. Defaults to 6.
        header_nrows (int, optional): Tamanho do cabeçalho em linhas. Defaults to 3.
        header_rows_ffill (int, optional): Quantidade de linhas do cabeçalho que devem ser replicadas à direita caso a próxima coluna seja vazia. Defaults to 0.
        sheet_name (str, optional): Nome ou índice numérico da planilha (aba) a ler. Defaults to None.
        strftime (str, optional): STRFTIME string to format if cell is datetime. Defaults to '%d/%m/%Y, %H:%M:%S'.

    Returns:
        Tuple[pd.DataFrame, str]: Retorna o dataframe e o nome da planilha (caso se tenha informado o índice numérico dela).
    """
    print(f'Processando {xlsx_file}.')
    colunas, final_sheet_name = planilhas.get_xlsx_column_headers(
        xlsx_file, sheet_name=sheet_name,
        min_col=data_startcolumn,
        min_row=header_startrow, max_row=header_startrow+header_nrows,
        rows_ffill=header_rows_ffill, strftime=strftime,)

    linhas = planilhas.get_xlsx_values(
        xlsx_file, min_col=data_startcolumn, min_row=data_startrow, sheet_name=sheet_name)

    retval = pd.DataFrame(linhas, dtype=str, columns=colunas)
    print(f'Processado {xlsx_file}.')
    return retval, final_sheet_name


if __name__ == '__main__':
    path = []
    path.append(Path(
        'C:/Users/Dante/Desktop/13133823-indicadores-criminais-geral-e-por-municipio-2022.xlsx'))

    export_xlsx_to_mysql(path, sheet_names={'2022', })
