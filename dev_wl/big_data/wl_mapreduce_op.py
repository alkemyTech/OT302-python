"""
    OT302-110
    Optimizar la ejecución actual del MapReduce para el grupo de datos B
"""
import xml.etree.ElementTree as ET
import collections
import csv
from pathlib import Path
import time
from multiprocessing import Pool

FILE_PATH = "dev_wl/meta_stackOverflow/posts.xml"


def divide_chunks(lista, numero):
    """en esta funcion se van generando los chunks

    Args:
        lista (list): listado con las listas de tags extraidos del archivo xml
        numero (int): numero de chunks a generar

    Yields:
        list: va retornando los chunks
    """

    for i in range(0, len(lista), numero):
        yield lista[i : i + numero]


def chunkify(xmlfile, number_of_chunks=16):
    """La informacion del archivo xmlfile que comple con la condicion de
        ser una pregunta postType = 1, con respuesta no aceptada "AcceptedAnswerId" == None,
        se dividen en 16 chunks.

    Args:
        xmlfile (str): nombre del archivo a procesar
        number_of_chunks (int, optional): cantidad de chunks en los que se divide la informacion. Defaults to 16.

    Returns:
       list: listado de chunks, es un listado de chunks, en donde cada chunk es a su vez un listado de los tags extraidos de cada pregunta.
    """

    # create element tree object
    tree = ET.parse(xmlfile)

    # get root element
    root = tree.getroot()

    list_tags = []
    # looping till length l
    for row in root.iter("row"):
        dir_row = row.attrib
        # The record is selected and it is processed, if it is a question type
        if dir_row.get("PostTypeId") == "1":

            # those without accepted answers are mapped
            if dir_row.get("AcceptedAnswerId") == None:
                tags_names = (
                    dir_row.get("Tags")
                    .replace("<", "")
                    .replace(">", " ")
                    .lstrip()
                    .split()
                )
                list_tags.append(tags_names)

    list_chunks = list(divide_chunks(list_tags, number_of_chunks))
    return list_chunks


def mapper(chunk):
    """se mapea pasando de una lista de chunks que contienen una lista de listas de tags a una lista con todas las listas de tags.

    Args:
        chunk (list): es un listado con listados de tags

    Returns:
        list: agrupa todos los listados de tags en un solo listado
    """

    listado = []
    for listado_listas_tags in chunk:
        for tags in listado_listas_tags:
            listado.append(tags)
    return listado


def shuffler(mapper):
    """se realiza el shuffle, se pasa de una lista que contien una lista de listas da tags a una lista de con todos los tags.

    Args:
        mapper (list): es un listado con listas que contienen tags

    Returns:
        list: se agrupan todos los tags en un solo listado
    """

    lista = []
    for lista_tags in mapper:
        for tag in lista_tags:
            lista.append(tag)
    return lista


# Funcio que reduce y resuelve segun las consignas
def reduce(lista):
    """se realiza el reducer, en donde cuenta cuantas veces se repite cada tags y se retorna los 10 con mas repeticiones.

    Args:
        lista (list): listado con todas las ocurrencias de los tags

    Returns:
        list: listado de tuplas (tag, cantidad) de los Top 10 tags con mas ocurrencias.
    """

    # Con la funcion y metodo "collections.Couter", agrupo por tags y cuento sus ocurrencias, se toman los 10 con mas ocurrencias
    # el resultado se agrega a la lista de resultados
    return collections.Counter(lista).most_common(10)


def savetoCSV(data, filename, fields):
    """Esta funcion graba los archivos de salida.

    Args:
        data (list): los datos reducidos a grabar en el reporte
        filename (string):
        fields (list): es una lista con los nombres de las columnas del reporte

    """

    # los datos vienen como una lista y graba el CSV
    with open(f"dev_wl/big_data/{filename}", "w") as csvfile:
        csv_out = csv.writer(csvfile)
        # writing headers (field names)
        csv_out.writerow(fields)
        # writing data rows
        for row in data:
            csv_out.writerow(row)


if __name__ == "__main__":
    """Se ejecunta los diferentes pasos para realizar el Map Reduce.
    Paso 1: se hace una particion de la informacion en 16 chunks.
    Paso 2: se mapea pasando de una lista de chunks que contienen una lista de listas de tags a una lista con todas las listas de tags.
    Paso 3: se realiza el shuffle, se pasa de una lista que contien una lista de listas da tags a una lista de con todos los tags.
    Paso 4: se realiza el reducer, en donde cuenta cuantas veces se repite cada tags y se retorna los 10 con mas repeticiones.
    Paso 5: se graba un archvio CSV con el reporte
    """

    start = time.time()
    # set path root
    root = Path.cwd()

    # mombre del archivo a mapear y reducir
    file = Path(root / FILE_PATH)

    # divido la informacion a extraer de los tags en 16 chunks
    data_chunks = chunkify(file, number_of_chunks=16)

    # se paraleliza el proceso de mapeado, en Pool se pone processes en None, entonces se utiliza el número retornado por os.cpu_count().
    pool = Pool(processes=None)
    # se procesa cada chunk y se retorna un listado con listados de listas de tags
    mapper = pool.map(mapper, data_chunks, chunksize=16)

    shuffled = shuffler(mapper)

    list_result = reduce(shuffled)

    print(type(list_result))
    print(list_result)

    end = time.time()

    print(end - start)

    # Graba los resultados en un CSV
    savetoCSV(list_result, "OP_top10tags.csv", ["TAG", "COUNT"])
