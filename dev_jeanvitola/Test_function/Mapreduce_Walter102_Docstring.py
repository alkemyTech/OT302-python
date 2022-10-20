from wl_map_reduce import *

import pytest


data= "posts.xml"

"""
La función mapper(), realiza el mapeo de los datos y como resultados retorna
una lista para cada consigna a resolver.

input : La entrada de la función sera un archivo de formato .xml, donde
se creará un objecto Tree utilizando la libreria xml.etree

output: Lo que se espera de la función es que identifique diversos tag y
busque de forma iterativa los diversos caracteres de interes que se iran guardando en unas listas
vacias, luego se creará una lista de lista, el resultado esperado es un lista de lista


"""
def test_mapper():
     assert isinstance(mapper(data),list)



"""  
La función shuffle_sort acomoda y ordena las listas que reciben como parámetro los datos mapeados,


input: la entrada que se espera es una lista de listas con los parámetros mapeados en la anterior
función.

output : Se espera que el resultado sea una lista que contenga un Dataframe, este objeto 
contiene el merge de los caracteres de interes.


"""


def test_shuffle_sort():
    lista = mapper(data)
    assert isinstance(shuffle_sort(lista), list)



"""  
La fución reduce, reduce la cual El método reduce() ejecuta una función reductora sobre cada elemento de un array,
 devolviendo como resultado un único valor

input : La entrada será una lista con 3 listas, en este caso sería la lista que retorna el mapper

Output : lo que se espera es una lista que contiene dos listas y una seria de pandas
"""


def test_reduce():
    lista = mapper(data)
    lista2 = shuffle_sort(lista)
    assert isinstance(reduce(lista2), list)
    
   
"""
La función SavetoCSV fuarda la salida de la función

   input:
        data (list | pandas.Serie): los datos reducidos a grabar en el reporte
        filename (string):
        fields (list): es una lista con los nombres de las columnas del reporte
        type_data (int): si es 1, los datos son una lista, si es 2, los datos son una serie de pandas

    output : La salida será un archivo .CSV con las casacteristicas menciondas en el input
    

"""


def test_savetoCSV():
    lista = mapper(data)
    lista2 = shuffle_sort(lista)
    lista3 = reduce(lista2)
    data2 = lista3
    filename = "top10tags.csv"
    fields = ["TAG", "COUNT"]
    type_data = 1
    assert isinstance(savetoCSV(data2,filename,fields,type_data), data_frame.to_csv(index=False))
