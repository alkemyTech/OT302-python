from wl_map_reduce import *

import pytest


data= "posts.xml"

def test_mapper():
     assert isinstance(mapper(data),list)


def test_shuffle_sort():
    lista = mapper(data)
    assert isinstance(shuffle_sort(lista), list)


def test_reduce():
    lista = mapper(data)
    lista2 = shuffle_sort(lista)
    assert isinstance(reduce(lista2), list)
    
   
def test_savetoCSV():
    lista = mapper(data)
    lista2 = shuffle_sort(lista)
    lista3 = reduce(lista2)
    data2 = lista3
    filename = "top10tags.csv"
    fields = ["TAG", "COUNT"]
    type_data = 1
    assert isinstance(savetoCSV(data2,filename,fields,type_data), data_frame.to_csv(index=False))
