from wl_mapreduce_op import *
import pytest 



post = "posts.xml"



def test_chunkify():
    assert isinstance(chunkify(xmlfile = post,number_of_chunks =16),list)


def test_mapper() :
    lista =chunkify(xmlfile = post,number_of_chunks =16)
    assert isinstance(mapper(chunk=lista),list)


def test_shuffler():
    lista =chunkify(xmlfile = post,number_of_chunks =16)
    lista2 = mapper(chunk=lista)
    assert isinstance(shuffler(mapper = lista2),list)


def test_reduce():
    lista =chunkify(xmlfile = post,number_of_chunks =16)
    lista2 = mapper(chunk=lista)
    lista3 = shuffler(mapper =lista2)
    assert isinstance(reduce(lista =lista3),object)


def test_savetoCSV():
    lista =chunkify(xmlfile = post,number_of_chunks =16)
    lista2 = mapper(chunk=lista)
    lista3 = shuffler(mapper =lista2)
    lista4 = reduce(lista=lista3)
    assert isinstance(savetoCSV(data =lista4 ,filename="OP_top10tags.csv",fields=["TAG", "COUNT"]), list)
    
