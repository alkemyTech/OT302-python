from wl_mapreduce_op import *
import pytest 


post = "posts.xml"


"""  
Está función llama un archivo de tipo .xml y lo convierte en un objeto para acceder a sus atributos,
la información del archivo .xml se itera con un condicional ==1 para que cumpla la condición de buscar 
el elemento  "PostType1"  con respuesta no aceptada "AcceptedAnswerId" == None,se dividen en 16 chunks.


input = la entradas son xmlfile = que sera igual a un archivo .xml en este caso posts.xml,
        number_of_chunks = hace referencia a las particiones del archivo que por defecto es de 16

output =  La salida sera un objeto chunks contenido en una lista

  """


def test_chunkify():
    assert isinstance(chunkify(xmlfile = post,number_of_chunks =16),list)



""" 
La función mapper(), realiza el mapeo de los datos y como resultados retorna
una lista para cada consigna a resolver.

input = La entrada sera una lista chunk con el listado de tags provenientes de la función anterior.

Output = La salida será una lista que contiene un listado con los tagas, es decir una lista de listas

"""

def test_mapper() :
    lista =chunkify(xmlfile = post,number_of_chunks =16)
    assert isinstance(mapper(chunk=lista),list)


""" 
La función shuffle pasa la salida de la función mapper(Lista que contine un listado de los tagas)

Output : La salida sera una lista que agrupa los elementos de los tags en un solo listado

"""

def test_shuffler():
    lista =chunkify(xmlfile = post,number_of_chunks =16)
    lista2 = mapper(chunk=lista)
    assert isinstance(shuffler(mapper = lista2),list)



"""  
La fución reduce ejecuta una función reductora sobre cada elemento de un array,
 devolviendo como resultado un único valor

input : Listado  de las ocurrencias de los tags

Output : La salida sera un objeto que contiene la cantidad del top 10 de los tags
"""

def test_reduce():
    lista =chunkify(xmlfile = post,number_of_chunks =16)
    lista2 = mapper(chunk=lista)
    lista3 = shuffler(mapper =lista2)
    assert isinstance(reduce(lista =lista3),object)

"""
La función SavetoCSV fuarda la salida de la función

   input:
        data (list | pandas.Serie): los datos reducidos a grabar en el reporte
        filename (string): nombre del archivo csv
        fields (list): es una lista con los nombres de las columnas del reporte

        
    output : La salida será un archivo .CSV con las casacteristicas menciondas en el input
    
"""


def test_savetoCSV():
    lista =chunkify(xmlfile = post,number_of_chunks =16)
    lista2 = mapper(chunk=lista)
    lista3 = shuffler(mapper =lista2)
    lista4 = reduce(lista=lista3)
    assert isinstance(savetoCSV(data =lista4 ,filename="OP_top10tags.csv",fields=["TAG", "COUNT"]), list)
    
