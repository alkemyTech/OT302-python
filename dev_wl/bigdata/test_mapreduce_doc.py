""" Este script "test_mapreducer.py" realiza el testeo de las funciones de "mapreduce.py" usando la libreria "pytest".

    Lo que se hace es documentar los test con dosctrins que sirvan para entender mejor lo que se está haciendo.

    Las consignas que debe resolvar "mapreduce.py"
        .Top 10 tipo de post sin respuestas aceptadas.
        .Relación entre cantidad de respuestas de un post y su puntaje.
        .Top 10 preguntas que tuvieron mayor tiempo de actividad.

    El script a testear fue realizado por: "Andres Reisz"
"""
# Se importan todas las funciones que se van a testear
from mapreduce import *

# Ubicacion y nombre del archivo que se utiliza para el testeo
FILE_PATH = "../meta_stackOverflow/posts.xml"


def test_mapreduce_tasker():
    """ La funcion "mapreduce_tasker" lo que hace es administrar el llamado a diferentes funciones segun la
        consigna a resolver.
        En este caso llama a "mapper_task_1" y "reducer_task_1" que en realidad son las funciones que estamos testeando.
        Se pasa como parametro el archivo "posts.xml" y se verifica si resuelve correctamente la consigna de obtener el 
        Top 10 de los tags que mas figuran en las preguntas.
        La funcion "mapreduce_tasker" que recibe como parametros, el archvio "posts.xml", la funcion "mapper_task_1" y la funcion
        "reducer_task_1", debe retornar un diccionario con clave: "tag" y valor: "cantidad de apariciones", son 10 "clave: valor", que
        representan el Top 10 de los tags.
        
        Para verificar si resuelve correctamente la consigna, se la compara con un diccionario que contiene el Top 10 de los tags.
        Se esta comparando contra el resultado correcto que debe arrojar, con los valores del archivo posts.xml.

        Resultado del test: passed

    """
    assert mapreduce_tasker(xml_file_path = FILE_PATH,
                         map_root = mapper_task_1,
                         reduce = reducer_task_1) == {  'discussion':2916,
                                                        'feature-request':2815,
                                                        'bug':1396,
                                                        'support':1261,
                                                        'stackoverflow':848,
                                                        'status-completed':647,
                                                        'tags':524,
                                                        'reputation':427,
                                                        'area51':372,
                                                        'questions':354}


def test_mapreduce_tasker2():
    """ La funcion "mapreduce_tasker" lo que hace es administrar el llamado a diferentes funciones segun la
        consigna a resolver.
        En este caso llama a "mapper_task_2" y "reducer_task_2" que en realidad son las funciones que estamos testeando.
        Se pasa como parametro el archivo "posts.xml" y se verifica si resuelve correctamente la consigna de obtener la
        relacion entre cantidad de respuestas de un post y su puntaje.
        La funcion "mapreduce_tasker" que recibe como parametros, el archvio posts.xml, la funcion "mapper_task_2" y la funcion
        "reducer_task_2", debe retornar un valor que indique la relacion entre cantidad de respuestas de un post y su puntaje.
        
        Para verificar si resuelve correctamente la consigna, el valor retornado por la funcion "mapper_tasker" se compara contra el resultado 
        correcto, segun los datos que se obtienen de procesar el archivo "posts.xml".

        Resultado del test: passed

    """

    assert mapreduce_tasker(xml_file_path = FILE_PATH,
                         map_root = mapper_task_2,
                         reduce = reducer_task_2) == 1.254095319555108


def test_mapreduce_tasker3():
    """ La funcion "mapreduce_tasker" lo que hace es administrar el llamado a diferentes funciones segun la
        consigna a resolver.
        En este caso llama a "mapper_task_3" y "reducer_task_3" que en realidad son las funciones que estamos testeando.
        Se pasa como parametro el archivo "posts.xml" y se verifica si resuelve correctamente la consigna de obtener el
        listado de Top 10 preguntas que tuvieron mayor tiempo de actividad.
        La funcion "mapreduce_tasker" que recibe como parametros, el archvio "posts.xml", la funcion "mapper_task_3" y la funcion
        "reducer_task_3", debe retornar una lista de valores.
        
        Para verificar si la funcion funciona correctamente, verificamos que el tipo de dato que esta retornando sea una lista.
        Tambien se verifica que la cantidad de elementos de la lista sea 10

        Resultado del test: passed

    """
    assert isinstance(mapreduce_tasker(xml_file_path = FILE_PATH,
                         map_root = mapper_task_3,
                         reduce = reducer_task_3), list)
    assert len(mapreduce_tasker(xml_file_path = FILE_PATH,
                         map_root = mapper_task_3,
                         reduce = reducer_task_3)) == 10

"""
   Las siguientes dos funciones que se van a testear "get_single_xml" y "map_function", son dos funciones que son utilizadas
   en las funciones antes testeadas de "mapper_tas_1/2/3" y "reducer_task_1/2/3"
"""


def test_get_single_xml():
    """ La funcion "get_single_xml" extrae la informacion de un archvio xml (en nuestro caso posts.xml)
        y retorna un objeto. Retorna el objeto de la funcion "ET.parse(xml_file_path).getroot()"
        
        Testeamos que la funcion retorne efectivamente un objeto cuando le pasamos como parametro la ubicacion y el archvo "posts.xml"

        Resultado del test: passed
    """
    assert isinstance(get_single_xml(FILE_PATH), object)


def test_map_function():
    """ La funcion "map_function" devuelve un diccionario de cada atributo contenido en un objeto raíz previamente extraído
        de un XML con el método "xml.etree.ElementTree getroot()".
        
        Testeamos que la funcion retorne efectivamente un diccionario cuando le pasamos como parametro la ubicacion y el archvo "posts.xml"

        Resultado del test: failed

        La funcion no esta retornando un diccionario como se indica en su documentacion. En realizad desta retornando un objeto.
    """
    assert isinstance(map_function(get_single_xml(FILE_PATH)), dict)

def test_map_function2():
    """ La funcion "map_function" deberia retornar un diccionario de cada atributo contenido en un objeto raíz previamente extraído
        de un XML con el método "xml.etree.ElementTree getroot()", segun como esta documentada, pero como vimos en el testeo anterior
        no retorna un diccionario.
        
        Testeamos que la funcion retorne un objeto y no el diccionario como dice la documentacion.

        Resultado del test: passed

        La funcion no esta retornando un diccionario como se indica en su documentacion. En realizad desta retornando un objeto.
    """
    assert isinstance(map_function(get_single_xml(FILE_PATH)), object)

