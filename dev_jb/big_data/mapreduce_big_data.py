# Modulos #
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

# Variables a utilizar
file_path = r'/Users/jeremy/Code/big_data/Stack Overflow 11-2010/112010 Meta Stack Overflow'
file_name = 'posts.xml'
file_name2 = 'comments.xml'

"""
Funcion: mapea el archivo .xml dejando una lista para cada consigna con los datos listos para hacer un reduce  
Args:
    file_path (str, obligatorio): path donde se encuentran almacendaos los archivos
    file_name (str, obligatorio): nombre del archivo post.xml 
    file_name2 (str, obligatorio): nombre del archivo comments.xml 
Return:
    list_tags: lista con todos los tags de los post con respuestas aceptadas
    list_relation: lista de 2 coordenadas con los score y cantidad de palabras por pregunta de un post
    list_answer_delay: lista de 3 coordenadas con el id del post, la fercha de creacion del post y la fecha de creacion
    del primer comentario
"""

def mapper(file_path : str,
           file_name :str,
           file_name2 : str
):
    """
    Funcion: mapea el archivo comments.xml devolviendo un diccionario con el id del post y la fecha de careacion de todos los 
    comentarios relacionado al post
    Args:
        file_path (str, obligatorio): path donde se encuentran almacendaos los archivos
    file_name (str, obligatorio): nombre del archivo comments.xml 
    Return:
        list_end_time: lista de 2 coordenadas con el id del post y la fechad ecereacion del comentario
    """
    
    def mapper_comments(file_path : str,
                        file_name : str
    ):
        # Genera el path donde se encuentra el archivo comments.xml
        xml_file = Path(file_path, file_name)
        
        # Crea el objeto tree
        tree = ET.parse(xml_file)

        # Genera el elemento root
        root = tree.getroot()
        
        # Lista que se utilizara en el proceso
        list_end_time = []
        
        # Itera sobre las filas del .xml para obtener los datos del mappeo
        for row in root.iter('row'):
            # Genera el objeto de treae la columna deseada
            cursor_row = row.attrib
            
            # Guarda el postId
            post_id = cursor_row.get('PostId')
            
            # Guarda el tiempo de creacion del comentario
            end_time = cursor_row.get('CreationDate')
            
            # Genera el array de 2 coordenadas con el postId y el tiempo de creacion del comentario
            list_end_time.append((post_id, end_time))
        
        # Devuele la lista
        return list_end_time

    """
    Funcion: deja una lista con el primer comentario de cada id de post
    Args:
        list_mapped (list, obligatorio)): lista mapeada obteniada de la funcion de mapeo
    Return:
        list_comment: devuelve la lista con el id del post relacionado al comentatio y la fecha del primer comentario
    """
    
    def reduce_comments(list_mapped : list
    ):
        # Crea los diccionarios a utilizar
        arrange = defaultdict(list)
        list_comment = defaultdict(list)
        
        # Iterea sobre la lista recibida para genrar una diccionario con key unica referida al id
        # y los calores de todos los creation time referidos a ese id
        for id, time in list_mapped:
            arrange[id].append(time)
        
        # Itera sobre le diccionario previamente cereado para encontrar el valor minimo de cada creation time y 
        # gurdarlo en un nuveo diccionario
        for id, times in arrange.items():
            list_comment[id].append(min(times))
        
        # Devuelve el listado final
        return list_comment
    
    # Genera el path donde se encuentra el archivo posts.xml
    xml_file = Path(file_path, file_name)
    
    # Crea el objeto tree
    tree = ET.parse(xml_file)

    # Genera el elemento root
    root = tree.getroot()

    # Listas y diccionarios que se utilizaran en el proceso
    list_tags = []
    list_relation = []
    list_answer_delay = []
    list_start_time = {}
    aux_answer_delay = defaultdict(list)

    # Itera sobre las filas del .xml para obtener los datos del mappeo 
    for row in root.iter('row'):
        # Genera el objeto de treae la columna deseada
        cursor_row = row.attrib
        
        # Accede solo a los posts que sean de tipo 'question'
        if cursor_row.get('PostTypeId') == '1':
            
            # Cuenta la contidad de palabras que hay en el texto del post
            body_w_count = len(cursor_row.get('Body').replace('<', '').replace('>', '').strip().split(' '))
            
            # Guarda el score de post
            post_score = cursor_row.get('Score')
            
            # Crea un array de 2 coordenadas por posicion que almacena los datos para relacionar el score con
            # la cantidad de palabras en el post
            list_relation.append([int(post_score) ,body_w_count])
            
            # Gurda la fecha de creacion del post
            ceration_time = cursor_row.get('CreationDate')
            
            # Revisa que el post tenga una respuesta aceptada
            if cursor_row.get("AcceptedAnswerId") != None:
                # Itera sobre los tags un post para agregarlos a una lista total
                for tag in cursor_row.get("Tags").replace('<', '').replace('>', ' ').strip().split(' '):
                    list_tags.append(tag)
            
        # Toma las columnas que son Answer
        else:
            # Guarda el id del post 
            question_id = cursor_row.get('Id')
            # Gurda el el tiempo de creacion del post en una lista referenciado por su id de post
            list_start_time[question_id] = ceration_time

    # Obtiene los datos de tiempo de creacion del primer comentario refereido a cada id
    aux_answer_delay = reduce_comments(mapper_comments(file_path, file_name2))
    
    # Guarda los id que no tienen un post asignado
    to_delete = {x : aux_answer_delay[x] for x in set(aux_answer_delay) - set(list_start_time)}
    
    # Elimina los comentarios que no tengan un post al que ser referidos
    for key in to_delete.keys():
        aux_answer_delay.pop(key) 
    
    # Genera un a lista con el id, la fecha de creacion del post y del comentario
    for id, values in aux_answer_delay.items():
        list_answer_delay.append([key, values[0], list_start_time[id]])
            
    # Devueleve las listas con los datos finales mapeados
    return list_tags, list_relation, list_answer_delay

# Desempaqueta las listas
list_tags, list_relation, list_answer_delay = mapper(file_path, file_name, file_name2)

# Top 10 tags con mayores respuestas acetadas
print(pd.Series(list_tags).value_counts().head(10))
print('\n')

# Correlacion entre cantidad de palabras en un post y su score
df_relation = pd.DataFrame(data = np.array(list_relation), columns = ['score', 'body'])
print(df_relation.corr())
print('\n')

# Grafico de la relacion
plt.scatter(x = df_relation['score'], y = df_relation['body'])

# Demora de respuesta promedio en posts
df_answer_delay = pd.DataFrame(data = np.array(list_answer_delay), columns = ['id', 'end_time', 'start_time'])
df_answer_delay['end_time'] = df_answer_delay['end_time'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f"))
df_answer_delay['start_time'] = df_answer_delay['start_time'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f"))
df_answer_delay['diff_time'] = df_answer_delay['end_time'] - df_answer_delay['start_time']
avg_answer_time = np.average(df_answer_delay['diff_time'].dt.total_seconds())
print(str(timedelta(seconds = avg_answer_time)))