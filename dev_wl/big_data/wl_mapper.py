#!/usr/bin/env python

# Este script hace la parte del MAPEO, va en la opcion -mapper de la linea de comando de Hadoop
import xml.etree.ElementTree as ET
import sys


i = 0 
# lee linea por linea la entrada del archivo posts.xml
for line in sys.stdin:
    if i > 1:
        # las dos primeras lineas de la entrada son etiquetas del XML que no hay que tomar.
        try:
            root = ET.fromstring(line)
            dir_row = root.attrib
            # seleccionan solo las lineas a procesar que cumplen con la condicion de la consigna de obtener los tags
            # de las preguntas sin respestas aceptadas
            if (dir_row.get("PostTypeId") == "1" and dir_row.get("AcceptedAnswerId") == None):
                    tags_names = (
                        dir_row.get("Tags")
                        .replace("<", "")
                        .replace(">", " ")
                        .lstrip()
                        .split()
                    )
                    for tag in tags_names:
                        tag = tag.strip()
                        # imprime las etiquetas clave y el valor 1 para que sea tomado por el script que realiza el reduce
                        print(f'{tag};1')
        except:
        # para saltear las etiquetas al final del archivo posts.xml
            pass
    i = i + 1

