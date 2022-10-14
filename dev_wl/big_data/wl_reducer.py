#!/usr/bin/env python

# Este script hace la parte del REDUCER, va en la opcion -reducer de la linea de comando de Hadoop
import sys
import pandas as pd

last_word = None
last_count = 0
cur_word = None

lista = []

# lee linea por linea la entrada que retorna el script de mapeo.
for line in sys.stdin:
    
    line = line.strip()

    # se separa la clave y el valor
    cur_word, count = line.split(';', 1)
    count = int(count)

    # arma una lista de diccionarios con la informacion de cada tag
    lista.append(
        {
        'cur_word': cur_word,
        'count': count
    })
   

# se calcula la cantidad de veces que se repite en tag en las pregunas sin respuestas aceptadas
df = pd.DataFrame(lista).sort_values(['cur_word'])
resultado = df.groupby(['cur_word'])['count'].sum()
resultado.sort_values(ascending=False, inplace=True)

# se imprime el resultado que dara origen al archvio part-00000 en el directorio resultado
print(resultado)

