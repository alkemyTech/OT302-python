
import logging as log
import os


try:
    os.makedirs('./logs/')
except FileExistsError:
    pass

""" Level logger: 
LOG.BASICCONFIG = Acá se establecen las configuraciones del logger que pueden ser varias
dependiente de que tanto sea el registro que queramos

LEVEL LOGGER = Logger.debug(), Logger.info(), Logger.warning(), Logger.error(), y Logger.critical() todos crean registros de 
registro con un mensaje y un nivel que corresponde a sus respectivos nombres de método. El mensaje es en realidad una cadena de formato, 
que puede contener la sintaxis estándar de sustitución de cadenas de %s, %d, %f, y así sucesivamente. 

FORMAT : Los formateadores  configuran el orden final, la estructura y el contenido del mensaje de 
registro. 

datefmt : Proporciona el formato fecha del registro del evento

Handlers(Gestores) = Son responsablkes de enviar los mensajes de los registros
de una severiad especifica a una ubicación especifica

SUBCLASES 
filehandler = es una instancia que envia mensajes de los archivos del disco
streamhandler = es una instancia que envia los mensajes a los stream(objeto de tipo archivo)


"""

# configuraciones del Log
log.basicConfig(level=log.DEBUG,
                format='%(asctime)s: %(levelname)s [%(filename)s:%(lineno)s] %(message)s',
                datefmt='%Y-%m-%d',
                handlers=[
                    log.FileHandler('./logs/dags_ETL.log'),
                    log.StreamHandler()
                ])
