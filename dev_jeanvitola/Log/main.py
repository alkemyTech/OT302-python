from log import log

# Prueba para  para probar la funcionalidad del LOG


def suma(a, b):
    c = a + b
    return c


suma(9, 8)
logger = log.getLogger(__name__)
log.debug("se ha implementado la suma")
