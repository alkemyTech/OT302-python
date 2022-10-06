import logging as lg

from colorlog import getLogger

class CustomLogger():

    def __init__(self, logger1, logger2) -> None:
        self.log_name1 = logger1
        self.log_name2 = logger2
        
        
        #Crear Handler de cada  logger
        log1Handler = lg.StreamHandler()
        log1Handler.setLevel(lg.INFO)
        log2Handler = lg.StreamHandler()
        log2Handler.setLevel(lg.INFO)
        #Adicionar Formato a cada logger
        log1Format = lg.Formatter("%(asctime)s - %(name)s - %(message)s", datefmt="%Y-%m-%d")
        log1Handler.setFormatter(log1Format)
        log2Format = lg.Formatter("%(asctime)s - %(name)s - %(message)s", datefmt="%Y-%m-%d")
        log2Handler.setFormatter(log2Format)
        #Crear cada logger
        self.jujuy_log = lg.getLogger(self.log_name1)
        self.jujuy_log.addHandler(log1Handler)
        self.palermo_log = lg.getLogger(self.log_name2)
        self.palermo_log.addHandler(log2Handler)
        

        
    
    def get_logger(self, name_log: str):
        if name_log == self.log_name1:
            return self.jujuy_log
        elif name_log == self.log_name2:
            return self.palermo_log
        else:
            raise 'Logger name no defined'
        




        
        
