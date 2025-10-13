import time
import csv


csv.field_size_limit(2147483647)

import sys

default_limit = 1000
sys.setrecursionlimit(default_limit*10)

def convertir_a_iso(fecha):
    fecha_partes = fecha.split(" ")
    dia, mes, año = fecha_partes[0].split("/")
    hora = fecha_partes[1]
    return f"{año}-{mes}-{dia} {hora}"

def new_logic():
    """
    Crea el catalogo para almacenar las estructuras de datos
    """
    #TODO: Llama a las funciónes de creación de las estructuras de datos
    pass


# Funciones para la carga de datos

def load_data(catalog, filename):
    """
    Carga los datos del reto
    """
    key=0
    valor={}
    archivo=open(filename,"r")
    titulos=archivo.readline().split(",")
    for tipo in titulos:
        if tipo =="pickup_datetime":
            llave=tipo
    linea=archivo.readline().split(",")
    while len(linea)>0:
        fecha=convertir_a_iso(linea[llave])
        key=fecha
        for i in range(0,len(titulos)):
            if i!=llave:
                valor[titulos[i]]=linea[i]
        catalog=sc.put(catalog,key,valor)
        linea=archivo.readline()
        valor={}
    archivo.close
    pass

# Funciones de consulta sobre el catálogo


def req_1(catalog):
    """
    Retorna el resultado del requerimiento 1
    """
    # TODO: Modificar el requerimiento 1
    pass


def req_2(catalog):
    """
    Retorna el resultado del requerimiento 2
    """
    # TODO: Modificar el requerimiento 2
    pass


def req_3(catalog):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    pass


def req_4(catalog):
    """
    Retorna el resultado del requerimiento 4
    """
    # TODO: Modificar el requerimiento 4
    pass


def req_5(catalog):
    """
    Retorna el resultado del requerimiento 5
    """
    # TODO: Modificar el requerimiento 5
    pass

def req_6(catalog):
    """
    Retorna el resultado del requerimiento 6
    """
    # TODO: Modificar el requerimiento 6
    pass


# Funciones para medir tiempos de ejecucion

def get_time():
    """
    devuelve el instante tiempo de procesamiento en milisegundos
    """
    return float(time.perf_counter()*1000)


def delta_time(start, end):
    """
    devuelve la diferencia entre tiempos de procesamiento muestreados
    """
    elapsed = float(end - start)
    return elapsed
