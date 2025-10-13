import time
from DataStructures.List import array_list as al
from DataStructures.List import single_linked_list as sl
from DataStructures.List import list_node as ln
from DataStructures.Map import map_entry as me
from DataStructures.Map import map_functions as mf
from DataStructures.Map import map_linear_probing as lp
from DataStructures.Map import map_separate_chaining as sc
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

    catalog=sc.new_map()
    return catalog



# Funciones para la carga de datos

def load_data(catalog, filename):
    """
    Carga los datos del reto
    """
    key=0
    valor={}
    archivo=open(filename,"r")
    titulos=archivo.readline().split(",")
    for i in range(0, len(titulos)):
        if titulos[i] =="pickup_datetime":
            llave=i
        elif titulos[i] =="neighborhood":
            llave=i
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


def req_2(catalog, inicio, final, N):
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
