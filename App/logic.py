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

def fecha(fecha,tipo="todo"):
    fecha_partes = fecha.split(" ")
    dia, mes, año = fecha_partes[0].split("/")
    if len(dia)<2:
        dia="0"+dia
    hora, minuto, segundos = fecha_partes[1].split(":")
    if len(hora)<2:
        hora="0"+hora
    if tipo == "todo":
        return f"{año}-{mes}-{dia} {hora}:{minuto}:{segundos}"
    elif tipo == "fecha":
        return f"{año}-{mes}-{dia}"
    elif tipo == "hora":
        return f"{hora}:{minuto}:{segundos}"

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
        fecha=fecha(linea[llave])
        key=fecha
        for i in range(0,len(titulos)):
            if i!=llave:
                valor[titulos[i]]=linea[i]
        catalog=sc.put(catalog,key,valor)
        linea=archivo.readline()
        valor={}
    archivo.close
    return catalog

# Funciones de consulta sobre el catálogo


def req_1(catalog, inicio, final, N):
    """
    Retorna el resultado del requerimiento 1
    """
    t_inicial = time.time()
    elementos = catalog["table"]["elements"]
    rta=[]
    for i in elementos: #filtro
        if inicio <= i["pickup_datetime"] <= final:
            rta.append(i)
    
    rta=sl.quick_sort(rta,sl.sort_criteria1) 
    
    cantidad = len(rta)

    if cantidad > 2*N:
        primeros = rta[:N]
        ultimos = rta[-N:]
    else:
        primeros = rta
        ultimos = []

    
    info_primeros = []
    for t in primeros:
        info_primeros.append({
            "pickup_datetime": t["pickup_datetime"],
            "pickup_location": [t["pickup_latitude"], t["pickup_longitude"]],
            "dropoff_datetime": t["dropoff_datetime"],
            "dropoff_location": [t["dropoff_latitude"], t["dropoff_longitude"]],
            "trip_distance": t["trip_distance"],
            "total_amount": t["total_amount"]
        })

    info_ultimos = []
    for t in ultimos:
        info_ultimos.append({
            "pickup_datetime": t["pickup_datetime"],
            "pickup_location": [t["pickup_latitude"], t["pickup_longitude"]],
            "dropoff_datetime": t["dropoff_datetime"],
            "dropoff_location": [t["dropoff_latitude"], t["dropoff_longitude"]],
            "trip_distance": t["trip_distance"],
            "total_amount": t["total_amount"]
        })
        
        
        
    t_final = time.time()
    tiempo_ms = (t_final - t_inicial) * 1000
    
    respuesta = {
        "tiempo_ms": round(tiempo_ms, 2),
        "total_trayectos": cantidad,
        "primeros": info_primeros,
        "ultimos": info_ultimos
    }
    
    return respuesta

def req_2(catalog, inicio, final, N):
    """
    Retorna el resultado del requerimiento 2
    """
    t_inicial = time.time()
    dic=catalog["table"]['elements']
    res=[]
    for i in dic:
        if inicio <= i["pickup_latitude"] <= final: #filtro
            res.append(i)
            
    res=sl.quick_sort(res,sl.sort_criteriar2) #uso quick sort con un sort criteria personalizado para este requerimiento
    s=len(res)
    respuesta=[]
    if s > 2*N:
        primeros=res[:N] 
        ultimos=res[-N:]
    else:
        respuesta=res
        ultimos=[]
        
    info_primeros = []
    for t in primeros:
        info_primeros.append({
            "pickup_datetime": t["pickup_datetime"],
            "pickup_location": [t["pickup_latitude"], t["pickup_longitude"]],
            "dropoff_datetime": t["dropoff_datetime"],
            "dropoff_location": [t["dropoff_latitude"], t["dropoff_longitude"]],
            "trip_distance": t["trip_distance"],
            "total_amount": t["total_amount"]
        })

    info_ultimos = []
    for t in ultimos:
        info_ultimos.append({
            "pickup_datetime": t["pickup_datetime"],
            "pickup_location": [t["pickup_latitude"], t["pickup_longitude"]],
            "dropoff_datetime": t["dropoff_datetime"],
            "dropoff_location": [t["dropoff_latitude"], t["dropoff_longitude"]],
            "trip_distance": t["trip_distance"],
            "total_amount": t["total_amount"]
        })
    t_final = time.time()
    tiempo_ms = (t_final - t_inicial) * 1000

    respuesta = {
        "tiempo_ms": round(tiempo_ms, 2),
        "total_trayectos": s,
        "primeros": info_primeros,
        "ultimos": info_ultimos
    }

    return respuesta


def req_3(catalog,inicial, final, num):
    ini=get_time()
    lista=sl.new_list()
    for i in catalog["table"]["elements"]:
        i=i["first"]
        while i!=None:
            millas=i["info"]["value"]["trip_distance"]
            if float(millas)>=inicial and float(millas)<=final:
                lista=sl.add_last(lista,i["info"])
    lista=sl.quick_sort_3(lista,sl.sort_criteria)
    tamaño=sl.size(lista)
    x={}
    if tamaño>2*num:
        primero=sl.sub_list(lista,0,num)
        x=auxiliar3(primero,0,x)
        num2=tamaño-num-1
        ultimo=sl.sub_list(lista,num2,num)
        x=auxiliar3(ultimo,num2,x)
    else:
        x=auxiliar3(lista,0,x)
    return{"tiempo de ejecicion:":delta_time(ini,get_time()),"Numero de trayectos:":tamaño,"info:":x}   
        

def req_4(catalog, fecha_busqueda, modo, tiempo_ref, N):
    """
    Retorna el resultado del requerimiento 4
    """
    t_inicial = get_time()
    dic = catalog["table"]["elements"]
    tabla = {}
    for i in dic:
        fecha = i["dropoff_datetime"].strftime("%Y-%m-%d")
        if fecha not in tabla:
            tabla[fecha] = []
        tabla[fecha].append(i)

    trayectos = tabla.get(fecha_busqueda, [])
    filtrados = []

    for t in trayectos:
        hora_fin = t["dropoff_datetime"].time()
        if modo == "ANTES" and hora_fin < tiempo_ref:
            filtrados.append(t)
        elif modo == "DESPUES" and hora_fin > tiempo_ref:
            filtrados.append(t)

    filtrados = sl.quick_sort(filtrados, sl.sort_criteria_r4)
    s = len(filtrados)

    if s > 2 * N:
        primeros = filtrados[:N]
        ultimos = filtrados[-N:]
    else:
        primeros = filtrados
        ultimos = []

    info_primeros = []
    for t in primeros:
        info_primeros.append({
            "pickup_datetime": t["pickup_datetime"],
            "pickup_location": [t["pickup_latitude"], t["pickup_longitude"]],
            "dropoff_datetime": t["dropoff_datetime"],
            "dropoff_location": [t["dropoff_latitude"], t["dropoff_longitude"]],
            "trip_distance": t["trip_distance"],
            "total_amount": t["total_amount"]
        })

    info_ultimos = []
    for t in ultimos:
        info_ultimos.append({
            "pickup_datetime": t["pickup_datetime"],
            "pickup_location": [t["pickup_latitude"], t["pickup_longitude"]],
            "dropoff_datetime": t["dropoff_datetime"],
            "dropoff_location": [t["dropoff_latitude"], t["dropoff_longitude"]],
            "trip_distance": t["trip_distance"],
            "total_amount": t["total_amount"]
        })

    t_final = get_time()
    tiempo_ms = t_final - t_inicial

    respuesta = {
        "tiempo_ms": round(tiempo_ms, 2),
        "total_trayectos": s,
        "primeros": info_primeros,
        "ultimos": info_ultimos
    }

    return respuesta


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


def auxiliar3(sl,num,x):
    actual=sl["firts"]
    while actual!=None:
        x[num]["Fecha y tiempo de recogida"]=fecha(actual["info"]["key"])
        x[num]["Latitud y longitud de recogida"]=[actual["info"]["value"]["pickup_latitude"],actual["info"]["value"]["pickup_longitude"]]
        x[num]["Fecha y tiempo de terminación"]=fecha(actual["info"]["value"]["dropoff_datetime"])
        x[num]["Latitud y longitud de dejada"]=[actual["info"]["value"]["dropoff_latitude"],actual["info"]["value"]["dropoff_longitude"]]
        x[num]["Distancia recorrida"]=actual["info"]["value"]["trip_distance"]
        x[num]["Costo total pagado"]=actual["info"]["value"]["total_amount"]
    return x
        