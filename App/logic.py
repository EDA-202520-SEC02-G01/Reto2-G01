import time
from DataStructures.List import array_list as al
from DataStructures.List import single_linked_list as sl
from DataStructures.List import list_node as ln
from DataStructures.Map import map_entry as me
from DataStructures.Map import map_functions as mf
from DataStructures.Map import map_linear_probing as lp
from DataStructures.Map import map_separate_chaining as sc
import csv
import math

csv.field_size_limit(2147483647)

import sys

default_limit = 1000
sys.setrecursionlimit(default_limit*10)

def fecha(fecha,tipo="todo"):
    fecha_partes = fecha.split(" ")
    fecha_1= fecha_partes[0].split("-")
    año=fecha_1[0]
    mes=fecha_1[1]
    dia=fecha_1[2]
    if len(dia)<2:
        dia="0"+dia
    tiempo = fecha_partes[1].split(":")
    hora =tiempo [0]
    minuto= tiempo[1]
    segundos=tiempo[2]
    if len(hora)<2:
        hora="0"+hora
    if tipo == "todo":
        return f"{año}/{mes}/{dia} {hora}:{minuto}:{segundos}"
    elif tipo == "fecha":
        return f"{año}/{mes}/{dia}"
    elif tipo == "hora":
        return f"{hora}:{minuto}:{segundos}"
    elif tipo == "fh":
        return f"{año}/{mes}/{dia} {hora}"
    elif tipo =="ho":
        return f"{hora}"
    

def new_logic():
    """
    Crea el catalogo para almacenar las estructuras de datos
    """

    catalog=sc.new_map(10,0.5)
    return catalog



# Funciones para la carga de datos

"""def load_data(catalog, filename):

    key=0
    valor={}
    import os
    ruta = os.path.join("Data", filename)
    archivo = open(ruta, "r")
    titulos=archivo.readline().split(",")
    for i in range(0, len(titulos)):
        if titulos[i] =="pickup_datetime":
            llave=i
        elif titulos[i] =="neighborhood":
            llave=i
    linea=archivo.readline().split(",")
    while len(linea)>0:
        fecha=linea[llave]
        key=fecha
        for i in range(0,len(titulos)):
            if i!=llave:
                valor[titulos[i]]=linea[i]
        catalog=sc.put(catalog,key,valor)
        linea=archivo.readline()
        valor={}
    archivo.close()
    return catalog"""
    
def load_data(catalog, filename):
    """
    Carga los datos del reto desde un archivo CSV.
    """
    import os
    ruta = os.path.join("Data", filename)
    
    if filename == "nyc-neighborhoods.csv":
        with open(ruta, "r", encoding="utf-8") as archivo:
            titulos = archivo.readline().strip().split(";")

            # Encuentra índice de la llave
            if "pickup_datetime" in titulos:
                llave = titulos.index("pickup_datetime")
            elif "neighborhood" in titulos:
                llave = titulos.index("neighborhood")

            for linea in archivo:
                linea = linea.strip()
                if not linea:
                    continue  

                campos = linea.split(";")
                if len(campos) != len(titulos):
                    continue  

                key = campos[llave]
                valor = {}

                for i in range(len(titulos)):
                    if i != llave:
                        valor[titulos[i]] = campos[i]

            catalog = sc.put(catalog, key, valor)
    else:
        with open(ruta, "r", encoding="utf-8") as archivo:
            titulos = archivo.readline().strip().split(",")

            # Encuentra índice de la llave
            if "pickup_datetime" in titulos:
                llave = titulos.index("pickup_datetime")
            elif "neighborhood" in titulos:
                llave = titulos.index("neighborhood")

            for linea in archivo:
                linea = linea.strip()
                if not linea:
                    continue 

                campos = linea.split(",")
                if len(campos) != len(titulos):
                    continue  

                key = campos[llave]
                valor = {}

                for i in range(len(titulos)):
                    if i != llave:
                        valor[titulos[i]] = campos[i]

                catalog = sc.put(catalog, key, valor)

        return catalog


# Funciones de consulta sobre el catálogo


def req_1(catalog, inicio, final, N):
    """
    Retorna el resultado del requerimiento 1
    """
    t_inicial = get_time()
    rta = []
    inicio = fecha(inicio, "todo")
    final = fecha(final, "todo")

    elementos = catalog["table"]["elements"]
    for pos in elementos:
        if pos and pos["first"]:
            nodo = pos["first"]
            while nodo is not None:
                t = nodo["info"]["value"]
                fecha_pick = fecha(t["pickup_datetime"], "todo")
                if inicio <= fecha_pick <= final:
                    rta.append(t)
                nodo = nodo["next"]

    rta = sl.quick_sort(rta, sl.sort_criteria1)
    cantidad = len(rta)

    if cantidad > 0:
        primeros = rta[:N]
    else:
        primeros = []

    if cantidad > 2 * N:
        ultimos = rta[-N:]
    else:
        ultimos = []

    info_primeros = []
    for t in primeros:
        info_primeros.append({
            "pickup_datetime": fecha(t["pickup_datetime"], "todo"),
            "pickup_location": [t["pickup_latitude"], t["pickup_longitude"]],
            "dropoff_datetime": fecha(t["dropoff_datetime"], "todo"),
            "dropoff_location": [t["dropoff_latitude"], t["dropoff_longitude"]],
            "trip_distance": t["trip_distance"],
            "total_amount": t["total_amount"]
        })

    info_ultimos = []
    for t in ultimos:
        info_ultimos.append({
            "pickup_datetime": fecha(t["pickup_datetime"], "todo"),
            "pickup_location": [t["pickup_latitude"], t["pickup_longitude"]],
            "dropoff_datetime": fecha(t["dropoff_datetime"], "todo"),
            "dropoff_location": [t["dropoff_latitude"], t["dropoff_longitude"]],
            "trip_distance": t["trip_distance"],
            "total_amount": t["total_amount"]
        })

    t_final = get_time()
    tiempo_ms = t_final - t_inicial

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
    t_inicial = get_time()
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

    
    t_final = get_time()
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
    viajes=sc.key_set(catalog)
    for viaje in viajes["elemets"]:
        valor=sc.get(catalog,viaje)
        if float(valor["trip_distance"])>=inicial and float(valor["trip_distance"])>=final:
            lista=sl.add_last(lista,i["info"])
    lista=sl.quick_sort_3(lista,sl.default_function)
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
    return{"tiempo de ejecicion:":delta_time(ini,get_time()),
           "Numero de trayectos:":tamaño,
           "info:":x}   
        

def req_4(catalog, fecha_busqueda, modo, tiempo_ref, N):
    """
    Retorna el resultado del requerimiento 4
    """
    t_inicial = get_time()
    tabla = {}

    elementos = catalog["table"]["elements"]
    for pos in elementos:
        if pos and pos["first"]:
            nodo = pos["first"]
            while nodo is not None:
                t = nodo["info"]["value"]
                fecha_drop = fecha(t["dropoff_datetime"], "todo")
                fecha_sola = fecha_drop.split(" ")[0] 
                if fecha_sola not in tabla:
                    tabla[fecha_sola] = []
                tabla[fecha_sola].append(t)
                nodo = nodo["next"]

    trayectos = tabla.get(fecha_busqueda, [])
    filtrados = []

    for t in trayectos:
        hora_fin = fecha(t["dropoff_datetime"], "hora")  
        if modo == "ANTES":
            if hora_fin < tiempo_ref:
                filtrados.append(t)
        elif modo == "DESPUES":
            if hora_fin > tiempo_ref:
                filtrados.append(t)

    filtrados = sl.quick_sort(filtrados, sl.sort_criteria_r4)
    cantidad = len(filtrados)

    if cantidad > 0:
        primeros = filtrados[:N]
    else:
        primeros = []

    if cantidad > 2 * N:
        ultimos = filtrados[-N:]
    else:
        ultimos = []

    info_primeros = []
    for t in primeros:
        info_primeros.append({
            "pickup_datetime": fecha(t["pickup_datetime"], "todo"),
            "pickup_location": [t["pickup_latitude"], t["pickup_longitude"]],
            "dropoff_datetime": fecha(t["dropoff_datetime"], "todo"),
            "dropoff_location": [t["dropoff_latitude"], t["dropoff_longitude"]],
            "trip_distance": t["trip_distance"],
            "total_amount": t["total_amount"]
        })

    info_ultimos = []
    for t in ultimos:
        info_ultimos.append({
            "pickup_datetime": fecha(t["pickup_datetime"], "todo"),
            "pickup_location": [t["pickup_latitude"], t["pickup_longitude"]],
            "dropoff_datetime": fecha(t["dropoff_datetime"], "todo"),
            "dropoff_location": [t["dropoff_latitude"], t["dropoff_longitude"]],
            "trip_distance": t["trip_distance"],
            "total_amount": t["total_amount"]
        })

    t_final = get_time()
    tiempo_ms = t_final - t_inicial

    respuesta = {
        "tiempo_ms": round(tiempo_ms, 2),
        "total_trayectos": cantidad,
        "primeros": info_primeros,
        "ultimos": info_ultimos
    }

    return respuesta


def req_5(catalog, filtro, N):
    """
    Retorna el resultado del requerimiento 5
    """
    t_inicial = get_time()
    info=sc.new_map(1,0.5)
    keys=sc.key_set(catalog)
    for key in keys["elements"]:
        value=sc.get(catalog,key)
        if filtro==fecha(value["dropoff_datetime"],"fh"):
            valor=value.copy()
            valor["pickup_datetime"] = key
            if sc.contains(info, filtro):
                lista = sc.get(info, filtro)
            else:
                lista = al.new_list()
            al.add_last(lista, valor)
            sc.put(info, filtro, lista)
    listaf=sc.get(info,filtro)
    listaf=sl.quick_sort(listaf,sl.sort_criteriar5)
    if sl.size(listaf) > 2*N:
        primeros = al.sub_list(listaf, 1, N)
        ultimos = al.sub_list(listaf, sl.size(listaf) - N + 1, N)
    else:
        primeros = listaf
        ultimos = None
    
    primerosf = al.new_list()
    for t in primeros["elements"]:
        al.add_last(primerosf,{
            "pickup_datetime": t["pickup_datetime"],
            "pickup_location": [t["pickup_latitude"], t["pickup_longitude"]],
            "dropoff_datetime": t["dropoff_datetime"],
            "dropoff_location": [t["dropoff_latitude"], t["dropoff_longitude"]],
            "trip_distance": t["trip_distance"],
            "total_amount": t["total_amount"]
        })

    ultimosf = al.new_list()
    if ultimos:
        for t in ultimos["elements"]:
            al.add_last(ultimosf({
                "pickup_datetime": t["pickup_datetime"],
                "pickup_location": [t["pickup_latitude"], t["pickup_longitude"]],
                "dropoff_datetime": t["dropoff_datetime"],
                "dropoff_location": [t["dropoff_latitude"], t["dropoff_longitude"]],
                "trip_distance": t["trip_distance"],
                "total_amount": t["total_amount"]
            }))

    t_final = get_time()
    tiempo_ms = (t_final - t_inicial) * 1000
    resultado = {
        "tiempo_ms": tiempo_ms,
        "total_trayectos":sl.size(listaf) ,
        "primeros": primerosf,
        "ultimos": ultimosf
    }

    return resultado
            
            
            
    
    

def req_6(catalog, bar, ini, fini, n):
    """
    Retorna el resultado del requerimiento 6
    """
    tiempo1=get_time()
    respuesta=sc.new_map(10,0.5)
    info=new_logic()
    info=load_data(info,"nyc-neighborhoods.csv")
    barrios=sc.key_set(info)
    viajes=sc.key_set(catalog)
    for viaje in viajes["elemets"]:
        if fecha(viaje,"ho")<=fini and fecha(viaje,"ho")>=ini:
            valor=sc.get(catalog,viaje)
            latitud1=valor["pickup_latitude"]
            longitud1=valor["pickup_longitude"]
            barrio=encuentra_barrio(info, barrios, latitud1,longitud1)
            valor["pickup_datetime"]=viaje
            if sc.contains(respuesta,barrio):
                lista=sc.get(respuesta,barrio)
            else:
                lista=sl.new_list()
            lista=sl.add_last(lista,valor)
            respuesta=sc.put(respuesta,barrio,lista)
    Lista_viajes=sc.get(respuesta,bar)
    Lista_viajes=sl.quick_sort(Lista_viajes,sl.sort_criteriar5)
    x={}
    tamaño=sl.size(Lista_viajes)
    if tamaño>2*n:
        primero=sl.sub_list(lista,0,n)
        x=auxiliar6(primero,0,x)
        num2=tamaño-n-1
        ultimo=sl.sub_list(lista,num2,n)
        x=auxiliar6(ultimo,num2,x)
    else:
        x=auxiliar6(Lista_viajes,0,x)
    x["Numero de trayectos"]=tamaño
    x["Tiempo de ejecucion"]=delta_time(tiempo1,get_time())
    return x


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
        x["viaje"][num]["Fecha y tiempo de recogida"]=fecha(actual["info"]["key"])
        x["viaje"][num]["Latitud y longitud de recogida"]=[actual["info"]["value"]["pickup_latitude"],actual["info"]["value"]["pickup_longitude"]]
        x["viaje"][num]["Fecha y tiempo de terminación"]=fecha(actual["info"]["value"]["dropoff_datetime"])
        x["viaje"][num]["Latitud y longitud de dejada"]=[actual["info"]["value"]["dropoff_latitude"],actual["info"]["value"]["dropoff_longitude"]]
        x["viaje"][num]["Distancia recorrida"]=actual["info"]["value"]["trip_distance"]
        x["viaje"][num]["Costo total pagado"]=actual["info"]["value"]["total_amount"]
        actual=actual["next"]
        num+=1
    return x

def haversine(lat1, lon1, lat2, lon2): 
    dLat = (lat2 - lat1) * math.pi / 180.0 
    dLon = (lon2 - lon1) * math.pi / 180.0 
    lat1 = (lat1) * math.pi / 180.0 
    lat2 = (lat2) * math.pi / 180.0 
    a = (pow(math.sin(dLat / 2), 2) + pow(math.sin(dLon / 2), 2) * math.cos(lat1) * math.cos(lat2)); 
    rad = 6371 
    c = 2 * math.asin(math.sqrt(a)) 
    return rad * c

def encuentra_barrio(map,keys,lat1,lon1):
    menor=0
    bar_m=""
    for barrio in keys["elements"]:
        info_barrio=sc.get(map,barrio)
        distancia=haversine(lat1,lon1,info_barrio["latitude"],info_barrio["longitude"])
        if distancia<menor:
            bar_m=barrio
    return bar_m
        
def auxiliar6(sl,num,x):
    actual=sl["firts"]
    while actual!=None:
        x["viaje"][num]["Fecha y tiempo de recogida"]=fecha(actual["info"]["pickup_datetime"])
        x["viaje"][num]["Latitud y longitud de recogida"]=[actual["info"]["pickup_latitude"],actual["info"]["pickup_longitude"]]
        x["viaje"][num]["Fecha y tiempo de terminación"]=fecha(actual["info"]["dropoff_datetime"])
        x["viaje"][num]["Latitud y longitud de dejada"]=[actual["info"]["dropoff_latitude"],actual["info"]["dropoff_longitude"]]
        x["viaje"][num]["Distancia recorrida"]=actual["info"]["trip_distance"]
        x["viaje"][num]["Costo total pagado"]=actual["info"]["total_amount"]
        actual=actual["next"]
        num+=1
    return x
    