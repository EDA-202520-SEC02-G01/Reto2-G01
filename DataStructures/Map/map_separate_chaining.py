from DataStructures.List import array_list as al
from DataStructures.Map import map_entry as me
from DataStructures.Map import map_functions as mf
from DataStructures.List import single_linked_list as sl
from DataStructures.List import list_node as ln
import random


def default_compare(key, entry):

   if key == me.get_key(entry):
      return 0
   elif key > me.get_key(entry):
      return 1
   return -1


def new_map(capacity, load_factor, prime=109345121):
    import random
    a = random.randint(1, prime - 1)
    b = random.randint(0, prime - 1)

    real_capacity = mf.next_prime(int(capacity / load_factor))

    table = al.new_list()
    for i in range(real_capacity):
        al.add_last(table, sl.new_list())

    my_map = {
        "prime": prime,
        "capacity": real_capacity,
        "scale": a,
        "shift": b,
        "table": table,
        "current_factor": 0,
        "limit_factor": load_factor,
        "size": 0,
    }
    return my_map

def get(my_map, key):
    index = mf.hash_value(my_map, key)
    lista = my_map["table"]["elements"][index]
    for entry in lista:
        if default_compare(key, entry) == 0:
            return me.get_value(entry)
    return None
    
def is_empty(my_map):
    if my_map["size"]==0:
        return True
    return False

def contains(my_map, key):
    for elementos in my_map["table"]["elements"]:
        actual=elementos["first"]
        encontro=False
        while actual is not None and encontro==False:
            if actual["info"]["key"]==key:
                encontro=True
    return encontro
    
def size(my_map):
    return my_map["size"]

def value_set(my_map):
    res=al.new_list()
    for elementos in my_map["table"]["elements"]:
        actual=elementos["first"]
        while actual is not None:
            if actual["info"]["key"]!=None:
                res=al.add_last(res, actual["info"]["value"])
            actual=actual["next"]
    res["size"]=my_map["size"]
    return res

def default_function1(key, element):
    if key == element["key"]:
        return 0
    else:
        return -1


def put(my_map,key,value):
    pos=mf.hash_value(my_map,key)
    search=my_map["table"]["elements"][pos]
    present=sl.is_present(search,key,default_function1)
    if present != -1:
        encontre=False
        actual=search["first"]
        while encontre == False:
            if actual["info"]["key"]==key:
                actual["info"]["value"]=value
                encontre=True
            else:
                actual = actual["next"]
        return my_map
    else:
        sl.add_last(search, {"key": key, "value": value})
        my_map["size"]+=1
        my_map["current_factor"]=my_map["size"] / my_map["capacity"]
        if my_map["current_factor"]>my_map["limit_factor"]:
            my_map=rehash(my_map)
        return my_map
            
def remove(my_map, key):
    pos=mf.hash_value(my_map,key)
    present=sl.is_present(my_map["table"]["elements"][pos],key,)
    my_list=my_map["table"]["elements"][pos]
    my_map["table"]["elements"][pos]=sl.delete_element(my_list,present)
    my_map["size"]-=1
    my_map["current_factor"]=my_map["size"] / my_map["capacity"]
    return my_map
    
def key_set(my_map):
    res = al.new_list()
    for i in my_map["table"]["elements"]:
        actual=i["first"]
        while actual is not None:
            al.add_last(res,i["key"])
            actual=actual["next"]
    return res

def rehash(my_map):
    numero_a=my_map["capacity"]*2
    numero_a=mf.next_prime(numero_a)
    x=new_map(numero_a,my_map["limit_factor"])
    for elemento in my_map["table"]["elements"]:
        nodo=elemento["first"]
        while nodo!=None:
            if nodo["info"]["key"]!=None:
                x=put(x,nodo["info"]["key"],nodo["info"]["value"])
                nodo=nodo["next"]
    return x