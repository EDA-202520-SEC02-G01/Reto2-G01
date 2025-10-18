import sys
from App import logic as lo
from DataStructures.Map import map_separate_chaining as msc

def new_logic():
    """
        Se crea una instancia del controlador
    """
    return lo.new_logic()
    
def print_menu():
    print("Bienvenido")
    print("0- Cargar información")
    print("1- Ejecutar Requerimiento 1")
    print("2- Ejecutar Requerimiento 2")
    print("3- Ejecutar Requerimiento 3")
    print("4- Ejecutar Requerimiento 4")
    print("5- Ejecutar Requerimiento 5")
    print("6- Ejecutar Requerimiento 6")
    print("7- Salir")

def load_data(control):
    """
    Carga los datos
    """
    
    filename=input("ingrese el nomnre del archivo:")
    return lo.load_data(control,filename)
    


def print_data(control, id):
    """
        Función que imprime un dato dado su ID
    """
    print(msc.get(control,id))
    

def print_req_1(control):
    """
        Función que imprime la solución del Requerimiento 1 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 1
    pass


def print_req_2(control):
    """
        Función que imprime la solución del Requerimiento 2 en consola
    """
    
    inicio=input("ingresa la fecha de inicio:")
    final=input("ingresa la fecha final:")
    N=int(input("ingresa el numero de parametros:"))
    print(lo.req_2(control,inicio,final,N))
    


def print_req_3(control):
    """
        Función que imprime la solución del Requerimiento 3 en consola
    """
    
    inicial=int(input("distancia inicial: "))
    final=int(input("distancia final: "))
    num=int(input("numero de muestra: "))
    print(lo.req_3(control,inicial,final,num))


def print_req_4(control):
    """
        Función que imprime la solución del Requerimiento 4 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 4
    pass


def print_req_5(control):
    """
        Función que imprime la solución del Requerimiento 5 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 5
    filtro=input("ingrese la fecha para el filtro:")
    N=int(input("ingrese el numero de muestra:"))
    print(lo.req_5(control,filtro,N))
    


def print_req_6(control):
    """
        Función que imprime la solución del Requerimiento 6 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 6
    filtro=input("ingrese la hora para el filtro:")
    N=int(input("ingrese el numero de muestra:"))
    filtro2=input("ingresa la segunda hora para el filtro:")
    print (lo.req_6(control,filtro,filtro2,N))

# Se crea la lógica asociado a la vista
control = new_logic()

# main del ejercicio
def main():
    """
    Menu principal
    """
    working = True
    #ciclo del menu
    while working:
        print_menu()
        inputs = input('Seleccione una opción para continuar\n')
        if int(inputs) == 0:
            print("Cargando información de los archivos ....\n")
            data= load_data(control)
        elif int(inputs) == 1:
            print_req_1(data)

        elif int(inputs) == 2:
            print_req_2(data)

        elif int(inputs) == 3:
            print_req_3(data)

        elif int(inputs) == 4:
            print_req_4(data)

        elif int(inputs) == 5:
            print_req_5(data)

        elif int(inputs) == 6:
            print_req_6(data)

        elif int(inputs) == 7:
            working = False
            print("\nGracias por utilizar el programa") 
        else:
            print("Opción errónea, vuelva a elegir.\n")
    sys.exit(0)
