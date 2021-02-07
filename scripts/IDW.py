import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score
from sklearn.preprocessing import Normalizer
from matplotlib import pyplot
from pykrige.ok import OrdinaryKriging
from pykrige.uk import UniversalKriging
from scipy.interpolate import RegularGridInterpolator
from scipy.interpolate import NearestNDInterpolator
from os import listdir

def encontrar_nombres_archivos_csv(sufijo=".csv" ):
    nombres_de_archivos = listdir()
    return [ nombre_de_archivo for nombre_de_archivo in nombres_de_archivos if nombre_de_archivo.endswith( sufijo ) ]

nombres_de_archivos_csv = encontrar_nombres_archivos_csv()
len(nombres_de_archivos_csv)

lista_conjuntos_de_datos_interpolacion_espacial = []
for nombre_de_archivo_csv in nombres_de_archivos_csv:
    conjunto_de_datos_interpolacion_espacial = pd.read_csv(nombre_de_archivo_csv)
    lista_conjuntos_de_datos_interpolacion_espacial.append(conjunto_de_datos_interpolacion_espacial)
    
len(lista_conjuntos_de_datos_interpolacion_espacial)

lista_conjuntos_de_datos_interpolacion_espacial[0]

lista_conjuntos_de_datos_interpolacion_espacial[0]

for i in range(len(lista_conjuntos_de_datos_interpolacion_espacial)):
    lista_conjuntos_de_datos_interpolacion_espacial[i] = lista_conjuntos_de_datos_interpolacion_espacial[i].drop(['Unnamed: 0', 'id', 'qhawax_id', 'timestamp_zone'], axis=1)


lista_diccionario_columnas_indice = []
for conjunto_de_datos_interpolacion_espacial in lista_conjuntos_de_datos_interpolacion_espacial:
    columnas_conjunto_de_datos_interpolacion_espacial = list(conjunto_de_datos_interpolacion_espacial.columns)
    diccionario_columnas_indice = {}
    
    for i in range(len(columnas_conjunto_de_datos_interpolacion_espacial)):
        diccionario_columnas_indice[columnas_conjunto_de_datos_interpolacion_espacial[i]] = i
        
    lista_diccionario_columnas_indice.append(diccionario_columnas_indice)
    
print(len(lista_diccionario_columnas_indice))

for diccionario_columnas_indice in lista_diccionario_columnas_indice:
    print(diccionario_columnas_indice)
    print(len(diccionario_columnas_indice))


normalizer = Normalizer().fit(lista_conjuntos_de_datos_interpolacion_espacial[0])

conjuntos_de_datos_interpolacion_espacial_normalizado = normalizer.transform(lista_conjuntos_de_datos_interpolacion_espacial[0])

df_conjuntos_de_datos_interpolacion_espacial_normalizado = pd.DataFrame(conjuntos_de_datos_interpolacion_espacial_normalizado, columns=lista_diccionario_columnas_indice[0].keys())

df_conjuntos_de_datos_interpolacion_espacial_normalizado


columnas_normalizar = ['CO', 'H2S', 'NO2', 'O3', 'PM25', 'PM10', 'SO2']

normalizer = Normalizer().fit(lista_conjuntos_de_datos_interpolacion_espacial[0][columnas_normalizar])

conjuntos_de_datos_interpolacion_espacial_normalizado = normalizer.transform(lista_conjuntos_de_datos_interpolacion_espacial[0][columnas_normalizar])

df_conjuntos_de_datos_interpolacion_espacial_normalizado = pd.DataFrame(conjuntos_de_datos_interpolacion_espacial_normalizado, columns=columnas_normalizar)

df_conjuntos_de_datos_interpolacion_espacial_normalizado['lat'] = lista_conjuntos_de_datos_interpolacion_espacial[0]['lat']

df_conjuntos_de_datos_interpolacion_espacial_normalizado['lon'] = lista_conjuntos_de_datos_interpolacion_espacial[0]['lon']

df_conjuntos_de_datos_interpolacion_espacial_normalizado

df_conjuntos_de_datos_interpolacion_espacial_normalizado.describe()


lista_conjuntos_de_datos_interpolacion_espacial_normalizado = []

for i in range(len(lista_conjuntos_de_datos_interpolacion_espacial)):
    normalizer = Normalizer().fit(lista_conjuntos_de_datos_interpolacion_espacial[i][columnas_normalizar])
    
    conjuntos_de_datos_interpolacion_espacial_normalizado = normalizer.transform(lista_conjuntos_de_datos_interpolacion_espacial[i][columnas_normalizar])
    
    df_conjuntos_de_datos_interpolacion_espacial_normalizado = pd.DataFrame(conjuntos_de_datos_interpolacion_espacial_normalizado, columns=columnas_normalizar)
    
    df_conjuntos_de_datos_interpolacion_espacial_normalizado['lat'] = lista_conjuntos_de_datos_interpolacion_espacial[i]['lat']
    
    df_conjuntos_de_datos_interpolacion_espacial_normalizado['lon'] = lista_conjuntos_de_datos_interpolacion_espacial[i]['lon']
    
    lista_conjuntos_de_datos_interpolacion_espacial_normalizado.append(df_conjuntos_de_datos_interpolacion_espacial_normalizado)
    
lista_conjuntos_de_datos_interpolacion_espacial_normalizado[0].head()


def matriz_de_distancias(x0, y0, x1, y1):
    observado = np.vstack((x0, y0)).T
    interpolado = np.vstack((x1, y1)).T

    # Realizar una matriz de distancias entre observaciones por pares
    d0 = np.subtract.outer(observado[:,0], interpolado[:,0])
    d1 = np.subtract.outer(observado[:,1], interpolado[:,1])

    return np.hypot(d0, d1)

def obtener_interpolacion_idw(x, y, z, xi, yi):
    distancias = matriz_de_distancias(x,y, xi,yi)

    # En IDW, los pesos son la inversa de las distancias
    pesos = 1.0 / distancias

    # Sumar todos los pesos a 1
    pesos /= pesos.sum(axis=0)

    # Multiplicar todos los pesos de cada punto interpolado por todos los valores de la variable a interpolar observados
    zi = np.dot(pesos.T, z)
    return zi


def obtener_interpolacion_idw(x, y, z, xi, yi):
    distancias = matriz_de_distancias(x,y, xi,yi)

    # En IDW, los pesos son la inversa de las distancias
    pesos = 1.0 / distancias

    # Sumar todos los pesos a 1
    pesos /= pesos.sum(axis=0)

    # Multiplicar todos los pesos de cada punto interpolado por todos los valores de la variable a interpolar observados
    zi = np.dot(pesos.T, z)
    return zi

def obtener_rmse(datos_reales, datos_predichos):
    return np.sqrt(mean_squared_error(datos_reales, datos_predichos))


def obtener_r2(datos_reales, datos_predichos):
    return r2_score(datos_reales, datos_predichos)


def mostrar_diferencia_valores_reales_interpolados(valores_variable_interpolacion_reales, valores_variable_interpolacion_interpolados):
    print('Valores Reales', 'Valores interpolados')
    for i in range(len(valores_variable_interpolacion_reales)):
        print(valores_variable_interpolacion_reales[i], valores_variable_interpolacion_interpolados[i])


def mostrar_diferencia_valores_reales_interpolados_dispersion(valores_variable_interpolacion_reales, valores_variable_interpolacion_interpolados):
    valores_variable_interpolacion_reales, valores_variable_interpolacion_interpolados = np.array(valores_variable_interpolacion_reales), np.array(valores_variable_interpolacion_interpolados)
    #pyplot.scatter(valores_variable_interpolacion_reales, valores_variable_interpolacion_interpolados)
    fig, ax = pyplot.subplots()
    ax.scatter(valores_variable_interpolacion_reales, valores_variable_interpolacion_interpolados)
    ax.plot([valores_variable_interpolacion_reales.min(), valores_variable_interpolacion_reales.max()], [valores_variable_interpolacion_reales.min(), valores_variable_interpolacion_reales.max()], 'k--', lw=4)
    ax.set_xlabel('Valores reales')
    ax.set_ylabel('Valores interpolados')
    #pyplot.xlabel("Valores reales")
    #pyplot.ylabel("Valores interpolados")
    pyplot.show()

def evaluar_interpolacion_conjunto_de_datos(conjunto_de_datos_interpolacion_espacial, 
                                            indice_columna_interpolacion, 
                                            indice_columna_coordenadas_x, 
                                            indice_columna_coordenadas_y, 
                                            nombre_metodo_interpolacion):
    coordenadas_x_conjunto_de_datos_interpolacion_espacial = conjunto_de_datos_interpolacion_espacial[:, indice_columna_coordenadas_x]
    coordenadas_y_conjunto_de_datos_interpolacion_espacial = conjunto_de_datos_interpolacion_espacial[:, indice_columna_coordenadas_y]
    valores_variable_interpolacion_conjunto_de_datos_interpolacion_espacial = conjunto_de_datos_interpolacion_espacial[:, indice_columna_interpolacion]
    
    valores_variable_interpolacion_reales = []
    valores_variable_interpolacion_interpolados = []
    
    numero_observaciones = len(conjunto_de_datos_interpolacion_espacial)
    
    for i in range(numero_observaciones):
        coordenada_x_observacion_a_evaluar = coordenadas_x_conjunto_de_datos_interpolacion_espacial[i]
        coordenada_y_observacion_a_evaluar = coordenadas_y_conjunto_de_datos_interpolacion_espacial[i]
        valor_variable_interpolacion_a_evaluar = valores_variable_interpolacion_conjunto_de_datos_interpolacion_espacial[i]
        
        valores_variable_interpolacion_reales.append(valor_variable_interpolacion_a_evaluar)
        
        coordenadas_x_conjunto_de_datos_interpolacion_espacial_observacion = np.delete(coordenadas_x_conjunto_de_datos_interpolacion_espacial, i)
        coordenadas_y_conjunto_de_datos_interpolacion_espacial_observacion = np.delete(coordenadas_y_conjunto_de_datos_interpolacion_espacial, i)
        valores_variable_interpolacion_conjunto_de_datos_interpolacion_espacial_observacion = np.delete(valores_variable_interpolacion_conjunto_de_datos_interpolacion_espacial, i)
        
        valor_variable_interpolacion_interpolado = 0.0
        
        if (nombre_metodo_interpolacion == 'IDW'):
        
            valor_variable_interpolacion_interpolado = obtener_interpolacion_idw(coordenadas_x_conjunto_de_datos_interpolacion_espacial_observacion, 
                                                                             coordenadas_y_conjunto_de_datos_interpolacion_espacial_observacion, 
                                                                             valores_variable_interpolacion_conjunto_de_datos_interpolacion_espacial_observacion, 
                                                                             coordenada_x_observacion_a_evaluar, 
                                                                             coordenada_y_observacion_a_evaluar)
        else:
            
            raise NotImplementedError
        
        valores_variable_interpolacion_interpolados.append(valor_variable_interpolacion_interpolado)
        
    #mostrar_diferencia_valores_reales_interpolados_dispersion(valores_variable_interpolacion_reales, valores_variable_interpolacion_interpolados)
    valores_variable_interpolacion_reales = np.array(valores_variable_interpolacion_reales)
    valores_variable_interpolacion_interpolados = np.array(valores_variable_interpolacion_interpolados)
    
    puntuacion_rmse = obtener_rmse(valores_variable_interpolacion_reales, valores_variable_interpolacion_interpolados)
    
    print('{:8.3f}'.format(puntuacion_rmse))
    
    return (puntuacion_rmse, valores_variable_interpolacion_reales, valores_variable_interpolacion_interpolados)



def repetir_evaluacion_interpolacion_conjuntos_de_datos(lista_conjunto_de_datos_interpolacion_espacial, 
                                            indice_columna_interpolacion, 
                                            indice_columna_coordenadas_x, 
                                            indice_columna_coordenadas_y, 
                                            nombre_metodo_interpolacion):
    
    puntuaciones = [evaluar_interpolacion_conjunto_de_datos(conjunto_de_datos_interpolacion_espacial, 
                                            indice_columna_interpolacion, 
                                            indice_columna_coordenadas_x, 
                                            indice_columna_coordenadas_y, 
                                            nombre_metodo_interpolacion) for conjunto_de_datos_interpolacion_espacial in lista_conjunto_de_datos_interpolacion_espacial]
    
    return puntuaciones


def obtener_dispersion_valores_reales_interpolados(listas_valores_variable_interpolacion_reales, listas_valores_variable_interpolacion_interpolados):
    valores_variable_interpolacion_reales = []
    valores_variable_interpolacion_interpolados = []
    indices = range(len(listas_valores_variable_interpolacion_reales))
    for i in indices:
        valores_variable_interpolacion_reales.extend(listas_valores_variable_interpolacion_reales[i])
        valores_variable_interpolacion_interpolados.extend(listas_valores_variable_interpolacion_interpolados[i])
        
    mostrar_diferencia_valores_reales_interpolados_dispersion(valores_variable_interpolacion_reales, valores_variable_interpolacion_interpolados)



def obtener_reporte_de_puntuaciones_del_modelo_evaluado(nombre_modelo_evaluado, puntuaciones_modelo_rmse):
    # print a summary
    #puntuaciones_modelo_rmse = [puntuacion[0] for puntuacion in puntuaciones_modelo]
    #puntuaciones_modelo_r2 = [puntuacion[1] for puntuacion in puntuaciones_modelo]
    
    media_puntuaciones_rmse, desviacion_estandar_puntuaciones_rmse = np.mean(puntuaciones_modelo_rmse), np.std(puntuaciones_modelo_rmse)
    print('%s: %.3f RMSE (+/- %.3f)' % (nombre_modelo_evaluado, media_puntuaciones_rmse, desviacion_estandar_puntuaciones_rmse))
    
    #media_puntuaciones_r2, desviacion_estandar_puntuaciones_r2 = np.mean(puntuaciones_modelo_r2), np.std(puntuaciones_modelo_r2)
    #print('%s: %.3f R2 (+/- %.3f)' % (nombre_modelo_evaluado, media_puntuaciones_r2, desviacion_estandar_puntuaciones_r2))
    
    # box and whisker plot
    pyplot.boxplot(puntuaciones_modelo_rmse)
    pyplot.show()


NOMBRE_COLUMNA_INTERPOLACION = 'CO'
NOMBRE_COLUMNA_COORDENADAS_X = 'lon'
NOMBRE_COLUMNA_COORDENADAS_Y = 'lat'
NOMBRE_MODELO = 'IDW'

indice_columna_interpolacion = lista_diccionario_columnas_indice[0][NOMBRE_COLUMNA_INTERPOLACION]
indice_columna_coordenadas_x = lista_diccionario_columnas_indice[0][NOMBRE_COLUMNA_COORDENADAS_X]
indice_columna_coordenadas_y = lista_diccionario_columnas_indice[0][NOMBRE_COLUMNA_COORDENADAS_Y]

lista_conjuntos_de_datos_interpolacion_espacial_valores = [conjunto_de_datos_interpolacion_espacial.values for conjunto_de_datos_interpolacion_espacial in lista_conjuntos_de_datos_interpolacion_espacial]
#lista_conjuntos_de_datos_interpolacion_espacial_valores = [conjunto_de_datos_interpolacion_espacial.values for conjunto_de_datos_interpolacion_espacial in lista_conjuntos_de_datos_interpolacion_espacial_normalizado]

puntuaciones = repetir_evaluacion_interpolacion_conjuntos_de_datos(lista_conjuntos_de_datos_interpolacion_espacial_valores, 
                                            indice_columna_interpolacion, 
                                            indice_columna_coordenadas_x, 
                                            indice_columna_coordenadas_y, 
                                            NOMBRE_MODELO)

puntuaciones_rmse = [puntuacion[0] for puntuacion in puntuaciones]

listas_valores_variable_interpolacion_reales = [puntuacion[1] for puntuacion in puntuaciones]

listas_valores_variable_interpolacion_interpolados = [puntuacion[2] for puntuacion in puntuaciones]

nombre_modelo_evaluado = NOMBRE_MODELO + ' - ' + NOMBRE_COLUMNA_INTERPOLACION

obtener_reporte_de_puntuaciones_del_modelo_evaluado(nombre_modelo_evaluado, puntuaciones_rmse)
obtener_dispersion_valores_reales_interpolados(listas_valores_variable_interpolacion_reales, listas_valores_variable_interpolacion_interpolados)



NOMBRE_COLUMNA_INTERPOLACION = 'CO'
NOMBRE_COLUMNA_COORDENADAS_X = 'lon'
NOMBRE_COLUMNA_COORDENADAS_Y = 'lat'
NOMBRE_MODELO = 'KRIGING'

indice_columna_interpolacion = lista_diccionario_columnas_indice[0][NOMBRE_COLUMNA_INTERPOLACION]
indice_columna_coordenadas_x = lista_diccionario_columnas_indice[0][NOMBRE_COLUMNA_COORDENADAS_X]
indice_columna_coordenadas_y = lista_diccionario_columnas_indice[0][NOMBRE_COLUMNA_COORDENADAS_Y]

lista_conjuntos_de_datos_interpolacion_espacial_valores = [conjunto_de_datos_interpolacion_espacial.values for conjunto_de_datos_interpolacion_espacial in lista_conjuntos_de_datos_interpolacion_espacial]

puntuaciones = repetir_evaluacion_interpolacion_conjuntos_de_datos(lista_conjuntos_de_datos_interpolacion_espacial_valores, 
                                            indice_columna_interpolacion, 
                                            indice_columna_coordenadas_x, 
                                            indice_columna_coordenadas_y, 
                                            NOMBRE_MODELO)

puntuaciones_rmse = [puntuacion[0] for puntuacion in puntuaciones]

listas_valores_variable_interpolacion_reales = [puntuacion[1] for puntuacion in puntuaciones]

listas_valores_variable_interpolacion_interpolados = [puntuacion[2] for puntuacion in puntuaciones]

nombre_modelo_evaluado = NOMBRE_MODELO + ' - ' + NOMBRE_COLUMNA_INTERPOLACION

obtener_reporte_de_puntuaciones_del_modelo_evaluado(nombre_modelo_evaluado, puntuaciones_rmse)
obtener_dispersion_valores_reales_interpolados(listas_valores_variable_interpolacion_reales, listas_valores_variable_interpolacion_interpolados)


NOMBRE_COLUMNA_INTERPOLACION = 'CO'
NOMBRE_COLUMNA_COORDENADAS_X = 'lon'
NOMBRE_COLUMNA_COORDENADAS_Y = 'lat'
NOMBRE_MODELO = 'NNIDW'

indice_columna_interpolacion = lista_diccionario_columnas_indice[0][NOMBRE_COLUMNA_INTERPOLACION]
indice_columna_coordenadas_x = lista_diccionario_columnas_indice[0][NOMBRE_COLUMNA_COORDENADAS_X]
indice_columna_coordenadas_y = lista_diccionario_columnas_indice[0][NOMBRE_COLUMNA_COORDENADAS_Y]

lista_conjuntos_de_datos_interpolacion_espacial_valores = [conjunto_de_datos_interpolacion_espacial.values for conjunto_de_datos_interpolacion_espacial in lista_conjuntos_de_datos_interpolacion_espacial]

puntuaciones = repetir_evaluacion_interpolacion_conjuntos_de_datos(lista_conjuntos_de_datos_interpolacion_espacial_valores, 
                                            indice_columna_interpolacion, 
                                            indice_columna_coordenadas_x, 
                                            indice_columna_coordenadas_y, 
                                            NOMBRE_MODELO)

puntuaciones_rmse = [puntuacion[0] for puntuacion in puntuaciones]

listas_valores_variable_interpolacion_reales = [puntuacion[1] for puntuacion in puntuaciones]

listas_valores_variable_interpolacion_interpolados = [puntuacion[2] for puntuacion in puntuaciones]

nombre_modelo_evaluado = NOMBRE_MODELO + ' - ' + NOMBRE_COLUMNA_INTERPOLACION

obtener_reporte_de_puntuaciones_del_modelo_evaluado(nombre_modelo_evaluado, puntuaciones_rmse)
obtener_dispersion_valores_reales_interpolados(listas_valores_variable_interpolacion_reales, listas_valores_variable_interpolacion_interpolados)
