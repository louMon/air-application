
def evaluar_interpolacion_conjunto_de_datos(conjunto_de_datos_interpolacion_espacial, indice_columna_interpolacion, 
                                            indice_columna_coordenadas_x, indice_columna_coordenadas_y, 
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
        
        valores_variable_interpolacion_interpolados.append(valor_variable_interpolacion_interpolado)
        
    #mostrar_diferencia_valores_reales_interpolados_dispersion(valores_variable_interpolacion_reales, valores_variable_interpolacion_interpolados)
    valores_variable_interpolacion_reales = np.array(valores_variable_interpolacion_reales)
    valores_variable_interpolacion_interpolados = np.array(valores_variable_interpolacion_interpolados)
    
    puntuacion_rmse = obtener_rmse(valores_variable_interpolacion_reales, valores_variable_interpolacion_interpolados)
    
    print('{:8.3f}'.format(puntuacion_rmse))
    
    return (puntuacion_rmse, valores_variable_interpolacion_reales, valores_variable_interpolacion_interpolados)



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

def main_function(NOMBRE_COLUMNA_INTERPOLACION,NOMBRE_COLUMNA_COORDENADAS_X,NOMBRE_COLUMNA_COORDENADAS_Y,
				  NOMBRE_MODELO, lista_conjuntos_de_datos_interpolacion_espacial):
	#NOMBRE_COLUMNA_INTERPOLACION = 'CO'
	#NOMBRE_COLUMNA_COORDENADAS_X = 'lon'
	#NOMBRE_COLUMNA_COORDENADAS_Y = 'lat'
	#NOMBRE_MODELO = 'IDW'

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
