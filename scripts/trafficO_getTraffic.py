import os
import time
import sys
import cv2
import numpy as np
import psycopg2
from pprint import pprint
import requests
import datetime
import dateutil.tz
from pytz import timezone

def getImageEnds(extremos):
    x1 = float(extremos[:10]) 
    y1 = float(extremos[11:21])
    x2 = float(extremos[22:32])
    y2 = float(extremos[33:43])
    x3 = float(extremos[44:54])
    y3 = float(extremos[55:65])
    x4 = float(extremos[66:76])
    y4 = float(extremos[77:87])   
    return (x1,y1,x2,y2,x3,y3,x4,y4)

#Para desarrollo
def getPosition(latM,lonM,x1,y1,x2,y2,x3,y3,x4,y4):
##pos prueba -12.060130, -77.072353 Comisaria Palomino
    #print("Lat y Lon", latM, lonM)
    posy = round((2000*abs(latM-x1))/(abs(x2-x1)))
    #print("posy: ",posy)
    if(posy < 0): posy =0
    elif (posy > 2000): posy = 2000
    #print("posy: " + str(posy))    
    posx = round((2000*abs(lonM-y3))/(abs(y2-y3)))
    #print("posx: ",posx)
    if(posx < 0): posx =0
    elif (posx > 2000): posx = 2000
    #print("posx: " + str(posx)) 
    return posx,posy

def getPositionCrop(posx,posy,const):
    izqx = 0
    izqy = 0
    izqx = posx + const
    izqy = posy - const
    if(izqy < 0): izqy = 0
    elif (izqx > 2000): izqx = 2000
    #print("Pos izquierda")
    #print(izqx,izqy)
    derx = 0
    dery = 0
    derx = posx - const
    dery = posy + const
    if(derx < 0): derx = 0
    elif (dery > 2000): dery = 2000
    #print("Pos derecha")
    #print(derx,dery)
    return (derx,izqx,izqy,dery)

def crop_image(pos_ini_x, pos_fin_x, pos_ini_y, pos_fin_y, img_name):
    #print(pos_ini_x,pos_fin_x,pos_ini_y,pos_fin_y)
    crop_image = img_name[int(pos_ini_x):int(pos_fin_x), int(pos_ini_y):int(pos_fin_y)]
    return crop_image

def cvtColor_image(crop_image):
    #print("en cvtcolor")
    gray_image = cv2.cvtColor(crop_image, cv2.COLOR_BGR2GRAY)
    return gray_image

def substract(img_with, img_without):
    #print("en substract")
    np_subtr = np.subtract(img_with,img_without)
    return np_subtr

def color_position(x1,y1,np_subtr):
    #print("en color_position")
    colors = []
    positions_colors = []
    for i in range(x1):
        for j in range(y1):
            k = np_subtr[i,j]
            if k > 0:
                colors.append(k)
                par = str(i) +","+ str(j)
                positions_colors.append(par)
    return positions_colors

# Get the pixel from the given image
def transform_pixel(original_img, i, j, substract_img):
    #print("en transform_pixel")
    x1, y1, z1 = original_img.shape
    if j > y1 or i > x1:
      return None
    # Get Pixel
    pixel = original_img[i,j]
    # Get R, G, B values (This are int from 0 to 255)
    red =   int(pixel[0])
    green = int(pixel[1])
    blue =  int(pixel[2])
    #print(pixel)
    flag_color=0
    if(red == 31 and green ==31 and blue ==129):
        #Label Rojo Intenso
        flag_color+=4
        gray = (red * 0.299) + (green * 0.587) + (blue * 0.114)
    elif(red == 50 and green == 60 and blue == 242):
        #Label Rojo
        flag_color+=3
        gray = (red * 0.299) + (green * 0.587) + (blue * 0.114)
    elif(red == 77 and green == 151 and blue == 255):
        #Label Naranja
        flag_color+=2
        gray = (red * 0.299) + (green * 0.587) + (blue * 0.114)
    elif(red == 104 and green == 214 and blue == 99):
        #Label Verde
        flag_color+=1
        gray = (red * 0.299) + (green * 0.587) + (blue * 0.114)
    else:
        gray = 0
    substract_img[i,j] = gray
    return flag_color

def clean_img(positions_colors, crop_image, subtr_image):
    #print("en clean img")
    green = 0.0
    orange = 0.0
    red = 0.0
    dark = 0.0
    all_colors = 0.0
    color_array=[]
    for i in range(len(positions_colors)):
        cadena = positions_colors[i]
        coma = cadena.find(",")
        x= int(cadena[0:coma])
        y= int(cadena[coma+1:])
        flag_color= transform_pixel(crop_image,x,y,subtr_image)
        if(flag_color==1):
            green += 1.0
        elif(flag_color==2):
            orange += 1.0
        elif(flag_color==3):
            red += 1.0
        elif(flag_color==4):
            dark += 1.0
    all_colors = green + orange + red + dark
    if(all_colors > 0):
       color_array.append(green/all_colors)
       color_array.append(orange/all_colors)
       color_array.append(red/all_colors)
       color_array.append(dark/all_colors)

    return color_array

def getLatLon(i):
    lat_start = i.find('')
    lat_end = i.find(',', lat_start)
    lat = i[lat_start:lat_end]
    lat= lat.strip()
    
    lon_start = i.find(',') + 1
    lon_end = i.find(' ', lon_start)
    lon = i[lon_start:lon_end]
    lon = lon.strip()
    
    return (lat,lon)
        

class DatabaseConnection:

  def __init__(self):
    try:
      self.connection = psycopg2.connect(
        "dbname='qairamap_db' user ='pucp-user' host='qairamap-db.c6xdvtbzawt6.us-east-1.rds.amazonaws.com' password='WLRn#Y7R' port='5432' ")
      self.connection.autocommit = True
      self.cursor = self.connection.cursor()
    except:
      pprint("Cannot connect to database")
  
  def create_table(self):
    create_table_command = "CREATE TABLE traffic(id serial PRIMARY KEY,latitude real, longitude real, Traffic_Green_1 real, Traffic_Orange_1 real, Traffic_Red_1 real, Traffic_Dark_1 real, Traffic_Green_2 real, Traffic_Orange_2 real, Traffic_Red_2 real, Traffic_Dark_2 real,data_time varchar(30))"
    self.cursor.execute(create_table_command)

  def select_traffic(self):
      self.cursor.execute("SELECT * FROM traffic")
      data= self.cursor.fetchall()
      for d in data:
        pprint("each traffic: {0}".format(d))

  def delete_data(self):
      self.cursor.execute("DELETE FROM traffic")

  def insert_new_record(self,lat, lon, color_array_1, color_array_2,address, district, comercial_name):
      var_address =address
      var_district= district
      var_comercial_name =comercial_name
      latitude = str(lat)
      longitude = str(lon)
      Traffic_Green_1 = str(color_array_1[0])
      Traffic_Orange_1 = str(color_array_1[1])
      Traffic_Red_1 = str(color_array_1[2])
      Traffic_Dark_1  = str(color_array_1[3])
      Traffic_Green_2 = str(color_array_2[0])
      Traffic_Orange_2 = str(color_array_2[1])
      Traffic_Red_2 = str(color_array_2[2])
      Traffic_Dark_2  = str(color_array_2[3])
      EST = timezone('America/Lima')
      ts2 = datetime.datetime.now()
      ts2 = ts2.replace(tzinfo=EST)
      horas_diferencia = datetime.timedelta(hours=5)
      tsfinal = ts2 - horas_diferencia
      val_time = tsfinal.strftime("%d-%b-%Y (%H:%M:%S.%f)")

      insert_command = "INSERT INTO traffic(latitude, longitude, Traffic_Green_1, Traffic_Orange_1, Traffic_Red_1, Traffic_Dark_1, Traffic_Green_2, Traffic_Orange_2, Traffic_Red_2, Traffic_Dark_2, data_time,address,district,comercial_name) VALUES('"+latitude+"','"+longitude+"','"+Traffic_Green_1+"','"+Traffic_Orange_1+"','"+Traffic_Red_1+"','"+Traffic_Dark_1+"','"+Traffic_Green_2+"','"+Traffic_Orange_2+"','"+Traffic_Red_2+"','"+Traffic_Dark_2+"','"+val_time+"','"+var_address+"','"+var_district+"','"+var_comercial_name+"')"
      self.cursor.execute(insert_command)

if __name__ == '__main__':

    database_connection = DatabaseConnection()
    #database_connection.drop_table()
    #database_connection.create_table()
    #database_connection.select_traffic()

    #f = open("/home/pucp-user/calidad-aire-2019/FilesTrafficOptimization/cercado_lima_map.txt", "r")
    f = open("/Users/lourdesmontalvo/Documents/Projects/calidad-aire-2019/FilesTrafficOptimization/cercado_lima_map.txt", "r")
    count = 1
    
    #false  -12.042640,-77.064328  
    posicion_modulos = {1:['-12.034540,-77.062328','-12.054215,-77.077775','-12.059715,-77.063475'], 2:['-12.072954,-77.040410','-12.048015,-77.013553','-12.063844,-77.021389','-12.046344,-77.023189','-12.056644,-77.020189','-12.065730,-77.032220','-12.072097,-77.015162'],3:['-12.109722,-77.05194444'],4:['-12.095461,-76.996249']}
    #posicion_modulos = {1: ['-12.067203,-77.064102'],2:['-12.062730,-77.037520','-12.069854,-77.046410'],3:['-12.072717,-77.004118'],4:['-12.050689,-77.076595'],5:['-12.040081,-77.047481','-12.029156,-77.055903','-12.048042,-77.043804'],6:['-12.043015,-77.013553','-12.053544,-77.023954']}
    
    #true
    modulos = {1:['-12.048042,-77.043804','-12.058715,-77.071775','-12.046579,-77.080374'], 2:['-12.070761,-77.043170','-12.040207,-77.016394','-12.052444,-77.033389','-12.056810,-77.016193','-12.050295,-77.026060','-12.063204,-77.035967','-12.045097,-77.042562'],3:['-12.109722,-77.05194444'],4:['-12.103056,-76.98916667']}
    #modulos = {1: ['-12.058715,-77.071775'],2:['-12.063204,-77.035967','-12.070761,-77.043170'],3:['-12.056810,-77.016193'],4:['-12.046579,-77.080374'],5:['-12.045097,-77.042562','-12.052444,-77.033389','-12.044413,-77.051146'],6:['-12.040207,-77.016394','-12.050295,-77.026060']}
    
    nombre_modulos = {1:['Base Serenazgo','Colegio Kennedy','SAT Deposito'], 2:['Campo Marte Senamhi','ExSetamen','Gruta Lourdes','Colegio Virgo Potens','Mercado Ramon Castilla','Parque Exposicion','Velatorio RamonCastilla'],3:['Complejo Deportivo Manuel Bonilla'],4:['Parque de la Felicidad']}
    #nombre_modulos = {1: ['Colegio_Kennedy'],2:['Parque_Exposicion','Campo_Marte_Senamhi'],3:['Colegio_Virgo_Potens'],4:['SAT_Deposito'],5:['Velatorio_RamonCastilla','Gruta_Lourdes','Base_Serenazgo'],6:['ExSetamen','Mercado_RamonCastilla']}
    
    address={1:['Sanchez Pinillos 170','Urb. Palomino','Av Argentina 2926'],2:['Jesús María 15072','Vía Evitamiento km 6.5','Plaza de la Democracia','Parque Historia de la Medicina Peruana, S/N, Av. Miguel Grau 13','Jr Ayacucho con Jr Ucuyali','Av. 28 de Julio','Avenida Alfonso Ugarte 168'],3:['Av. del Ejercito 1300'],4:['Parque Nro 2 Javier Prado']}

    district={1:['Cercado de Lima','Cercado de Lima','Cercado de Lima'],2:['Jesus Maria','Cercado de Lima','Cercado de Lima', 'Cercado de Lima', 'Cercado de Lima','Cercado de Lima','Cercado de Lima'],3:['Miraflores'],4:['San Borja']}

    for l in f:
      nmapa_start = l.find('Mapa ') + 5
      nmapa_end = l.find('-', nmapa_start)
      nmapa = l[nmapa_start:nmapa_end]
      nmapa= nmapa.strip()

      poscentral_start = l.find('Pos') + 18
      poscentral_end = l.find(')', poscentral_start)
      poscentral = l[poscentral_start:poscentral_end]
      poscentral= poscentral.strip()
      
      extremos_start = l.find('Extremo') + 32
      extremos_end = l.find(')', extremos_start)
      extremos = l[extremos_start:extremos_end]
      extremos= extremos.strip()

      x1,y1,x2,y2,x3,y3,x4,y4 = getImageEnds(extremos)
    
      #im_crop_with = cv2.imread('/home/pucp-user/calidad-aire-2019/ImagesTrafficOptimization/traffic_optimization_with_'+ nmapa +'.png')
      #im_crop_without = cv2.imread('/home/pucp-user/calidad-aire-2019/ImagesTrafficOptimization/traffic_optimization_without_'+nmapa +'.png')
      
      im_crop_with = cv2.imread('/Users/lourdesmontalvo/Documents/Projects/calidad-aire-2019/ImagesTrafficOptimization/traffic_optimization_with_'+ nmapa +'.png')
      im_crop_without = cv2.imread('/Users/lourdesmontalvo/Documents/Projects/calidad-aire-2019/ImagesTrafficOptimization/traffic_optimization_without_'+nmapa +'.png')
      
      nromapa = int(nmapa)
      l =0
      for i in posicion_modulos[nromapa]:
          (lat,lon) = getLatLon(i)  
        
          (x,y) = getPosition(float(lat),float(lon),x1,y1,x2,y2,x3,y3,x4,y4)
          (izqx1,izqy1,derx1,dery1) = getPositionCrop(x,y,300)
          (izqx2,izqy2,derx2,dery2) = getPositionCrop(x,y,200)
        
          image_with_crop1 = crop_image(izqx1,izqy1,derx1,dery1,im_crop_with)
          image_without_crop1 = crop_image(izqx1,izqy1,derx1,dery1,im_crop_without)    
          image_with_crop2 = crop_image(izqx2,izqy2,derx2,dery2,im_crop_with)
          image_without_crop2 = crop_image(izqx2,izqy2,derx2,dery2,im_crop_without)
          
          #cv2.imwrite('/home/pucp-user/calidad-aire-2019/ImagesTrafficOptimization/crop_1_'+nombre_modulos[nromapa][l]+'.png',image_with_crop1)
          #cv2.imwrite('/home/pucp-user/calidad-aire-2019/ImagesTrafficOptimization/crop_2_'+nombre_modulos[nromapa][l]+'.png',image_with_crop2)
          
          cv2.imwrite('/Users/lourdesmontalvo/Documents/Projects/calidad-aire-2019/ImagesTrafficOptimization/crop_1_'+nombre_modulos[nromapa][l]+'.png',image_with_crop1)
          cv2.imwrite('/Users/lourdesmontalvo/Documents/Projects/calidad-aire-2019/ImagesTrafficOptimization/crop_2_'+nombre_modulos[nromapa][l]+'.png',image_with_crop2)

          img_cvt_with1 = cvtColor_image(image_with_crop1)
          img_cvt_without1 = cvtColor_image(image_without_crop1)
          img_cvt_with2 = cvtColor_image(image_with_crop2)
          img_cvt_without2 = cvtColor_image(image_without_crop2)

          img_substract1 = substract(img_cvt_with1,img_cvt_without1)
          img_substract2 = substract(img_cvt_with2,img_cvt_without2)

          (x1_substract,y1_substract) = img_substract1.shape
          (x2_substract,y2_substract) = img_substract2.shape

          positions_color1 = color_position(x1_substract, y1_substract,img_substract1)
          positions_color2 = color_position(x2_substract, y2_substract,img_substract2)

          color_array1 = clean_img(positions_color1, image_with_crop1, img_substract1)
          color_array2 = clean_img(positions_color2, image_with_crop2, img_substract2)

          (latReal,lonReal) = getLatLon(modulos[nromapa][l]) 
          print(latReal,lonReal)
          database_connection.insert_new_record(latReal,lonReal,color_array1,color_array2,address[nromapa][l],district[nromapa][l],nombre_modulos[nromapa][l])
          print(str(l))
          l+=1
      

    #database_connection.select_traffic()

