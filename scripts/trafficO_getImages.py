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
import math

def crop_image(width, height, img_name):
    image = cv2.imread('/home/pucp-user/calidad-aire-2019/ImagesTrafficOptimization/'+img_name, cv2.IMREAD_COLOR)
    #image = cv2.imread('/Users/lourdesmontalvo/Documents/Projects/calidad-aire-2019/ImagesTrafficOptimization/'+img_name, cv2.IMREAD_COLOR)
    nuevo_y = height-55
    crop_image = image[55:nuevo_y, 0:width]
    cv2.imwrite('/home/pucp-user/calidad-aire-2019/ImagesTrafficOptimization/'+img_name,crop_image)
    #cv2.imwrite('/Users/lourdesmontalvo/Documents/Projects/calidad-aire-2019/ImagesTrafficOptimization/'+img_name,crop_image)
    return crop_image

if __name__ == '__main__':
    count = 1
    centros = {1 : ['-12.048771','-77.067287'], 2 : ['-12.058735','-77.028479'],3:['-12.097553','-77.043421'],4:['-12.101369','-77.005875'],5:['-12.030802','-76.965465'],6:['-12.026741','-76.918568'],7:['-12.026741','-76.918568'],8:['-12.026741','-76.918568']}
    
    #centros = {1 : ['-12.070475','-77.075608'], 2 : ['-12.071398','-77.045739'], 3: ['-12.071566','-77.014325'],4 : ['-12.042020','-77.071488'],5 : ['-12.042020','-77.044623'],6 : ['-12.041600','-77.015269']}
    extremos = {1: ['-12.070131,-77.088488','-12.027891,-77.088674','-12.028428,-77.044687','-12.070043,-77.045827'],2:['-12.079281,-77.048983','-12.037726,-77.049984','-12.037948,-77.006218','-12.079610,-77.007128'],3:['-12.121283,-77.064092','-12.077279,-77.064177','-12.076842,-77.020814','-12.121232,-77.020625'],4:['-12.121835,-77.015297','-12.077883,-77.014012','-12.078245,-76.974072','-12.122199,-76.973950'],5:['-12.121835,-77.015297','-12.077883,-77.014012','-12.078245,-76.974072','-12.122199,-76.973950'],6:['-12.121835,-77.015297','-12.077883,-77.014012','-12.078245,-76.974072','-12.122199,-76.973950'],7:['-12.121835,-77.015297','-12.077883,-77.014012','-12.078245,-76.974072','-12.122199,-76.973950'],8:['-12.121835,-77.015297','-12.077883,-77.014012','-12.078245,-76.974072','-12.122199,-76.973950']}
    #extremos = {1: ['-12.089115,-77.095045','-12.051735,-77.095201','-12.052420,-77.056391','-12.085726,-77.055846'],2:['-12.090182,-77.065563','-12.052839,-77.065251','-12.052001,-77.026363','-12.091210,-77.027052'],3:['-12.090672,-77.033484','-12.052622,-77.033109','-12.052507,-76.997259','-12.090823,-76.995302'],4:['-12.060869,-77.089344','-12.023183,-77.090446','-12.023109,-77.051734','-12.061788,-77.052815'],5:['-12.061277,-77.063514','-12.023038,-77.063627','-12.023242,-77.026007','-12.060173,-77.024578'],6:['-12.059774,-77.033891','-12.022833,-77.034269','-12.023992,-76.996289','-12.060921,-76.996276']

    os.remove("/home/pucp-user/calidad-aire-2019/FilesTrafficOptimization/cercado_lima_map.txt")
    f = open("/home/pucp-user/calidad-aire-2019/FilesTrafficOptimization/cercado_lima_map.txt", "a")
    #os.remove("/Users/lourdesmontalvo/Documents/Projects/calidad-aire-2019/FilesTrafficOptimization/cercado_lima_map.txt")
    #f = open("/Users/lourdesmontalvo/Documents/Projects/calidad-aire-2019/FilesTrafficOptimization/cercado_lima_map.txt", "a")
    for i in range(1, 9):
        for j in range(count, count+2):
            strPhantomCommand = 'phantomjs /home/pucp-user/calidad-aire-2019/FilesTrafficOptimization/googlemaps{}.js'.format(j)
            #strPhantomCommand = 'phantomjs /Users/lourdesmontalvo/Documents/Projects/calidad-aire-2019/FilesTrafficOptimization/googlemaps{}.js'.format(j)
            print(strPhantomCommand)
            os.system(strPhantomCommand)

        crop_with = crop_image(2000,2110,'traffic_optimization_with_{}.png'.format(i))
        crop_without = crop_image(2000,2110,'traffic_optimization_without_{}.png'.format(i))
        f.write("Mapa "+ str(i)+" - Posicion Central ("+ str(centros[i][0])+","+str(centros[i][1])+") - Extremo (Izq Inf hacia arriba: ("+str(extremos[i][0])+";"+str(extremos[i][1])+";"+str(extremos[i][2])+";"+str(extremos[i][3])+")")
        f.write("\n")
        count += 2
    f.close()
