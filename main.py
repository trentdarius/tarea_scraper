import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import random
from time import sleep
import sqlite3


def extractor_pagina():
    #aqui extraemos la info de las filas y columnas de la pagina
    tablas = driver.find_elements_by_xpath("//*[@id='A2248:form-visualizar:datosplantilla_data']/tr")
    TablaCol = driver.find_elements_by_xpath("//*[@id='A2248:form-visualizar:datosplantilla_data']/tr[1]/td")

    df_aux = pd.DataFrame()
    #aqui los segmentamos en cada variable para luego insertarla a la base de datos
    for a in tablas:
        ano = a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[1]/div').text
        mes = a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[2]/div').text
        estamento = a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[3]/div').text
        nombre_completo = a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[4]/div').text
        cargo_funcion = a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[5]/div').text
        grado = a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[6]/div').text
        calificacion_prof= a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[7]/div').text
        region= a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[8]/div').text
        asig_esp=a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[9]/div').text
        un_monetaria = a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[10]/div').text
        remuneracion_bruta= a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[11]/div').text
        montos_horas_diurnas = a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[12]/div').text
        montos_horas_nocturnas= a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[13]/div').text
        montos_horas_festivas= a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[14]/div').text
        fecha_inicio = a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[15]/div').text
        fecha_termino = a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[16]/div').text
        declaracion_intereses = a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[17]/div').text
        viaticos = a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[18]/div').text
        observaciones = a.find_element_by_xpath('//*[@id="A2248:form-visualizar:datosplantilla_data"]/tr/td[19]/div').text

        df = {'ano': ano, 'mes': mes, 'estamento': estamento, 'nombre_completo': nombre_completo, 'cargo': cargo_funcion,
              'grado': grado, 'calificacion profesional': calificacion_prof, 'region': region,
              'asignaciones_especiales': asig_esp, 'unida_monetaria': un_monetaria,
              'remuneracion_bruta': remuneracion_bruta,
              'monto_hrs_diurnas': montos_horas_diurnas, 'monto_hrs_nocturnas': montos_horas_nocturnas,
              'monto_hrs_festivas': montos_horas_festivas, 'fecha_inicio': fecha_inicio,
              'fecha_termino': fecha_termino, 'declaracion_intereses': declaracion_intereses, 'viaticos': viaticos, 'observaciones' : observaciones}
        df_aux = df_aux.append(pd.DataFrame.from_records([df]))

    return  df_aux

df_final = pd.DataFrame()

#personal a contrata
options = Options()
options.add_argument("start-maximized")
driver = webdriver.Chrome(chrome_options=options, executable_path='chromedriver.exe')
driver.get('https://www.portaltransparencia.cl/PortalPdT/pdtta/-/ta/AO096/PR/PCONT/50719041')


for i in range(5):
    if i == 0:
        df_final = df_final.append(extractor_pagina())
        print(len(df_final))
    else:
        try:
            boton =driver.find_element_by_xpath("//*[@id='A2248:form-visualizar:datosplantilla_paginator_top']/span[3]/span["+str(i+1)+"]")
            sleep(3)
            boton.click()
            sleep(10)
            df_final = df_final.append(extractor_pagina())
            print(len(df_final))

        except:
            break

# CREAMOS LA CONEXION A LA BD SQLITE3
con = sqlite3.connect("bd_scraper_juan_perez.sqlite3")

consulta = con.cursor()

# SENTENCIA DE CREACION DE TABLA
sentencia_sql ="""
CREATE TABLE IF NOT EXISTS remuneracion_personal_contrata(
id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
ano VARCHAR(4) NOT NULL,
mes VARCHAR(13),
estamento VARCHAR(500),
nombre_completo VARCHAR(2000),
cargo_funcion VARCHAR(300),
grado VARCHAR(15),
calificacion_profesional VARCHAR(300),
region VARCHAR(150),
asignaciones_especiales VARCHAR(15),
unidad_monetaria VARCHAR(15),
remuneracion_bruta VARCHAR(15),
monto_hrs_diurnas VARCHAR(20),
monto_hrs_nocturnas VARCHAR(20),
monto_hrs_festivas VARCHAR(20),
fecha_inicio VARCHAR(15),
fecha_termino VARCHAR(15),
declaracion_intereses VARCHAR(20),
viaticos VARCHAR(20),
observaciones VARCHAR(150)
)
"""

if (consulta.execute(sentencia_sql)):
    print('Tabla creada con exito')
else:
    print('Ha ocurrido un error')

# INSERT DE LOS RESULTADOS OBTENIDOS A LA BD
for index, row in df_final.iterrows():


    valores = (row['ano'], row['mes'], row['estamento'], row['nombre_completo'], row['cargo_funcion'], row['grado']
               , row['calificacion_profesional'], row['region'], row['asignaciones_especiales'], row['unidad_monetaria']
               , row['remuneracion_bruta'], row['monto_hrs_diurnas'], row['monto_hrs_nocturnas'], row['monto_hrs_festivas']
               , row['fecha_inicio'], row['fecha_termino'], row['declaracion_intereses'], row['viaticos']
               , row['observaciones'])
    insert_sentencia = """
    INSERT INTO remuneracion_personal_contrata(ano, mes, estamento, nombre_completo, cargo_funcion, grado, calificacion_profesional, region,
    asignaciones_especiales, unidad_monetaria, remuneracion_bruta, monto_hrs_diurnas, monto_hrs_nocturnas, monto_hrs_festivas,
    fecha_inicio, fecha_termino, declaracion_intereses, viaticos, observaciones)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    if (consulta.execute(insert_sentencia, valores)):
        print('Registro Ingresado con Exito')
    else:
        print('Error en ingreso de registro')

# CIERRE DE LA CONSULTA Y COMMIT CON LOS CAMBIOS HACIA LA BD SQLITE3
consulta.close()
con.commit()