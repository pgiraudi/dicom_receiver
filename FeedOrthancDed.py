#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
from requests.auth import HTTPBasicAuth
import mysql.connector
import time as lapsus
import os
import sys
from datetime import datetime, time
import pydicom

laststudy_id = None
"""Este script requiere carpeta montada del storage
por lo tanto se ubica en el directorio de la aplicación web
 (/public/wwwzipndwnld/ )que ya las tiene montadas"""
def registrar(folder,dicom_img_folder):
    if os.path.isdir(folder):
        listarchivos=os.listdir(folder)
        cantarchivos=len(listarchivos)
        counter=0
        print("Carpeta leida: "+folder+" "+str(cantarchivos)+" archivos")
        print(datetime.now())
        sys.stdout.flush()
        for file in listarchivos:
            relpath=os.path.normpath(folder+'/'+file)
            if counter==cantarchivos-1:
                UploadFile(relpath,True,dicom_img_folder)
            else:
                UploadFile(relpath, False,dicom_img_folder)
            counter+=1
    else:
        print("La carpeta "+folder+" no existe")

def UploadBuffer(dicom,doregister,dicom_img_folder):
    IMPORTED_STUDIES=set()
    COUNT_ERROR=0
    COUNT_SUCCESS=0
    auth = HTTPBasicAuth('pmurad', 'felipe')
    url='http://149.50.159.62:8042'
    r = requests.post('%s/instances' % url, auth=auth, data=dicom)
    try:
        r.raise_for_status()
        info = r.json()
        COUNT_SUCCESS += 1
    except:
          COUNT_ERROR += 1
          """print('  not a valid DICOM file, ignoring it')"""
    if not info['ParentStudy'] in IMPORTED_STUDIES:
        IMPORTED_STUDIES.add(info['ParentStudy'])
        r2 = requests.get('%s/instances/%s/tags?short' % (url, info['ID']), auth=auth)
        r2.raise_for_status()
        tags = r2.json()
        if doregister:
            if '0020,000d' in tags:
                study_inst_id=tags['0020,000d']
            else:
                study_inst_id='no disponible'
            registrar_data_orthanc(dicom_img_folder,info['ParentPatient'],info['ParentStudy'],study_inst_id)
            print(u'Envio registrado '+dicom_img_folder)
            sys.stdout.flush()
        """print('')
        print('New imported study:')
        print('  Orthanc ID of the patient: %s' % info['ParentPatient'])
        print('  Orthanc ID of the study: %s' % info['ParentStudy'])
        print('  DICOM Patient ID: %s' % (
            tags['0010,0020'] if '0010,0020' in tags else '(empty)'))
        print('  DICOM Study Instance UID: %s' % (
            tags['0020,000d'] if '0020,000d' in tags else '(empty)'))
        print('')"""

def Conectar():
    try:
        db = mysql.connector.connect(host='192.168.0.240', user='mantenimiento', password='cambio18',
                                     database='clinik', port=3306, use_unicode=True, buffered=True)
        return db
    except mysql.connector.Error as err:
        msg = err.msg
        print(u'El servidor de Centro Roca en 192.168.0.240 dice:\n' + msg,
                      u"Problemas al conectarse al servidor")

def UploadFile_sin_filtro(file,doregister,dicom_img_folder):
    with open(file, 'rb') as f:
        dicom = f.read()
        """print('Uploading: %s (%dMB)' % (self, len(dicom) / (1024 * 1024)))"""
        UploadBuffer(dicom,doregister,dicom_img_folder)


def UploadFile(file, doregister, dicom_img_folder): # Filtrando de MG toda serie que comience la descripción con "3D_"
    try:
        ds = pydicom.dcmread(file, stop_before_pixels=True)

        modality = ds.get("Modality", "")
        if isinstance(modality, bytes):
            modality = modality.decode(errors="ignore")

        # Aplicar filtro sólo si es mamografía
        if modality == "MG":
            series_desc = ds.get("SeriesDescription", "")
            if isinstance(series_desc, bytes):
                series_desc = series_desc.decode(errors="ignore")

            # Filtrar series 3D
            if series_desc.startswith("3D_"):
                print(f"Saltando serie 3D (MG): {series_desc} en archivo {file}")
                return  # No subir este archivo
    except Exception as e:
        print(f"No se pudo leer DICOM para filtro (se enviará igual): {file}  Error: {e}")

    # Si no corresponde filtrar o no es MG, proceder normalmente
    with open(file, 'rb') as f:
        dicom = f.read()
        UploadBuffer(dicom, doregister, dicom_img_folder)



def registrar_data_orthanc(folder, patient_id, study_id, study_inst_id):
    odb = Conectar()
    crs = odb.cursor()
    sqlupdate = """update dicom_img set www='CL',equipo=CONCAT(equipo,'~'), orth_patient_id='""" + patient_id + """', orth_study_id='
""" + study_id + """',orth_study_inst_id='""" + study_inst_id + """',orth_envio=curdate() where carpeta='""" + folder + """'"""
    crs.execute(sqlupdate)
    crs.close()
    odb.close()
    print("Registrado: "+folder)

def consultar_nuevos():
    odb=Conectar()
    crs = odb.cursor()
    ahora=datetime.now().time()
    hora_referencia=time (1,0)

    if ahora>hora_referencia:
        # Todo lo demas envío inmediatamente con delay de una hora
        sqlselect="""select carpeta,CONCAT(stor,carpeta) as fpath,id,nombre,fecha,modality from dicom_img where 
        modality<>'BMD' and modality<>'SR' and www='NO' and modality<>'CT' and modality<>'MR' and modality<> 'MG' and  
        carpeta<>'estudio_no_dicom' and fecha>='2025-08-06' and hora<DATE_ADD(now(),interval -60 minute) order by fecha, hora"""
    else:
        # todo lo pendiente + Tomografias y resonancias y mamografia envío a las 1 de la mañana (hora de referencia)
        sqlselect="""select carpeta,CONCAT(stor,carpeta) as fpath,id,nombre,fecha,modality from dicom_img where  
        modality<>'SR' and  modality<>'BMD' and www='NO' and carpeta<>'estudio_no_dicom' and fecha>='2025-08-06' order by id"""

    crs.execute(sqlselect)
    rows=crs.fetchall()
    crs.close()
    odb.close()

    if rows is not None:
        for row in rows:
            carpeta=row[0]
            storagefullpath = row[1].replace('\\', '/').split('/', 3)[3]
            storagefullpath =storagefullpath.replace('dicom_6','DICOM_6') #adaptado a la carpeta montada en este servidor
            print(u'Iniciando ' + row[3] + u' del ' + str(row[4]) + u' modality:' + row[5]+' '+storagefullpath)
            registrar(storagefullpath, carpeta)


INTERVALO = 30  # segundos

try:
    while True:
        sys.stdout.flush()
        for i in range(INTERVALO, 0, -1):
            #sys.stdout.write("\rVolviendo a enviar estudios en... %2d segundos" % i)
            #sys.stdout.flush()
            lapsus.sleep(1)
        #sys.stdout.write("\rEnviando estudios...                           ")
        #sys.stdout.flush()
        consultar_nuevos()
except KeyboardInterrupt:
    sys.stdout.write("\nEjecución interrumpida por el usuario.\n")


