#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from pynetdicom import AE, evt
from pynetdicom import StoragePresentationContexts
from pathlib import Path
import mysql.connector

STOR = "\\\\\\\\192.168.0.150\\\\dicom_6\\\\e_dicom\\\\"

class DICOMReceiver:
    def __init__(self, ae_title, port, output_dir, log_file):
        self.ae_title = ae_title
        self.port = port
        self.output_dir = output_dir
        self.log_file = log_file
        os.makedirs(self.output_dir, exist_ok=True)

        # Initialize logging
        self.logger = logging.getLogger(f"DICOMReceiver:{ae_title}:{port}")
        self.logger.setLevel(logging.INFO)

        # Avoid duplicate handlers in case of multiple instances
        if not self.logger.hasHandlers():
            #formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
            formatter = logging.Formatter(f'[%(asctime)s] [{self.ae_title}@{self.port}] %(levelname)s - %(message)s')

            # Console handler
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

            """# File handler
            fh = logging.FileHandler(self.log_file)
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)"""
            
            # Rotating file handler: 5 MB por archivo, máximo 5 archivos
            fh = RotatingFileHandler(
                self.log_file, maxBytes=5 * 1024 * 1024, backupCount=5
            )
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

        # Initialize AE
        self.ae = AE(ae_title=self.ae_title)

        # Add storage presentation contexts
        for context in StoragePresentationContexts:
            self.ae.add_supported_context(context.abstract_syntax)

        # Setup event handlers
        self.handlers = [
            (evt.EVT_C_STORE, self.handle_store),
            (evt.EVT_ACCEPTED, self.handle_assoc),
            (evt.EVT_RELEASED, self.handle_release),
            (evt.EVT_ABORTED, self.handle_abort),
            (evt.EVT_CONN_CLOSE, self.handle_connection_closed),
        ]
        


    def _slug(self, val: str, default: str = "unknown") -> str:
        """minúsculas, sin acentos, espacios->-, solo [a-z0-9._-]"""
        if not val:
            return default
        s = str(val).replace("^", " ")
        try:
            import unicodedata
            s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
        except Exception:
            pass
        s = s.lower().replace(" ", "-")
        allowed = "abcdefghijklmnopqrstuvwxyz0123456789._-"
        s = "".join(ch if ch in allowed else "-" for ch in s)
        while "--" in s:
            s = s.replace("--", "-")
        return s.strip("-._") or default

    def _safe(self, val: str, default: str = "unknown") -> str:
        return (str(val).strip() if val else default) or default

    def _study_date(self, ds) -> str:
        d = getattr(ds, "StudyDate", "") or ""
        return d[:8] if (len(d) >= 8 and d.isdigit()) else datetime.now().strftime("%Y%m%d")

    def handle_store(self, event):
        ds = event.dataset
        ds.file_meta = event.file_meta
        try:
            # ---- componentes del nombre de carpeta (UN SOLO NIVEL) ----
            patient_id   = self._safe(getattr(ds, "PatientID", None))
            patient_name = self._slug(getattr(ds, "PatientName", None))
            patient_name = patient_name.upper()
            study_date   = self._study_date(ds)
            modality     = self._slug(getattr(ds, "Modality", None))
            modality = modality.upper()
            fecha = getattr(ds, "StudyDate", None)
            hora = getattr(ds, "StudyTime", None)
            equipo= getattr(ds, "ManufacturerModelName", None)
            study_id_tag_value=getattr(ds, "StudyID", None)
            if study_id_tag_value is not None:
                if study_id_tag_value.isdigit():
                    study_id=study_id_tag_value
                else:
                    study_id='1'
            else:
                study_id='1'

            # Gestionar descripción para ecografía y RX
            descrip_estudio = getattr(ds, "StudyDescription", None)
            if not descrip_estudio:
                descrip_estudio = getattr(ds,"BodyPartExamined",None)
            if not descrip_estudio:
                descrip_estudio =  getattr(ds,"ProtocolName",None)
            if not descrip_estudio:
                descrip_estudio='No disponible'
            if descrip_estudio == '':
                descrip_estudio = 'No disponible'

            # usar StudyInstanceUID como ID único
            study_uid = self._safe(getattr(ds, "StudyInstanceUID", None))
            study_suffix = f"study-{self._slug(study_uid)}"

            folder_name = f"{patient_id}_{patient_name}_{study_date}_{modality}_{study_suffix}"
            out_dir = os.path.join(self.output_dir, folder_name)

            # ---- detectar si la carpeta es nueva y crearla ----
            p = Path(out_dir)
            was_new = not p.exists()
            p.mkdir(parents=True, exist_ok=True)
            os.makedirs(out_dir, exist_ok=True)

            if was_new:
                self.logger.info(f'Nueva carpeta: {folder_name}')
                l=(study_id,patient_id,patient_name,folder_name,fecha,hora,modality,descrip_estudio,STOR,equipo)
                self.insert_new_st(l)
            else:
                self.logger.info(f'Carpeta existente: {folder_name}')


            # ---- nombre de archivo ----
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            sop_uid = self._safe(getattr(ds, "SOPInstanceUID", None))
            filename = f"{timestamp}_{sop_uid}.dcm" if sop_uid != "unknown" else f"{timestamp}.dcm"
            file_path = os.path.join(out_dir, filename)

            ds.save_as(file_path, write_like_original=False)
            self.logger.info(f"[{event.assoc.requestor.address}] Stored DICOM: {file_path}")
            return 0x0000  # Success

        except Exception as e:
            self.logger.error(f"Failed to store DICOM: {e}")
            return 0xA700

         

        """"def handle_store(self, event):
            # handle para guardar archivos en carpeta intermediaria (especificada en el archivo json)
            ds = event.dataset
            ds.file_meta = event.file_meta
            try:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                sop_uid = ds.SOPInstanceUID
                filename = f"{timestamp}_{sop_uid}.dcm"
                file_path = os.path.join(self.output_dir, filename)
                ds.save_as(file_path, write_like_original=False)
                self.logger.info(f"[{event.assoc.requestor.address}] Stored DICOM: {file_path}")
                return 0x0000  # Success
            except Exception as e:
                self.logger.error(f"Failed to store DICOM: {e}")
                return 0xA700  # Out of resources"""
        
    def insert_new_st(self,l):
        try:
            db=self.Conectar()
            crs=db.cursor()
            sqlstring=f"insert into dicom_img(id_e,hc,nombre,carpeta,fecha,hora,modality,descrip,stor,equipo) " \
            f"values({l[0]},{l[1]},'{l[2]}','{l[3]}','{l[4]}','{l[5]}','{l[6]}','{l[7]}','{l[8]}','{l[9]}')"
            self.logger.info(sqlstring)
            crs.execute(sqlstring)
            crs.close()
            db.close
        except mysql.connector.Error as err:
            self.logger.error(f'Problemas con el acceso a la base de datos: {err}')
    
    def Conectar(self):
        try:
            db = mysql.connector.connect(host='192.168.0.240', user='mantenimiento', password='cambio18',
            database='clinik',port=3306, use_unicode=True, buffered=True)
            return db
        except mysql.connector.Error as err:
            msg = err.msg
            self.logger.error(f'El servidor dice:\n{msg}')


    def handle_assoc(self, event):
        self.logger.info(f"Association accepted from {event.assoc.requestor.address}")

    def handle_release(self, event):
        self.logger.info(f"Association released by {event.assoc.requestor.address}")

    def handle_abort(self, event):
        self.logger.warning(f"Association aborted by {event.assoc.requestor.address}")

    def handle_connection_closed(self, event):
        self.logger.info(f"Connection closed: {event.assoc.requestor.address}")

    def start(self, block=True):
        self.logger.info(f"Starting DICOM SCP on port {self.port} with AE Title '{self.ae_title}'")
        self.ae.start_server(('0.0.0.0', self.port), evt_handlers=self.handlers, block=block)
