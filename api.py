from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import tempfile
import shutil
import os
import zipfile
import json
import textwrap
from fpdf import FPDF
from typing import List
from ETL import procesar_archivo, guardar_en_db

app = FastAPI(title="ETL API")

# ====== EVITAR LÍMITES DE MEMORIA EN AWS ======
# Forzamos a que FastAPI guarde los archivos temporales en el disco duro
# en lugar de la carpeta por defecto (/tmp) que en Linux suele compartir la RAM.
# Esto soluciona el "Error parsing the body" con archivos gigantes.
temp_folder = os.path.join(os.getcwd(), "temporales")
os.makedirs(temp_folder, exist_ok=True)
tempfile.tempdir = temp_folder

# ====== CONFIGURACIÓN DE BASE DE DATOS ======
# 1. Para usar Neon en la nube de forma segura (Render inyectará DATABASE_URL):
# Si no encuentra la variable en Render, usará tu conexión por defecto para que siga funcionando en tu PC.
DB_URI = os.getenv("DATABASE_URL", "postgresql://neondb_owner:npg_2mqukLJz4rEb@ep-holy-dawn-aq7qt7xs-pooler.c-8.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require")

# 2. Para usar PostgreSQL Local en tu computadora, usa esta (cambia tu contraseña):
# DB_URI = "postgresql://postgres:Mar0409_@localhost:5432/postgres"

# Habilitar CORS para que el frontend pueda comunicarse con esta API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def servir_frontend():
    return FileResponse("index.html")

@app.post("/api/descargar_log_txt")
def descargar_log_txt(log_data: dict, background_tasks: BackgroundTasks):
    """
    Recibe una lista de strings (el log) y la convierte en un archivo TXT para descargar.
    """
    log_lines = log_data.get("log_lines", [])
    if not log_lines:
        raise HTTPException(status_code=400, detail="No se proporcionaron líneas de registro para el TXT.")

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt", dir=temp_folder, mode="w", encoding="utf-8") as tmp_txt:
            tmp_txt.write("\n".join(log_lines))
            tmp_txt_name = tmp_txt.name
            
        background_tasks.add_task(os.remove, tmp_txt_name)
        return FileResponse(path=tmp_txt_name, media_type='text/plain', filename='log_de_cambios.txt')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al generar el archivo TXT: {e}")

@app.post("/api/procesar")
def procesar(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    separador: str = Form(","),
    eliminar_duplicados: bool = Form(False),
    nombre_tabla: str = Form(None),
    columnas_eliminar: str = Form("[]"),
    mapa_nombres: str = Form("{}"),
    formato_descarga: str = Form("csv"),
    nombre_descarga: str = Form("")
):
    tmp_path = None # Path del archivo CSV a procesar
    uploaded_filepath = None # Path del archivo original subido
    try:
        # Interpretar las listas y diccionarios enviados como JSON desde el Frontend
        try:
            cols_a_eliminar = json.loads(columnas_eliminar)
            if not isinstance(cols_a_eliminar, list) or len(cols_a_eliminar) == 0:
                cols_a_eliminar = None
        except json.JSONDecodeError:
            cols_a_eliminar = None
            
        try:
            mapa_dict = json.loads(mapa_nombres)
            if not isinstance(mapa_dict, dict) or len(mapa_dict) == 0:
                mapa_dict = None
        except json.JSONDecodeError:
            mapa_dict = None

        # Guardar el archivo subido en un path temporal
        with tempfile.NamedTemporaryFile(delete=False, dir=temp_folder, suffix=os.path.splitext(file.filename)[1]) as tmp_upload:
            shutil.copyfileobj(file.file, tmp_upload)
            uploaded_filepath = tmp_upload.name

        # Si es un zip, lo descomprimimos. Si no, usamos el archivo original.
        if file.filename.lower().endswith(".zip"):
            print("Archivo ZIP detectado. Extrayendo...")
            with zipfile.ZipFile(uploaded_filepath, 'r') as zip_ref:
                csv_filename_in_zip = next((name for name in zip_ref.namelist() if name.lower().endswith(('.csv', '.txt')) and not name.startswith('__MACOSX')), None)
                if not csv_filename_in_zip:
                    raise HTTPException(status_code=400, detail="El archivo ZIP no contiene ningún archivo .csv o .txt.")
                
                # Creamos un nuevo archivo temporal para el contenido del CSV extraído
                with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", dir=temp_folder) as extracted_file:
                    extracted_file.write(zip_ref.read(csv_filename_in_zip))
                    tmp_path = extracted_file.name # Este es el archivo que procesaremos
                print(f"Archivo extraído a: {tmp_path}")
        else:
            # Si no es zip, el archivo a procesar es el que se subió
            tmp_path = uploaded_filepath
            
        # Procesamos con la ruta del archivo, no con los bytes en RAM
        df, log = procesar_archivo(
            file_path=tmp_path,
            separador=separador,
            eliminar_duplicados=eliminar_duplicados,
            columnas_a_eliminar=cols_a_eliminar,
            mapa_nombres=mapa_dict,
        )
        print("\n".join(log))
        
        # ⚡ Iniciar la subida a Neon silenciosamente en segundo plano
        if nombre_tabla:
            background_tasks.add_task(guardar_en_db, df, DB_URI, nombre_tabla)
        
        # Escribir salida en otro archivo temporal y transmitirlo
        original_base, _ = os.path.splitext(file.filename)
        filename_base = nombre_descarga.strip() if nombre_descarga.strip() else f"limpio_{original_base}"
        
        if formato_descarga == "excel":
            if df.height > 1048576:
                raise ValueError(f"Tu archivo tiene {df.height} filas. Microsoft Excel solo soporta un máximo de 1,048,576 filas por hoja. Por favor, selecciona el formato CSV o Parquet para descargar este archivo.")
            out_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx", dir=temp_folder)
            df.write_excel(out_tmp.name)
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            download_filename = f"{filename_base}.xlsx"
        elif formato_descarga == "parquet":
            out_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet", dir=temp_folder)
            df.write_parquet(out_tmp.name)
            media_type = "application/octet-stream"
            download_filename = f"{filename_base}.parquet"
        else:
            out_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", dir=temp_folder)
            df.write_csv(out_tmp.name, separator=separador)
            media_type = "text/csv"
            download_filename = f"{filename_base}.csv"
        
        background_tasks.add_task(os.remove, out_tmp.name)
        
        # Devolvemos el archivo y, en las cabeceras, enviamos el log para que el frontend lo pueda usar
        headers = {
            "X-ETL-Log": json.dumps(log),
            "Access-Control-Expose-Headers": "X-ETL-Log" # Permite que el JS lea la cabecera
        }
        
        return FileResponse(path=out_tmp.name, media_type=media_type, filename=download_filename, headers=headers)
    except Exception as e:
        # Si algo sale mal (ej: Polars no puede leer el CSV), devolvemos un error claro al frontend.
        raise HTTPException(status_code=400, detail=f"Error al procesar el archivo: {e}")
    finally:
        # Limpieza de archivos temporales
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
        # Si el archivo subido era un zip, tmp_path es diferente y hay que borrar el zip también
        if uploaded_filepath and uploaded_filepath != tmp_path and os.path.exists(uploaded_filepath):
            os.remove(uploaded_filepath)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)