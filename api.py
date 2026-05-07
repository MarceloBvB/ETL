from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import tempfile
import shutil
import os
import json
from ETL import procesar_archivo, guardar_en_db

app = FastAPI(title="ETL API")

# ====== CONFIGURACIÓN DE BASE DE DATOS ======
# 1. Para usar Neon en la nube, descomenta la siguiente línea y comenta la local:
DB_URI = "postgresql://neondb_owner:npg_2mqukLJz4rEb@ep-holy-dawn-aq7qt7xs-pooler.c-8.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

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

        # Guardar en disco temporalmente para soportar archivos GIGANTES sin agotar la RAM
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            shutil.copyfileobj(file.file, tmp)
            tmp_path = tmp.name
            
        try:
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
                out_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
                df.write_excel(out_tmp.name)
                media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                download_filename = f"{filename_base}.xlsx"
            elif formato_descarga == "parquet":
                out_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
                df.write_parquet(out_tmp.name)
                media_type = "application/octet-stream"
                download_filename = f"{filename_base}.parquet"
            else:
                out_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
                df.write_csv(out_tmp.name, separator=separador)
                media_type = "text/csv"
                download_filename = f"{filename_base}.csv"
            
            # Programar la eliminación del archivo descargable una vez se haya enviado de forma segura
            background_tasks.add_task(os.remove, out_tmp.name)
            
            return FileResponse(path=out_tmp.name, media_type=media_type, filename=download_filename)
        finally:
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                os.remove(tmp_path)
    except Exception as e:
        # Si algo sale mal (ej: Polars no puede leer el CSV), devolvemos un error claro al frontend.
        raise HTTPException(status_code=400, detail=f"Error al procesar el archivo: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)