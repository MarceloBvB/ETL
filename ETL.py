from datetime import datetime
import unicodedata
import polars as pl

def normalizar_texto(texto):
    """Elimina acentos, maneja nulos/números y convierte el texto a mayúsculas."""
    if texto is None or texto == "":
        return ""
    
    texto_str = str(texto)
    # Normalizar para separar los caracteres de sus acentos y luego eliminarlos
    texto_limpio = unicodedata.normalize('NFKD', texto_str).encode('ASCII', 'ignore').decode('utf-8')
    
    return texto_limpio.upper()

def limpiar_columna_polars(col_name):
    """Versión nativa y ultra-rápida de la limpieza usando funciones de Polars (código en Rust)."""
    return (
        pl.col(col_name)
        .fill_null("")
        .str.to_uppercase()
        .str.replace_all(r"[ÁÀÂÄ]", "A")
        .str.replace_all(r"[ÉÈÊË]", "E")
        .str.replace_all(r"[ÍÌÎÏ]", "I")
        .str.replace_all(r"[ÓÒÔÖ]", "O")
        .str.replace_all(r"[ÚÙÛÜ]", "U")
        .str.replace_all(r"[Ñ]", "N")
    )

def procesar_archivo(file_path: str, separador: str = ",", eliminar_duplicados: bool = False, columnas_a_eliminar: list = None, mapa_nombres: dict = None):
    """
    Función central para procesar los datos, independiente de la interfaz.
    """
    # Cambiamos read_csv por scan_csv para activar el modo perezoso (Lazy Loading)
    # Esto evita cargar todo el archivo en la memoria RAM de golpe.
    lf = pl.scan_csv(file_path, separator=separador, infer_schema_length=10000, truncate_ragged_lines=True)
    
    log_cambios = [
        "=== REGISTRO DE CAMBIOS DE ETL ===",
        f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Archivo original enlazado. (Modo ahorro de memoria RAM activado)"
    ]
    
    if eliminar_duplicados:
        lf = lf.unique()
        log_cambios.append(f"Acción: Se eliminaron filas duplicadas.")
        
    if columnas_a_eliminar:
        columnas_existentes = [col for col in columnas_a_eliminar if col in lf.columns]
        if columnas_existentes:
            lf = lf.drop(columnas_existentes)
            log_cambios.append(f"Acción: Se eliminaron las columnas: {', '.join(columnas_existentes)}.")
        
    if mapa_nombres:
        lf = lf.rename(mapa_nombres)
        renombradas = [f"'{k}' -> '{v}'" for k, v in mapa_nombres.items() if k != v]
        if renombradas:
            log_cambios.append(f"Acción: Columnas renombradas: {', '.join(renombradas)}")
            
    # lf.schema devuelve un diccionario {columna: tipo_de_dato}
    columnas_texto = [limpiar_columna_polars(col) for col, dtype in lf.schema.items() if dtype == pl.String]
    
    if columnas_texto:
        lf = lf.with_columns(columnas_texto)
        log_cambios.append("Acción: Se normalizó el texto en las columnas de texto.")
        
    # Recién aquí procesamos los datos usando 'streaming' por bloques para no agotar la RAM
    df = lf.collect(streaming=True)
    log_cambios.append(f"Proceso finalizado. Total resultante: {df.height} filas y {df.width} columnas.")

    return df, log_cambios

def guardar_en_db(df: pl.DataFrame, db_uri: str, nombre_tabla: str):
    """Guarda el DataFrame en la base de datos en un proceso silencioso de segundo plano."""
    try:
        print(f"⏳ [Segundo Plano] Subiendo {df.height} filas a la tabla '{nombre_tabla}' en la base de datos... (No cierres la terminal)")
        df.write_database(table_name=nombre_tabla, connection=db_uri, if_table_exists="replace", engine="adbc")
        print(f"✅ [Segundo Plano] ¡Éxito! Datos guardados en la tabla '{nombre_tabla}'.")
    except Exception as e:
        print(f"❌ [Segundo Plano] Error al exportar a la base de datos: {e}")