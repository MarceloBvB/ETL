from datetime import datetime
import unicodedata
import polars as pl
import re

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

def parse_date_robust(date_str):
    """Intenta parsear una fecha de distintos formatos posibles de forma robusta."""
    if not date_str or str(date_str).strip() == "":
        return None
    # Extraer solo dígitos y posibles separadores (-, /)
    cleaned = re.sub(r"[^\d/-]", "-", str(date_str))
    cleaned = re.sub(r"-+", "-", cleaned).strip("-")
    formats = ['%d-%m-%Y', '%Y-%m-%d', '%m-%d-%Y', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%y', '%y-%m-%d']
    for fmt in formats:
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            pass
    return None

def procesar_portafolio_2(file_path: str, separador: str = ","):
    """Procesa el portafolio 2: Famosos y Cumpleaños."""
    # 1. Cargar y eliminar registros duplicados
    df = pl.read_csv(file_path, separator=separador, infer_schema_length=10000, truncate_ragged_lines=True)
    df = df.unique()
    
    cols = df.columns
    if len(cols) < 2:
        raise ValueError("El archivo no pudo dividirse. Verifica que el separador sea correcto.")
        
    col_nombre = cols[0]
    col_fecha = cols[1]
    
    today = datetime.now().date()
    
    procesados = []
    for row in df.to_dicts():
        nombre = str(row.get(col_nombre, ""))
        fecha_str = str(row.get(col_fecha, ""))
        
        # 2. Quitar separadores no permitidos en nombres (solo letras y espacios)
        nombre_limpio = re.sub(r"[^a-zA-ZáéíóúÁÉÍÓÚñÑ\s]", "", nombre).strip()
        
        # 3. Formatear Fecha y calcular edad
        dt = parse_date_robust(fecha_str)
        if dt:
            # Corregir error común de años de 2 dígitos (ej: 68 -> 2068 en vez de 1968)
            if dt.year > today.year:
                dt = dt.replace(year=dt.year - 100)
                
            # Unificar formato Chile: DD-MM-YYYY
            fecha_formateada = dt.strftime('%d-%m-%Y')
            
            # 4. Atributo Edad
            age = today.year - dt.year - ((today.month, today.day) < (dt.month, dt.day))
            
            # 5. Flag de Cumpleaños
            es_cumple = (today.month == dt.month) and (today.day == dt.day)
        else:
            fecha_formateada = fecha_str
            age = None
            es_cumple = False
            
        procesados.append({
            "Nombre": nombre_limpio,
            "Fecha_Nacimiento": fecha_formateada,
            "Edad": age,
            "Es_Cumpleanos": es_cumple
        })
        
    return pl.DataFrame(procesados)

def procesar_portafolio_3(file_path: str, separador: str = ","):
    """Procesa el portafolio 3: Lugares, Georeferencias y Direcciones."""
    # 1. Eliminar duplicados
    df = pl.read_csv(file_path, separator=separador, infer_schema_length=10000, truncate_ragged_lines=True)
    df = df.unique()
    
    # Asegurar que exista un ID para relacionar las tablas
    if "ID" not in df.columns and "id" not in [c.lower() for c in df.columns]:
        df = df.with_columns(pl.Series("ID", range(1, len(df) + 1)))
    id_col = next((c for c in df.columns if c.lower() == "id"), "ID")
    
    # Buscador automático de columnas relacionadas a la temática
    def get_col(keywords):
        return next((col for col in df.columns if any(k in col.lower() for k in keywords)), None)
        
    # 2. Crear las tres tablas separando las columnas dinámicamente
    # Direcciones: ID, nombre_calle, numero_calle, ciudad_estado_provincia, país
    calle_col, num_col = get_col(["calle", "street", "direccion"]), get_col(["numero", "num", "number"])
    ciudad_col, pais_col = get_col(["ciudad", "city", "provincia", "estado", "state"]), get_col(["pais", "country"])
    df_direcciones = df.select([
        pl.col(id_col).alias("ID"),
        pl.col(calle_col).alias("nombre_calle") if calle_col else pl.lit("").alias("nombre_calle"),
        pl.col(num_col).alias("numero_calle") if num_col else pl.lit("").alias("numero_calle"),
        pl.col(ciudad_col).alias("ciudad_estado_provincia") if ciudad_col else pl.lit("").alias("ciudad_estado_provincia"),
        pl.col(pais_col).alias("país") if pais_col else pl.lit("").alias("país"),
    ])
    # Georeferencias: ID, latitud, longitud
    lat_col, lon_col = get_col(["lat", "latitud"]), get_col(["lon", "longitud"])
    df_geo = df.select([pl.col(id_col).alias("ID"), pl.col(lat_col).alias("latitud") if lat_col else pl.lit(0.0).alias("latitud"), pl.col(lon_col).alias("longitud") if lon_col else pl.lit(0.0).alias("longitud")])
    # Lugares: ID, nombre_lugar
    nombre_col = get_col(["lugar", "nombre", "name", "place"])
    df_lugares = df.select([pl.col(id_col).alias("ID"), pl.col(nombre_col).alias("nombre_lugar") if nombre_col else pl.lit("").alias("nombre_lugar")])
    return df_lugares, df_geo, df_direcciones