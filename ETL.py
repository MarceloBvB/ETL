from datetime import datetime
import unicodedata
import polars as pl
import re
import os
import logging
from rapidfuzz import process, fuzz
import urllib.request
import urllib.parse
import json
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DataCleaner")

class DataCleaner:
    """Clase profesional para normalización y enriquecimiento de datos usando RapidFuzz."""
    
    def __init__(self, file_path: str):
        self.master_list = []
        self.processed_master_list = []
        self._load_data(file_path)

    def _load_data(self, path: str):
        try:
            if path.endswith('.json'):
                with open(path, 'r', encoding='utf-8') as f:
                    self.master_list = json.load(f)
            elif path.endswith('.csv'):
                with open(path, 'r', encoding='utf-8') as f:
                    self.master_list = [line.strip() for line in f.readlines() if line.strip()]
            else:
                raise ValueError("Formato no soportado. Debe ser .json o .csv")
            
            # Pre-procesar la lista maestra una sola vez en memoria para optimizar el rendimiento
            self.processed_master_list = [self._preprocess(item) for item in self.master_list]
            logger.info(f"Cargados {len(self.master_list)} valores maestros desde {path}")
        except Exception as e:
            logger.error(f"Error crítico al cargar lista maestra desde {path}: {e}")
            raise

    def _preprocess(self, text: str) -> str:
        if not isinstance(text, text.__class__):
            return ""
        # 1. Normalización Unicode (eliminar acentos y tildes correctamente)
        text = unicodedata.normalize('NFKD', str(text)).encode('ASCII', 'ignore').decode('utf-8')
        # 2. Minúsculas
        text = text.lower()
        # 3. Eliminar caracteres especiales (mantener letras, números y espacios)
        text = re.sub(r'[^a-z0-9\s]', '', text)
        # 4. Manejar espacios múltiples (ej: "cabo d   ehornos" -> "cabo d ehornos")
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def corregir_dato(self, entrada: str, umbral_similitud: float = 80.0) -> str:
        if not entrada or not str(entrada).strip():
            return entrada
        
        entrada_limpia = self._preprocess(entrada)
        if not entrada_limpia:
            return entrada
            
        try:
            # WRatio maneja excelentemente diferencias de longitud y errores de espacios/tipográficos
            match = process.extractOne(
                entrada_limpia, 
                self.processed_master_list, 
                scorer=fuzz.WRatio, 
                score_cutoff=umbral_similitud
            )
            
            if match:
                mejor_coincidencia, score, indice = match
                valor_corregido = self.master_list[indice].upper()
                logger.debug(f"Corregido: '{entrada}' -> '{valor_corregido}' (Score: {score:.2f})")
                return valor_corregido
            else:
                logger.warning(f"Sin coincidencia ({entrada}). Se mantiene el valor original.")
                return entrada
        except Exception as e:
            logger.error(f"Fallo al procesar dato '{entrada}': {e}")
            return entrada

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
    if separador == "\\t":
        separador = "\t"
        
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

def _leer_csv_robusto(file_path: str, separador: str) -> pl.DataFrame:
    """Intenta leer el CSV con el separador indicado; si falla, auto-detecta \t o ;."""
    if separador == "\\t":
        separador = "\t"
        
    try:
        df = pl.read_csv(file_path, separator=separador, infer_schema_length=10000, truncate_ragged_lines=True)
        if len(df.columns) >= 2:
            return df
    except Exception:
        pass
        
    # Fallback automático: Si falla o da solo 1 columna, intentamos con otros
    for alt_sep in ["\t", ";", ",", "|"]:
        try:
            df_alt = pl.read_csv(file_path, separator=alt_sep, infer_schema_length=10000, truncate_ragged_lines=True)
            if len(df_alt.columns) >= 2: return df_alt
        except Exception: pass
        
    # Si todo falla y queda 1 sola columna, la leemos SIN cabecera para no perder el primer registro
    return pl.read_csv(file_path, separator=separador, has_header=False, infer_schema_length=10000, truncate_ragged_lines=True)

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

def procesar_portafolio_unificado(file_path: str, separador: str = ","):
    """Unifica la lógica de los portafolios 2 y 3: procesa cualquier archivo, 
    limpia textos, estandariza fechas, calcula edades y divide en tablas si detecta lugares."""
    df = _leer_csv_robusto(file_path, separador)
    
    # --- NUEVO: Inteligencia para separar datos amontonados en 1 sola columna ---
    if len(df.columns) == 1:
        col_name = df.columns[0]
        # Usamos una expresión regular para separar "ID. Nombre - Fecha" de forma limpia
        patron = r"^(\d+)\.?\s+(.*?)(?:\s+-\s+(.*))?$"
        
        df_extracted = df.select(
            pl.col(col_name).str.extract(patron, 1).alias("ID"),
            pl.col(col_name).str.extract(patron, 2).alias("Nombre"),
            pl.col(col_name).str.extract(patron, 3).alias("Fecha_Nacimiento")
        )
        
        if df_extracted["Nombre"].null_count() < len(df_extracted):
            df = df_extracted
            df = df.with_columns(pl.col("Nombre").str.strip_chars())
            
    # Renombrar columnas genéricas para que el Excel sea entendible
    for col in df.columns:
        if col.startswith("column_"):
            df = df.rename({col: "Dato_Principal"})
    
    # 1. Limpieza universal de textos (Mayúsculas, sin tildes)
    cols_texto = [col for col, dtype in df.schema.items() if dtype == pl.String and col != "Fecha_Nacimiento"]
    today = datetime.now().date()
    
    if cols_texto:
        df = df.with_columns([
            limpiar_columna_polars(col).str.replace_all(r"[^A-Z0-9\s\.,:\-/_]", "").str.strip_chars().alias(col)
            for col in cols_texto
        ])

    # 2. AHORA SÍ eliminamos duplicados (ya que todo está normalizado e igual)
    df = df.unique(maintain_order=True)

    # 3. Asegurar ID si no existe (al final, para que no interfiera al borrar duplicados)
    has_id = any(c.lower() == "id" for c in df.columns)
    if not has_id:
        df = df.with_columns(pl.Series("ID", range(1, len(df) + 1)))
    id_col = next((c for c in df.columns if c.lower() == "id"), "ID")

    # 4. Detectar fechas automáticamente en cualquier columna (Lógica Famosos)
    columnas_con_fechas = []
    if "Fecha_Nacimiento" in df.columns:
        columnas_con_fechas.append("Fecha_Nacimiento")
    else:
        for col in cols_texto:
            muestras = df[col].drop_nulls().head(5).to_list()
            if any(parse_date_robust(m) is not None for m in muestras):
                columnas_con_fechas.append(col)

    def extraer_fecha(val):
        if not val: return val
        val_str = str(val).upper().strip()
        if re.search(r"A\.?\s*C\.?", val_str) or re.search(r"^-\d+", val_str):
            year = re.sub(r"[^\d]", "", val_str)
            return f"{year} a.C. (Antes de Cristo)"
            
        dt = parse_date_robust(val)
        if dt:
            if dt.year > today.year: dt = dt.replace(year=dt.year - 100)
            return dt.strftime('%d-%m-%Y')
        return val

    def calcular_edad(val):
        if not val: return None
        val_str = str(val).upper().strip()
        if re.search(r"A\.?\s*C\.?", val_str) or re.search(r"^-\d+", val_str):
            year = re.sub(r"[^\d]", "", val_str)
            if year.isdigit():
                return today.year + int(year) # Ej: 2026 + 300 = 2326 años
            return None
            
        dt = parse_date_robust(val)
        if dt:
            if dt.year > today.year: dt = dt.replace(year=dt.year - 100)
            return today.year - dt.year - ((today.month, today.day) < (dt.month, dt.day))
        return None

    def es_cumple(val):
        if not val: return False
        val_str = str(val).upper().strip()
        if re.search(r"A\.?\s*C\.?", val_str) or re.search(r"^-\d+", val_str):
            return False # Las fechas A.C. rara vez tienen día y mes exacto
            
        dt = parse_date_robust(val)
        if dt:
            if dt.year > today.year: dt = dt.replace(year=dt.year - 100)
            return (today.month == dt.month) and (today.day == dt.day)
        return False

    for col in columnas_con_fechas:
        df = df.with_columns(
            pl.col(col).map_elements(extraer_fecha, return_dtype=pl.String).alias(col),
            pl.col(col).map_elements(calcular_edad, return_dtype=pl.Int32).alias("Edad_Calculada"),
            pl.col(col).map_elements(es_cumple, return_dtype=pl.Boolean).alias("Es_Cumpleanos")
        )

    # 5. Detectar columnas geográficas (Lógica Lugares)
    def get_col(keywords):
        return next((col for col in df.columns if any(k in col.lower() for k in keywords)), None)
        
    calle_col, num_col = get_col(["calle", "street", "direccion"]), get_col(["numero", "num", "number"])
    ciudad_col, pais_col = get_col(["ciudad", "city", "provincia", "estado", "state"]), get_col(["pais", "country"])
    lat_col, lon_col = get_col(["lat", "latitud"]), get_col(["lon", "longitud"])
    nombre_col = get_col(["lugar", "nombre", "name", "place"])

    tablas = {}
    if calle_col or lat_col or ciudad_col:
        df_direcciones = df.select([
            pl.col(id_col).alias("ID"), pl.col(calle_col).alias("nombre_calle") if calle_col else pl.lit("").alias("nombre_calle"),
            pl.col(num_col).alias("numero_calle") if num_col else pl.lit("").alias("numero_calle"),
            pl.col(ciudad_col).alias("ciudad_estado_provincia") if ciudad_col else pl.lit("").alias("ciudad_estado_provincia"),
            pl.col(pais_col).alias("país") if pais_col else pl.lit("").alias("país"),
        ])
        df_geo = df.select([pl.col(id_col).alias("ID"), pl.col(lat_col).alias("latitud") if lat_col else pl.lit("0.0").alias("latitud"), pl.col(lon_col).alias("longitud") if lon_col else pl.lit("0.0").alias("longitud")])
        df_lugares = df.select([pl.col(id_col).alias("ID"), pl.col(nombre_col).alias("nombre_lugar") if nombre_col else pl.lit("").alias("nombre_lugar")])
        
        tablas["Lugares"] = df_lugares
        tablas["Georeferencias"] = df_geo
        tablas["Direcciones"] = df_direcciones
        
    # Siempre devolvemos la tabla unificada por si acaso
    df_principal = df.clone()
    if id_col in df_principal.columns:
        df_principal = df_principal.drop(id_col)
    tablas["Tabla_Principal_Normalizada"] = df_principal
    
    return tablas