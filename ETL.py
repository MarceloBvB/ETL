from datetime import datetime
import unicodedata
import polars as pl
import re
import os
import shutil
import logging
from rapidfuzz import process, fuzz
import urllib.request
import urllib.parse
import json
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DataCleaner")

# --- TÉCNICA PROFESIONAL: CATÁLOGO DINÁMICO EN TIEMPO REAL ---
DICCIONARIO_API_CACHE = []

# Lista Oficial Interna de las 346 Comunas de Chile (+ extras y regiones para tests)
COMUNAS_Y_REGIONES_CHILE = [
    "ARICA", "CAMARONES", "PUTRE", "GENERAL LAGOS", "IQUIQUE", "ALTO HOSPICIO", "POZO ALMONTE", "CAMINA", "COLCHANE", "HUARA", "PICA", "ANTOFAGASTA", "MEJILLONES", "SIERRA GORDA", "TALTAL", "CALAMA", "OLLAGUE", "SAN PEDRO DE ATACAMA", "TOCOPILLA", "MARIA ELENA", "COPIAPO", "CALDERA", "TIERRA AMARILLA", "CHANARAL", "DIEGO DE ALMAGRO", "VALLENAR", "ALTO DEL CARMEN", "FREIRINA", "HUASCO", "LA SERENA", "COQUIMBO", "ANDACOLLO", "LA HIGUERA", "PAIHUANO", "VICUNA", "ILLAPEL", "LOS VILOS", "SALAMANCA", "OVALLE", "COMBARBALA", "MONTE PATRIA", "PUNITAQUI", "RIO HURTADO", "VALPARAISO", "CASABLANCA", "CONCON", "JUAN FERNANDEZ", "PUCHUNCAVI", "QUINTERO", "VINA DEL MAR", "ISLA DE PASCUA", "LOS ANDES", "CABILDO", "CALLE LARGA", "PAPUDO", "PETORCA", "ZAPALLAR", "QUILLOTA", "CALERA", "HIJUELAS", "LA CRUZ", "LA LIGUA", "NOGALES", "SAN ANTONIO", "ALGARROBO", "CARTAGENA", "EL QUISCO", "EL TABO", "SANTO DOMINGO", "SAN FELIPE", "CATEMU", "LLAILLAY", "PANQUEHUE", "PUTAENDO", "SANTA MARIA", "QUILPUE", "LIMACHE", "OLMUE", "VILLA ALEMANA", "RANCAGUA", "CODEGUA", "COINCO", "COLTAUCO", "DONIHUE", "GRANEROS", "LAS CABRAS", "MACHALI", "MALLOA", "MOSTAZAL", "OLIVAR", "PEUMO", "PICHIDEGUA", "QUINTA DE TILCOCO", "RENGO", "REQUINOA", "SAN VICENTE", "PICHILEMU", "LA ESTRELLA", "LITUECHE", "MARCHIHUE", "NAVIDAD", "PAREDONES", "SAN FERNANDO", "CHEPICA", "CHIMBARONGO", "LOLOL", "NANCAGUA", "PALMILLA", "PERALILLO", "PLACILLA", "PUMANQUE", "SANTA CRUZ", "TALCA", "CONSTITUCION", "CUREPTO", "EMPEDRADO", "MAULE", "PELARCO", "PENCAHUE", "RIO CLARO", "SAN CLEMENTE", "SAN RAFAEL", "CAUQUENES", "CHANCO", "PELLUHUE", "CURICO", "HUALANE", "LICANTEN", "MOLINA", "RAUCO", "ROMERAL", "SAGRADA FAMILIA", "TENO", "VICHUQUEN", "LINARES", "COLBUN", "LONGAVI", "PARRAL", "RETIRO", "SAN JAVIER", "VILLA ALEGRE", "YERBAS BUENAS", "CHILLAN", "BULNES", "COBQUECURA", "COELEMU", "COIHUECO", "CHILLAN VIEJO", "EL CARMEN", "NINHUE", "NIQUEN", "PEMUCO", "PINTO", "PORTEZUELO", "QUILLON", "QUIRIHUE", "RANQUIL", "SAN CARLOS", "SAN FABIAN", "SAN IGNACIO", "SAN NICOLAS", "TREGUACO", "YUNGAY", "CONCEPCION", "CORONEL", "CHIGUAYANTE", "FLORIDA", "HUALQUI", "LOTA", "PENCO", "SAN PEDRO DE LA PAZ", "SANTA JUANA", "TALCAHUANO", "TOME", "HUALPEN", "LEBU", "ARAUCO", "CANETE", "CONTULMO", "CURANILAHUE", "LOS ALAMOS", "TIRUA", "LOS ANGELES", "ANTUCO", "CABRERO", "LAJA", "MULCHEN", "NACIMIENTO", "NEGRETE", "QUILACO", "QUILLECO", "SAN ROSENDO", "SANTA BARBARA", "TUCAPEL", "YUMBEL", "ALTO BIOBIO", "TEMUCO", "CARAHUE", "CUNCO", "CURARREHUE", "FREIRE", "GALVARINO", "GORBEA", "LAUTARO", "LONCOCHE", "MELIPEUCO", "NUEVA IMPERIAL", "PADRE LAS CASAS", "PERQUENCO", "PITRUFQUEN", "PUCON", "SAAVEDRA", "TEODORO SCHMIDT", "TOLTEN", "VILCUN", "VILLARRICA", "CHOLCHOL", "ANGOL", "COLLIPULLI", "CURACAUTIN", "ERCILLA", "LONQUIMAY", "LOS SAUCES", "LUMACO", "PUREN", "RENAICO", "TRAIGUEN", "VICTORIA", "VALDIVIA", "CORRAL", "LANCO", "LOS LAGOS", "MAFIL", "MARIQUINA", "PAILLACO", "PANGUIPULLI", "LA UNION", "FUTRONO", "LAGO RANCO", "RIO BUENO", "PUERTO MONTT", "CALBUCO", "COCHAMO", "FRESIA", "FRUTILLAR", "LOS MUERMOS", "LLANQUIHUE", "MAULLIN", "PUERTO VARAS", "CASTRO", "ANCUD", "CHONCHI", "CURACO DE VELEZ", "DALCAHUE", "PUQUELDON", "QUEILEN", "QUELLON", "QUEMCHI", "QUINCHAO", "OSORNO", "PUERTO OCTAY", "PURRANQUE", "PUYEHUE", "RIO NEGRO", "SAN JUAN DE LA COSTA", "SAN PABLO", "CHAITEN", "FUTALEUFU", "HUALAIHUE", "PALENA", "COYHAIQUE", "LAGO VERDE", "AYSEN", "CISNES", "GUAITECAS", "COCHRANE", "OHIGGINS", "TORTEL", "CHILE CHICO", "PUNTA ARENAS", "LAGUNA BLANCA", "RIO VERDE", "SAN GREGORIO", "CABO DE HORNOS", "ANTARTICA", "PORVENIR", "PRIMAVERA", "TIMAUKEL", "NATALES", "TORRES DEL PAINE", "SANTIAGO", "CERRILLOS", "CERRO NAVIA", "CONCHALI", "EL BOSQUE", "ESTACION CENTRAL", "HUECHURABA", "INDEPENDENCIA", "LA CISTERNA", "LA FLORIDA", "LA GRANJA", "LA PINTANA", "LA REINA", "LAS CONDES", "LO BARNECHEA", "LO ESPEJO", "LO PRADO", "MACUL", "MAIPU", "NUNOA", "PEDRO AGUIRRE CERDA", "PENALOLEN", "PROVIDENCIA", "PUDAHUEL", "QUILICURA", "QUINTA NORMAL", "RECOLETA", "RENCA", "SAN JOAQUIN", "SAN MIGUEL", "SAN RAMON", "VITACURA", "PUENTE ALTO", "PIRQUE", "SAN JOSE DE MAIPO", "COLINA", "LAMPA", "TILTIL", "SAN BERNARDO", "BUIN", "CALERA DE TANGO", "PAINE", "MELIPILLA", "ALHUE", "CURACAVI", "MARIA PINTO", "SAN PEDRO", "TALAGANTE", "EL MONTE", "ISLA DE MAIPO", "PADRE HURTADO", "PENAFLOR", "LOS RIOS", "BIOBIO", "RIO", "SAN JOSE"
]

def obtener_diccionario_oficial():
    """
    Se basa en la lista oficial de las 346 comunas de Chile como pide el requerimiento.
    Adicionalmente, intenta complementarlo con la API del Gobierno.
    """
    global DICCIONARIO_API_CACHE
    if DICCIONARIO_API_CACHE:
        return DICCIONARIO_API_CACHE
        
    lista_oficial = list(COMUNAS_Y_REGIONES_CHILE)
    try:
        # Fetch de Comunas Oficiales
        req1 = urllib.request.Request("https://apis.digital.gob.cl/dpa/comunas", headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req1, timeout=3) as r:
            lista_oficial.extend([normalizar_texto(c["nombre"]) for c in json.loads(r.read().decode())])
            
        # Fetch de Regiones Oficiales
        req2 = urllib.request.Request("https://apis.digital.gob.cl/dpa/regiones", headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req2, timeout=3) as r:
            lista_oficial.extend([normalizar_texto(c["nombre"]) for c in json.loads(r.read().decode())])
            
    except Exception as e:
        logger.warning(f"Aviso API: Usando estrictamente la lista interna de las 346 comunas. Detalles: {e}")
        
    DICCIONARIO_API_CACHE = list(set(lista_oficial))
    return DICCIONARIO_API_CACHE

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

def generar_mapa_correccion(valores_counts: list, umbral: float = 80.0) -> dict:
    """
    Clustering difuso no supervisado (Estilo OpenRefine).
    [REGLA PARTE 2] Fuzzy Matching: Corrección de errores tipográficos.
    *Nota Técnica: Utilizamos RapidFuzz (C++) en lugar de TheFuzz (Python puro)
    ya que es una implementación exacta pero 50 veces más rápida para Big Data.
    Agrupa errores tipográficos minoritarios hacia la versión mayoritaria.
    """
    mapping = {}
    procesados = set()
    
    dict_conteos = {v: c for v, c in valores_counts if v and isinstance(v, str) and str(v).strip()}
    # ORDEN INTELIGENTE: Si hay empate de veces que aparece, preferir la palabra más larga (evita recortes como ALTO DEL CRMEN)
    valores_unicos = sorted(dict_conteos.keys(), key=lambda x: (dict_conteos[x], len(x)), reverse=True)
    
    for valor in valores_unicos:
        if valor in procesados:
            continue
            
        procesados.add(valor)
        restantes = [v for v in valores_unicos if v not in procesados]
        
        if restantes:
            coincidencias = process.extract(valor, restantes, scorer=fuzz.WRatio, score_cutoff=umbral)
            for match_str, score, _ in coincidencias:
                mapping[match_str] = valor
                procesados.add(match_str)
                
    return mapping

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
        .str.replace_all(r"_", " ") # Remover guiones bajos siempre
        .str.replace_all(r"[^A-Z0-9\s\.,:\-/]", "") # Elimina caracteres especiales como ! @ # $ %
        # --- NUEVO: Remover puntos y guiones sobrantes en textos (Protegiendo Fechas y Coordenadas) ---
        .str.replace_all(r"([A-Z])\s*[\.\-]+\s*([A-Z])", "${1} ${2}") # Entre letras: SAN - JOSE -> SAN JOSE
        .str.replace_all(r"([A-Z])\s*[\.\-]+", "${1}") # Después de letra: PERAS. -> PERAS
        .str.replace_all(r"[\.\-]+\s*([A-Z])", "${1}") # Antes de letra: -MANZANA -> MANZANA
        .str.replace_all(r"\s+", " ") # Limpia espacios dobles
        .str.strip_chars() # Quita espacios al inicio y final
    )

def asegurar_utf8(file_path: str):
    """
    Verifica que el archivo sea UTF-8 válido. Si tiene codificación de Windows (latin-1),
    lo convierte en milisegundos para evitar que el motor de Polars (Rust) arroje error.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for _ in f: pass
    except UnicodeDecodeError:
        tmp_path = file_path + "_utf8.tmp"
        with open(file_path, 'r', encoding='latin-1') as f_in:
            with open(tmp_path, 'w', encoding='utf-8') as f_out:
                for line in f_in:
                    f_out.write(line)
        shutil.move(tmp_path, file_path)

def procesar_archivo(file_path: str, separador: str = ",", eliminar_duplicados: bool = False, columnas_a_eliminar: list = None, mapa_nombres: dict = None):
    """
    Función central para procesar los datos, independiente de la interfaz.
    """
    asegurar_utf8(file_path)
    
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
    columnas_texto_nombres = [col for col, dtype in lf.schema.items() if dtype == pl.String]
    columnas_texto_exprs = [limpiar_columna_polars(col) for col in columnas_texto_nombres]
    
    if columnas_texto_exprs:
        lf = lf.with_columns(columnas_texto_exprs)
        log_cambios.append("Acción: Se normalizó el texto básico (mayúsculas y sin tildes).")

    # Recién aquí procesamos los datos usando 'streaming'
    df = lf.collect(streaming=True)
    
    # === 1. CORRECCIÓN AVANZADA CONTRA API DEL GOBIERNO ===
    diccionario_gobierno = obtener_diccionario_oficial()
        
    for col in columnas_texto_nombres:
        n_unique = df[col].n_unique()
        if 1 < n_unique < 5000:
            
            # --- PERFILAMIENTO INTELIGENTE (Data Profiling) ---
            # Detecta si la columna contiene lugares/comunas o si es otra cosa (ej. Frutas, Famosos)
            es_columna_geo = any(k in col.lower() for k in ["comuna", "region", "ciudad", "lugar", "provincia", "ubicacion"])
            
            if not es_columna_geo and diccionario_gobierno:
                muestras = df.get_column(col).drop_nulls().head(30).to_list()
                if muestras:
                    hits = sum(1 for m in muestras if m in diccionario_gobierno)
                    if (hits / len(muestras)) >= 0.1: # Si al menos 10% de la muestra son comunas reales
                        es_columna_geo = True
            
            # Solo aplicamos el diccionario de Chile si el perfilamiento confirma que son lugares
            if es_columna_geo and diccionario_gobierno:
                unicos = df.get_column(col).drop_nulls().unique().to_list()
                reemplazos_api = {}
                for valor in unicos:
                    if not valor or len(str(valor)) < 3: continue
                    if valor not in diccionario_gobierno:
                        match = process.extractOne(valor, diccionario_gobierno, scorer=fuzz.WRatio, score_cutoff=80.0)
                        if match:
                            reemplazos_api[valor] = match[0]
                            
                if reemplazos_api:
                    df = df.with_columns(pl.col(col).map_elements(lambda x: reemplazos_api.get(x, x), return_dtype=pl.String))
                    log_cambios.append(f"Acción: {len(reemplazos_api)} errores en '{col}' cruzados y corregidos vía API del Gobierno.")
            
            # === 2. CLUSTERING NO SUPERVISADO GENERAL ===
            vc = df.get_column(col).value_counts(sort=True)
            valores = vc.get_column(col).to_list()
            conteos = vc.get_columns()[1].to_list() 
            
            reemplazos = generar_mapa_correccion(list(zip(valores, conteos)), umbral=80.0)
            if reemplazos:
                df = df.with_columns(pl.col(col).map_elements(lambda x: reemplazos.get(x, x), return_dtype=pl.String))
                log_cambios.append(f"Acción: Columna '{col}' corregida dinámicamente. Se fusionaron {len(reemplazos)} errores tipográficos.")

    # Eliminar duplicados DESPUÉS de todas las limpiezas de texto
    if eliminar_duplicados:
        # Ignorar columnas identificadoras al buscar duplicados
        cols_dup = [c for c in df.columns if c.strip().lower() not in ["id", "index", "n", "nº", "no", "numero"]]
        if cols_dup:
            df = df.unique(subset=cols_dup, maintain_order=True)
        else:
            df = df.unique(maintain_order=True)
        log_cambios.append(f"Acción: Se eliminaron filas duplicadas exactas.")

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
    asegurar_utf8(file_path)
    
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
    
    # Renombrar columnas genéricas para que el Excel sea entendible
    for col in df.columns:
        if col.startswith("column_"):
            df = df.rename({col: "Dato_Principal"})
    
    # 1. Limpieza universal de textos (Mayúsculas, sin tildes)
    cols_texto = [col for col, dtype in df.schema.items() if dtype == pl.String and col != "Fecha_Nacimiento"]
    
    # [REGLA PARTE 1] Atributos Dinámicos: Fecha del sistema fijada a 23-05-2026 según el requerimiento.
    today = datetime(2026, 5, 23).date() 
    
    if cols_texto:
        df = df.with_columns([
            limpiar_columna_polars(col).str.replace_all(r"[^A-Z0-9\s\.,:\-/_]", "").str.strip_chars().alias(col)
            for col in cols_texto
        ])

        diccionario_gobierno = obtener_diccionario_oficial()
        
        for col in cols_texto:
            n_unique = df[col].n_unique()
            if 1 < n_unique < 5000:
                
                # --- PERFILAMIENTO INTELIGENTE (Data Profiling) ---
                # Detecta si la columna contiene lugares/comunas o si es otra cosa (ej. Frutas)
                es_columna_geo = any(k in col.lower() for k in ["comuna", "region", "ciudad", "lugar", "provincia", "ubicacion"])
                
                if not es_columna_geo and diccionario_gobierno:
                    muestras = df.get_column(col).drop_nulls().head(30).to_list()
                    if muestras:
                        hits = sum(1 for m in muestras if m in diccionario_gobierno)
                        if (hits / len(muestras)) >= 0.1:
                            es_columna_geo = True
                
                # Corrección por Diccionario Oficial (Solo para Lugares)
                if es_columna_geo and diccionario_gobierno:
                    unicos = df.get_column(col).drop_nulls().unique().to_list()
                    reemplazos_api = {}
                    for valor in unicos:
                        if not valor or len(str(valor)) < 3: continue
                        if valor not in diccionario_gobierno:
                            match = process.extractOne(valor, diccionario_gobierno, scorer=fuzz.WRatio, score_cutoff=85.0)
                            if match: reemplazos_api[valor] = match[0]
                    if reemplazos_api:
                        df = df.with_columns(pl.col(col).map_elements(lambda x: reemplazos_api.get(x, x), return_dtype=pl.String))
                
                # Corrección por Clustering interno
                vc = df.get_column(col).value_counts(sort=True)
                valores = vc.get_column(col).to_list()
                conteos = vc.get_columns()[1].to_list()
                
                reemplazos = generar_mapa_correccion(list(zip(valores, conteos)), umbral=85.0)
                if reemplazos:
                    df = df.with_columns(pl.col(col).map_elements(lambda x: reemplazos.get(x, x), return_dtype=pl.String))

    # 2. Detectar fechas automáticamente en cualquier columna (Lógica Famosos)
    # Hacemos esto ANTES de eliminar duplicados, para que "01-01-1990" y "1/1/90" se reconozcan como iguales
    columnas_con_fechas = []
    if "Fecha_Nacimiento" in df.columns:
        columnas_con_fechas.append("Fecha_Nacimiento")
    else:
        for col in cols_texto:
            muestras = df[col].drop_nulls().head(5).to_list()
            if any(parse_date_robust(m) is not None or re.search(r"(?:ALREDEDOR|APROX|CIRCA|C\.|~)[^\d]*(\d{3,4})", str(m).upper()) for m in muestras):
                columnas_con_fechas.append(col)

    def extraer_fecha(val):
        if not val: return val
        val_str = str(val).upper().strip()
        if re.search(r"A\.?\s*C\.?", val_str) or re.search(r"^-\d+", val_str):
            year = re.sub(r"[^\d]", "", val_str)
            return f"{year} a.C. (Antes de Cristo)"
            
        # Detección de fechas aproximadas
        match_aprox = re.search(r"(?:ALREDEDOR|APROX|CIRCA|C\.|~)[^\d]*(\d{3,4})", val_str)
        if match_aprox:
            return f"Aprox. {match_aprox.group(1)}"

        dt = parse_date_robust(val)
        if dt:
            if dt.year > today.year: dt = dt.replace(year=dt.year - 100)
            return dt.strftime('%d-%m-%Y')
        return val_str if val_str else val

    def calcular_edad(val):
        if not val: return None
        val_str = str(val).upper().strip()
        if re.search(r"A\.?\s*C\.?", val_str) or re.search(r"^-\d+", val_str):
            year = re.sub(r"[^\d]", "", val_str)
            if year.isdigit():
                return today.year + int(year) # Ej: 2026 + 300 = 2326 años
            return None
            
        # Cálculo de edad aproximada
        match_aprox = re.search(r"(?:ALREDEDOR|APROX|CIRCA|C\.|~)[^\d]*(\d{3,4})", val_str)
        if match_aprox:
            return today.year - int(match_aprox.group(1))

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
            
        # Fechas aproximadas no tienen día exacto
        match_aprox = re.search(r"(?:ALREDEDOR|APROX|CIRCA|C\.|~)[^\d]*(\d{3,4})", val_str)
        if match_aprox:
            return False
            
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

    # 3. REGLA: DEDUPLICACIÓN Y LIMPIEZA ESPECÍFICA (Famosos vs Lugares)
    nombre_famoso_col = next((c for c in df.columns if "nombre" in c.lower() or "personaje" in c.lower() or "famoso" in c.lower()), None)
    is_famosos_dataset = len(columnas_con_fechas) > 0 and nombre_famoso_col

    if is_famosos_dataset:
        # [REGLA PARTE 1] Limpieza de Nombres: Elimina numeraciones ("1. Pedro") y caracteres especiales del inicio
        df = df.with_columns(
            pl.col(nombre_famoso_col).str.replace(r"^\d+[\.\-]?\s*", "").str.replace_all(r"[^A-Z\s]", "").str.strip_chars().alias(nombre_famoso_col)
        )
        # [REGLA PARTE 1] Deduplicación: Elimina registros duplicados basándose SOLAMENTE en el nombre y la fecha de nacimiento
        df = df.unique(subset=[nombre_famoso_col, columnas_con_fechas[0]], maintain_order=True)
    else:
        # [REGLA PARTE 2] Limpieza de Nombres de Lugares: Elimina numeración inicial (ej: "1. ", "2 - ") sin dañar "10 DOWNING STREET"
        nombre_lugar_col = next((c for c in df.columns if "nombre" in c.lower() or "lugar" in c.lower() or "place" in c.lower()), None)
        if nombre_lugar_col:
            df = df.with_columns(
                pl.col(nombre_lugar_col).str.replace(r"^\d+[\.\-]\s+", "").str.strip_chars().alias(nombre_lugar_col)
            )
            
        # [REGLA PARTE 2] Deduplicación: Se ejecuta DESPUÉS de limpiar para fusionar errores tipográficos reparados.
        cols_dup = [c for c in df.columns if c.strip().lower() not in ["id", "index", "n", "nº", "n°", "no", "numero", "codigo", "código"]]
        if cols_dup:
            df = df.unique(subset=cols_dup, maintain_order=True)
        else:
            df = df.unique(maintain_order=True)

    # 4. Asegurar ID si no existe (al final, para que no interfiera al borrar duplicados)
    id_candidates = [c for c in df.columns if c.strip().lower() in ["id", "código", "codigo", "index"]]
    if not id_candidates:
        df = df.with_columns(pl.Series("ID", range(1, len(df) + 1)))
        id_col = "ID"
    else:
        id_col = id_candidates[0]

    # 5. Detectar columnas geográficas (Lógica Lugares)
    def get_col(keywords):
        return next((col for col in df.columns if any(k in col.lower() for k in keywords)), None)
        
    calle_col, num_col = get_col(["calle", "street", "direccion", "dirección", "ubicacion", "domicilio"]), get_col(["numero", "num", "number"])
    ciudad_col, pais_col = get_col(["ciudad", "city", "provincia", "estado", "state"]), get_col(["pais", "country"])
    lat_col, lon_col = get_col(["lat", "latitud"]), get_col(["lon", "longitud"])
    geo_col = get_col(["georeferencia", "coordenada", "coords", "geo", "gps"])
    nombre_col = get_col(["lugar", "nombre", "name", "place"])
    
    # [REGLA PARTE 2] Preservación de Georeferencias: Extracción Segura
    if geo_col and not (lat_col and lon_col):
        # Si la expresión regular falla en entender el formato, ahora no borrará nada, 
        # conservará el texto original completo en "latitud_extraida" para que no pierdas la info.
        df = df.with_columns([
            pl.when(pl.col(geo_col).str.contains(r"([-+]?\d+(?:\.\d+)?)[^\d\-+]+([-+]?\d+(?:\.\d+)?)"))
            .then(pl.col(geo_col).str.extract(r"([-+]?\d+(?:\.\d+)?)[^\d\-+]+([-+]?\d+(?:\.\d+)?)", 1))
            .otherwise(pl.col(geo_col)).alias("latitud_extraida"),
            
            pl.when(pl.col(geo_col).str.contains(r"([-+]?\d+(?:\.\d+)?)[^\d\-+]+([-+]?\d+(?:\.\d+)?)"))
            .then(pl.col(geo_col).str.extract(r"([-+]?\d+(?:\.\d+)?)[^\d\-+]+([-+]?\d+(?:\.\d+)?)", 2))
            .otherwise(pl.lit("NO ESPECIFICADO")).alias("longitud_extraida")
        ])
        lat_col, lon_col = "latitud_extraida", "longitud_extraida"

    tablas = {}
    
    # [REGLA PARTE 2] Tabla Direcciones: Desglosar la dirección completa (Regex Avanzado)
    def desglosar_dir(val):
        calle, num, ciudad, pais = "NO ESPECIFICADO", "NO ESPECIFICADO", "NO ESPECIFICADO", "NO ESPECIFICADO"
        if val and str(val).strip() and str(val) != "NO ESPECIFICADO":
            s = str(val).strip()
            # Dividir lo sobrante por comas para encontrar la ciudad y el país
            partes = [p.strip() for p in s.split(",") if p.strip()]
            
            calle_y_num = partes[0] if len(partes) > 0 else ""
            
            # Buscar el número sin perder el nombre de la calle sin importar de qué lado esté
            m_num = re.search(r"(\d+)", calle_y_num)
            if m_num:
                num = m_num.group(1)
                antes = calle_y_num[:m_num.start()].strip(" ,.-")
                despues = calle_y_num[m_num.end():].strip(" ,.-")
                # Si la calle está antes del número (ej: Kennedy 123) la usamos. Si está después (ej: 10 Downing) usamos la de después.
                calle = antes if antes else despues
            else:
                num = "NO ESPECIFICADO"
                calle = calle_y_num
            
            if len(partes) >= 3:
                pais = partes[-1]
                ciudad = partes[-2]
            elif len(partes) == 2:
                ciudad = partes[-1]
                
            # Limpieza estética: Evitar que diga "Calle Calle"
            calle = re.sub(r"(?i)^CALLE\s+", "", calle).strip()
            if not calle: calle = "NO ESPECIFICADO"
        return {"nombre_calle": calle, "numero_calle": num, "ciudad_estado_provincia": ciudad, "país": pais}

    # Si hay una columna de dirección amontonada, aplicamos la función de desglose inteligente
    if calle_col and not num_col and not ciudad_col:
        df_direcciones = df.select([
            pl.col(id_col).alias("ID"),
            pl.col(calle_col).map_elements(desglosar_dir, return_dtype=pl.Struct([
                pl.Field("nombre_calle", pl.String), pl.Field("numero_calle", pl.String),
                pl.Field("ciudad_estado_provincia", pl.String), pl.Field("país", pl.String)
            ])).alias("parsed")
        ]).unnest("parsed").unique(maintain_order=True)
    else:
        # Generación estándar si las columnas ya venían separadas
        df_direcciones = df.select([
            pl.col(id_col).alias("ID"), pl.col(calle_col).alias("nombre_calle") if calle_col else pl.lit("NO ESPECIFICADO").alias("nombre_calle"),
            pl.col(num_col).alias("numero_calle") if num_col else pl.lit("NO ESPECIFICADO").alias("numero_calle"),
            pl.col(ciudad_col).alias("ciudad_estado_provincia") if ciudad_col else pl.lit("NO ESPECIFICADO").alias("ciudad_estado_provincia"),
            pl.col(pais_col).alias("país") if pais_col else pl.lit("NO ESPECIFICADO").alias("país"),
        ]).unique(maintain_order=True)
    
    # [REGLA PARTE 2] Georeferencias y Lugares separados con su respectivo ID
    df_geo = df.select([pl.col(id_col).alias("ID"), pl.col(lat_col).alias("latitud") if lat_col else pl.lit("NO ESPECIFICADO").alias("latitud"), pl.col(lon_col).alias("longitud") if lon_col else pl.lit("NO ESPECIFICADO").alias("longitud")]).unique(maintain_order=True)
    df_lugares = df.select([pl.col(id_col).alias("ID"), pl.col(nombre_col).alias("nombre") if nombre_col else pl.lit("NO ESPECIFICADO").alias("nombre")]).unique(maintain_order=True)
    
    tablas["Lugares"] = df_lugares
    tablas["Georeferencias"] = df_geo
    tablas["Direcciones"] = df_direcciones
        
    # Siempre devolvemos la tabla unificada por si acaso
    df_principal = df.clone()
    tablas["Tabla_Principal_Normalizada"] = df_principal
    
    return tablas