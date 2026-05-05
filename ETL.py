import io
import unicodedata
import polars as pl
import streamlit as st
from fpdf import FPDF

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

# Aquí va la lógica principal de tu aplicación Streamlit
if __name__ == "__main__":
    st.title("Aplicación ETL - Limpieza de Datos")
    
    # Inicializamos un contador en la sesión para reiniciar el componente de subida
    if "uploader_key" not in st.session_state:
        st.session_state.uploader_key = 0
        
    uploaded_file = st.file_uploader("Sube tu archivo", type=["csv", "txt"], key=str(st.session_state.uploader_key))
    
    if uploaded_file is not None:
        st.success("Archivo cargado con éxito.")
        
        # Opción para que el usuario elija el separador correcto (evita que todo quede en una sola columna)
        separador = st.radio("Selecciona el separador de tu archivo:", [",", ";", "\\t (Tabulador)"], horizontal=True)
        # Limpiar la opción del tabulador para Polars
        separador_polars = "\t" if "Tabulador" in separador else separador

        try:
            # Streamlit guarda el archivo en memoria, Polars puede leerlo directamente
            df = pl.read_csv(uploaded_file, separator=separador_polars, infer_schema_length=10000)
            
            st.write(f"El archivo tiene **{df.height}** filas y **{df.width}** columnas.")
            st.write("Vista previa de los datos originales:")
            st.dataframe(df.head()) # Muestra solo las primeras filas para no saturar la pantalla
            
            st.write("---")
            st.write("### 🛠️ Selección de Columnas")
            # Multiselect que muestra todas las columnas por defecto
            columnas_seleccionadas = st.multiselect(
                "Desmarca las columnas que NO deseas incluir en tu archivo final:",
                options=df.columns,
                default=df.columns
            )
            
            if len(columnas_seleccionadas) == 0:
                st.warning("⚠️ Por favor, selecciona al menos una columna para continuar.")
            else:
                # Filtramos el DataFrame para que solo tenga las columnas que el usuario eligió
                df = df.select(columnas_seleccionadas)
                
                st.write("### ✏️ Renombrar Columnas (Opcional)")
                st.write("Haz doble clic en la celda de la derecha si deseas cambiar el nombre de una columna:")
                
                # Creamos una tabla interactiva para que el usuario edite los nombres
                nombres_datos = [{"Columna Original": c, "Nuevo Nombre": c} for c in columnas_seleccionadas]
                nombres_editados = st.data_editor(
                    nombres_datos,
                    disabled=["Columna Original"], # Evita que borren el nombre original por accidente
                    hide_index=True
                )
                # Extraemos los nuevos nombres y renombramos las columnas en Polars
                mapa_nombres = {row["Columna Original"]: row["Nuevo Nombre"] for row in nombres_editados}
                df = df.rename(mapa_nombres)
                
                st.write("⏳ Transformando los datos...")
                # ⚡ OPTIMIZACIÓN: Aplicamos la limpieza a todas las columnas de texto AL MISMO TIEMPO usando motor nativo
                columnas_texto = [limpiar_columna_polars(col) for col in df.columns if df[col].dtype == pl.String]
                
                if columnas_texto:
                    df = df.with_columns(columnas_texto)
                
                st.success("¡Datos transformados con éxito!")
                st.write("Vista previa de los primeros 100 datos limpios:")
                # Mostramos solo una muestra para no congelar el navegador web con 1.3 GB de datos
                st.dataframe(df.head(100))
                
                st.write("### Opciones de Descarga")
                
                # 1. PARQUET (Recomendado para Big Data)
                buffer_parquet = io.BytesIO()
                df.write_parquet(buffer_parquet)
                
                st.download_button(
                    label="📥 Descargar en Parquet (Rápido y ultraligero)",
                    data=buffer_parquet,
                    file_name="datos_limpios.parquet",
                    mime="application/octet-stream"
                )
                
                # Excel tiene un límite estricto de 1,048,576 filas.
                if df.height > 1048575:
                    st.warning(f"⚠️ Tu archivo tiene **{df.height}** filas. Excel solo soporta un máximo de 1,048,575 filas. Tienes la opción de descargarlo en CSV.")
                    csv_limpio = df.write_csv(separator=separador_polars)
                    
                    st.download_button(
                        label="📥 Descargar en CSV (Alternativa tradicional)",
                        data=csv_limpio,
                        file_name="datos_limpios.csv",
                        mime="text/csv"
                    )
                else:
                    # Convertir el DataFrame limpio a Excel (en memoria) para descargarlo
                    buffer = io.BytesIO()
                    df.write_excel(buffer)
                    
                    st.download_button(
                        label="📥 Descargar en Excel",
                        data=buffer,
                        file_name="datos_limpios.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            
            st.write("---")
            st.write("### 📄 Reporte de Datos")
            
            # Generar un reporte básico en PDF
            pdf = FPDF()
            pdf.add_page()
            pdf.set_font("helvetica", size=16)
            pdf.cell(200, 10, txt="Reporte de Limpieza de Datos", ln=1, align="C")
            
            pdf.set_font("helvetica", size=12)
            pdf.cell(200, 10, txt=f"Total de registros limpios: {df.height}", ln=1)
            pdf.cell(200, 10, txt=f"Total de columnas: {df.width}", ln=1)
            pdf.cell(200, 10, txt="Estructura de la tabla:", ln=1)
            
            pdf.set_font("helvetica", size=10)
            for col in df.columns:
                # Normalizamos el nombre por seguridad para que el PDF no falle con símbolos raros
                col_limpia = unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('utf-8')
                pdf.cell(200, 8, txt=f"- {col_limpia} (Tipo: {str(df[col].dtype)})", ln=1)
                
            # Compatibilidad segura para obtener el archivo en memoria (bytes)
            try:
                pdf_bytes = bytes(pdf.output())
            except TypeError:
                pdf_bytes = pdf.output(dest='S').encode('latin-1')
                
            st.download_button(
                label="📥 Descargar Reporte en PDF",
                data=pdf_bytes,
                file_name="reporte_datos.pdf",
                mime="application/pdf"
            )
            
        except Exception as e:
            st.error(f"Hubo un error al leer el archivo: {e}")
            
        st.write("---") # Línea divisoria visual
        if st.button("🔄 Limpiar todo y subir otro archivo"):
            st.session_state.uploader_key += 1
            st.rerun()