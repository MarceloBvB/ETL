import pytest
import polars as pl
from ETL import normalizar_texto, limpiar_columna_polars

def test_normalizar_texto_removes_accents():
    """Prueba que los acentos (tildes) se eliminen correctamente y el texto esté en mayúsculas."""
    assert normalizar_texto("áéíóú") == "AEIOU"
    assert normalizar_texto("ÁÉÍÓÚ") == "AEIOU"
    assert normalizar_texto("Valparaíso") == "VALPARAISO"
    assert normalizar_texto("Concepción") == "CONCEPCION"

def test_normalizar_texto_special_characters():
    """Prueba el comportamiento con caracteres especiales como 'ñ' y 'ü'."""
    assert normalizar_texto("ñandú") == "NANDU"
    assert normalizar_texto("pingüino") == "PINGUINO"
    assert normalizar_texto("Viña del Mar") == "VINA DEL MAR"

def test_normalizar_texto_handles_none_and_empty():
    """Prueba que los valores None y las cadenas vacías se manejen de forma segura sin errores."""
    assert normalizar_texto(None) == ""
    assert normalizar_texto("") == ""

def test_normalizar_texto_handles_numbers():
    """Prueba que los valores numéricos se conviertan a cadenas de texto de forma segura."""
    assert normalizar_texto(12345) == "12345"
    assert normalizar_texto(12.34) == "12.34"

def test_polars_dataframe_mapping():
    """Prueba la lógica exacta de mapeo de Polars utilizada en la aplicación Streamlit."""
    df = pl.DataFrame({
        "comunas": ["Río", "San José", None, "Ñuñoa"],
        "regiones": ["Los Ríos", "Valparaíso", "Biobío", ""]
    })
    
    columnas_texto = [limpiar_columna_polars(col) for col in df.columns if df[col].dtype == pl.String]
    if columnas_texto:
        df = df.with_columns(columnas_texto)
    
    assert df["comunas"].to_list() == ["RIO", "SAN JOSE", "", "NUNOA"]
    assert df["regiones"].to_list() == ["LOS RIOS", "VALPARAISO", "BIOBIO", ""]