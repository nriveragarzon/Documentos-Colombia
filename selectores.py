# Librerias
import os
import json
import snowflake.connector
from snowflake.snowpark import Session
import pandas as pd
import numpy as np
import time
import re
import warnings

###############################################################
# FUNCIONES PARA GENERAR LAS OPCIONES DE ELECCIÓN PARA USUARIOS
###############################################################

# Selector de continentes
def selector_continentes(session):
    """
    Esta función ejecuta una consulta SQL para obtener una lista de continentes distintos
    desde la base de datos de exportaciones en Snowflake y los devuelve como una lista de opciones ordenada.

    Parámetros:
    - session: objeto de conexión activo a Snowflake.

    Retorna:
    - opciones: Lista de continentes distintos ordenada alfabéticamente.
    """

    # 1. Construir la consulta SQL base
    query = """
    SELECT DISTINCT A.CONTINENTE
    FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.BASE_EXPORTACIONES AS A
    WHERE A.CONTINENTE NOT IN ('No Declarados')
    ORDER BY 1 ASC
    """

    # 2. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas
    data = session.sql(query).collect()

    # 3. Convertir los resultados en una lista de opciones ordenada
    opciones = sorted({row['CONTINENTE'] for row in data})

    # 4. Devolver la lista de opciones
    return opciones

# Selector de HUBS
def selector_hubs(session):
    """
    Esta función ejecuta una consulta SQL para obtener una lista de hubs distintos
    desde la base de datos de exportaciones en Snowflake y los devuelve como una lista de opciones ordenada.

    Parámetros:
    - session: objeto de conexión activo a Snowflake.

    Retorna:
    - opciones: Lista de hubs distintos ordenada alfabéticamente.
    """

    # 1. Construir la consulta SQL base
    query = """
    SELECT DISTINCT A.HUB
    FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.BASE_EXPORTACIONES AS A
    ORDER BY 1 ASC
    """

    # 2. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas
    data = session.sql(query).collect()

    # 3. Convertir los resultados en una lista de opciones ordenada
    opciones = sorted({row['HUB'] for row in data})

    # 4. Devolver la lista de opciones
    return opciones

# Selector de países
def selector_paises(session, continentes):
    """
    Esta función ejecuta una consulta SQL para obtener una lista de países distintos
    desde la base de datos de exportaciones en Snowflake y los devuelve como una lista de opciones ordenada.

    Parámetros:
    - session: objeto de conexión activo a Snowflake.
    - continentes: lista de strings con los continentes seleccionados para filtrar los países de interés.

    Retorna:
    - opciones: Lista de países distintos ordenada alfabéticamente.
    """
    # 0. Transformar lista de continentes 
    continente_pais_list = [continentes] if continentes else []

    # 1. Construir la consulta SQL base
    query = """
    SELECT DISTINCT A.PAIS_DESTINO
    FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.BASE_EXPORTACIONES AS A
    WHERE 1=1
    """
    # Agrupación geográfica: 
    if continentes:
        query += f""" AND A.CONTINENTE IN ({','.join([f"'{continente}'" for continente in continente_pais_list])})"""

    # 2. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas
    data = session.sql(query).collect()

    # 3. Convertir los resultados en una lista de opciones ordenada
    opciones = sorted({row['PAIS_DESTINO'] for row in data})

    # 4. Devolver la lista de opciones
    return opciones

# Selector de departamentos
def selector_departamento(session):
    """
    Esta función ejecuta una consulta SQL para obtener una lista de departamentos distintos
    desde la base de datos de exportaciones en Snowflake y los devuelve como una lista de opciones ordenada.

    Parámetros:
    - session: objeto de conexión activo a Snowflake.

    Retorna:
    - opciones: Lista de hubs distintos ordenada alfabéticamente.
    """

    # 1. Construir la consulta SQL base
    query = """
    SELECT DISTINCT A.DEPARTAMENTO_ORIGEN
    FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.BASE_EXPORTACIONES AS A
    ORDER BY 1 ASC
    """

    # 2. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas
    data = session.sql(query).collect()

    # 3. Convertir los resultados en una lista de opciones ordenada
    opciones = sorted({row['DEPARTAMENTO_ORIGEN'] for row in data})

    # 4. Devolver la lista de opciones
    return opciones