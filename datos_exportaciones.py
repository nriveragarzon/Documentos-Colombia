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


#######################################################################
# FUNCIONES PARA IMPORTACIÓN DE DATOS Y TRANSFORMACIÓN DE EXPORTACIONES
#######################################################################

# Función para obtener los datos de exportaciones agrupados
def get_data_exportaciones(session, continentes=None, zonas_geograficas=None, paises=None, departamentos=None, hubs=None,tlcs=None, tipo_tlcss=None, tipos=None, years=None):
    """
    Extrae datos de exportaciones desde Snowflake aplicando filtros específicos y agrupa los resultados.

    Parámetros:
    session (snowflake.snowpark.Session): Sesión activa en Snowflake.
    continentes (list): Lista de continentes a filtrar.
    zonas_geograficas (list): Lista de zonas geográficas a filtrar.
    paises (list): Lista de países a filtrar.
    departamentos (list): Lista de departamentos a filtrar.
    hubs (list): Lista de hubs a filtrar.
    tlcs (list): Lista de tratados de libre comercio a filtrar.
    tipo_tlcss (list): Lista de tipos de acuerdos comerciales a filtrar.
    tipos (list): Lista de tipos de posición arancelaria a filtrar.
    years (list): Lista de años a filtrar.

    Pasos del proceso:
    1. Verificar que los parámetros son listas o None.
    2. Construir la consulta SQL base.
    3. Añadir condiciones a la consulta SQL según los parámetros proporcionados.
    4. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas.
    5. Devolver el DataFrame resultante.

    Retorna:
    DataFrame: DataFrame con los datos de exportaciones filtrados y agrupados.
    """
    
    # 1. Verificar que los parámetros son listas o None
    for param in [tipos, continentes, zonas_geograficas, paises, departamentos, hubs, tlcs, tipo_tlcss, years]:
        if param is not None and not isinstance(param, list):
            raise ValueError("Todos los parámetros deben ser listas o None")
        
    # 2. Construir la consulta SQL base
    query = """
    SELECT A.TIPO,
        A.CADENA,
        A.SECTOR,
        A.SUBSECTOR,
        A.PAIS_DESTINO,
        A.HUB,
        A.CONTINENTE,
        A.ZONA_GEOGRAFICA,
        A.TLCS,
        A.TIPO_ACUERDO,
        A.DEPARTAMENTO_ORIGEN,
        A.MEDIO_TRANSPORTE,
        A.CADENA_FRIO,
        A.TIPO_ESTRELLA,
        A.CADENA_ESTRELLA,
        A.SECTOR_ESTRELLA,
        A.SUBSECTOR_ESTRELLA,
        A.DPTO_MAS_EXPORTA_ESTRELLA,
        A.YEAR,
        SUM(A.VALOR_USD) AS VALOR_USD, 
        SUM(A.PESO_KG) AS PESO_KG_NETO
    FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.BASE_EXPORTACIONES AS A
    WHERE 1=1
    """
    
    # 3. Añadir condiciones a la consulta SQL según los parámetros proporcionados

    # Agrupación geográfica: 
    if continentes:
        query += f""" AND A.CONTINENTE IN ({','.join([f"'{continente}'" for continente in continentes])})"""
    if zonas_geograficas:
        query += f""" AND A.ZONA_GEOGRAFICA IN ({','.join([f"'{zona}'" for zona in zonas_geograficas])})"""
    if paises:
        query += f""" AND A.PAIS_DESTINO IN ({','.join([f"'{pais}'" for pais in paises])})"""
    if departamentos:
        query += f""" AND A.DEPARTAMENTO_ORIGEN IN ({','.join([f"'{departamento}'" for departamento in departamentos])})"""
        
    # Agrupación administrativa:
    if hubs:
        query += f""" AND A.HUB IN ({','.join([f"'{hub}'" for hub in hubs])})"""
    
    # Acuerdo comercial:
    if tlcs:
        query += f""" AND A.TLCS IN ({','.join([f"'{tlc}'" for tlc in tlcs])})"""
    if tipo_tlcss:
        query += f""" AND A.TIPO_ACUERDO IN ({','.join([f"'{tipo_tlcs}'" for tipo_tlcs in tipo_tlcss])})"""
    
    # Posición arancelaria:
    if tipos:
        query += f""" AND A.TIPO IN ({','.join([f"'{tipo}'" for tipo in tipos])})"""

    # Agrupación de fechas (dos fechas siempre):
    if years:
        query += f""" AND A.YEAR IN ({','.join([f"'{year}'" for year in years])})"""
    
    # Agregar group by 
    query += """
    GROUP BY A.TIPO,
        A.CADENA,
        A.SECTOR,
        A.SUBSECTOR,
        A.PAIS_DESTINO,
        A.HUB,
        A.CONTINENTE,
        A.ZONA_GEOGRAFICA,
        A.TLCS,
        A.TIPO_ACUERDO,
        A.DEPARTAMENTO_ORIGEN,
        A.MEDIO_TRANSPORTE,
        A.CADENA_FRIO,
        A.TIPO_ESTRELLA,
        A.CADENA_ESTRELLA,
        A.SECTOR_ESTRELLA,
        A.SUBSECTOR_ESTRELLA,
        A.DPTO_MAS_EXPORTA_ESTRELLA,
        A.YEAR
    """
    
    # 4. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas
    data = pd.DataFrame(session.sql(query).collect())
    
    # 5. Devolver el DataFrame resultante
    return data

# Función para obtener el número de empresas
def get_data_exportaciones_numero_empresas(session, continentes=None, zonas_geograficas=None, paises=None, departamentos=None, hubs=None, tlcs=None, tipo_tlcss=None, tipos=None, years=None, umbral=10000):
    """
    Extrae datos de exportaciones desde Snowflake aplicando filtros específicos y cuenta el número de empresas únicas que superan un umbral de exportación.

    Parámetros:
    session (snowflake.snowpark.Session): Sesión activa en Snowflake.
    continentes (list): Lista de continentes a filtrar.
    zonas_geograficas (list): Lista de zonas geográficas a filtrar.
    paises (list): Lista de países a filtrar.
    departamentos (list): Lista de departamentos a filtrar.
    hubs (list): Lista de hubs a filtrar.
    tlcs (list): Lista de tratados de libre comercio a filtrar.
    tipo_tlcss (list): Lista de tipos de acuerdos comerciales a filtrar.
    tipos (list): Lista de tipos de posición arancelaria a filtrar.
    years (list): Lista de años a filtrar.
    umbral (int): Umbral mínimo de exportación en USD para considerar una empresa.

    Pasos del proceso:
    1. Verificar que los parámetros son listas o None.
    2. Construir la consulta SQL base.
    3. Añadir condiciones a la consulta SQL según los parámetros proporcionados.
    4. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas.
    5. Filtrar el DataFrame para empresas que superan el umbral de exportación.
    6. Contar el número de empresas únicas.
    7. Devolver el número de empresas únicas.

    Retorna:
    int: Número de empresas únicas que superan el umbral de exportación.
    """
    
    # 1. Verificar que los parámetros son listas o None
    for param in [continentes, zonas_geograficas, paises, departamentos, hubs, tlcs, tipo_tlcss, tipos, years]:
        if param is not None and not isinstance(param, list):
            raise ValueError("Todos los parámetros deben ser listas o None")
        
    # 2. Construir la consulta SQL base
    query = """
    SELECT A.NIT_EXPORTADOR,
        A.RAZON_SOCIAL,
        A.SECTOR_ESTRELLA,
        A.YEAR, 
        SUM(A.VALOR_USD) AS VALOR_USD
    FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.BASE_EXPORTACIONES AS A
    WHERE A.TIPO = 'No Mineras' 
        AND A.TIPO_ESTRELLA = 'No Mineras' 
        AND A.CADENA_ESTRELLA IN ('Agroalimentos', 'Industrias 4.0', 'Metalmecánica y Otras Industrias', 'Químicos y Ciencias de la Vida', 'Sistema Moda')
        AND A.NIT_EXPORTADOR NOT IN ('-1')
    """

    # 3. Añadir condiciones a la consulta SQL según los parámetros proporcionados

    # Agrupación geográfica: 
    if continentes:
        query += f""" AND A.CONTINENTE IN ({','.join([f"'{continente}'" for continente in continentes])})"""
    if zonas_geograficas:
        query += f""" AND A.ZONA_GEOGRAFICA IN ({','.join([f"'{zona}'" for zona in zonas_geograficas])})"""
    if paises:
        query += f""" AND A.PAIS_DESTINO IN ({','.join([f"'{pais}'" for pais in paises])})"""
    if departamentos:
        query += f""" AND A.DPTO_MAS_EXPORTA_ESTRELLA IN ({','.join([f"'{departamento}'" for departamento in departamentos])})"""
        
    # Agrupación administrativa:
    if hubs:
        query += f""" AND A.HUB IN ({','.join([f"'{hub}'" for hub in hubs])})"""
    
    # Acuerdo comercial:
    if tlcs:
        query += f""" AND A.TLCS IN ({','.join([f"'{tlc}'" for tlc in tlcs])})"""
    if tipo_tlcss:
        query += f""" AND A.TIPO_ACUERDO IN ({','.join([f"'{tipo_tlcs}'" for tipo_tlcs in tipo_tlcss])})"""
    
    # Posición arancelaria:
    if tipos:
        query += f""" AND A.TIPO IN ({','.join([f"'{tipo}'" for tipo in tipos])})"""

    # Agrupación de fechas (dos fechas siempre):
    if years:
        query += f""" AND A.YEAR IN ({','.join([f"'{year}'" for year in years])})"""
    
    # 4. Añadir group by
    query += """
    GROUP BY A.NIT_EXPORTADOR,
        A.RAZON_SOCIAL,
        A.SECTOR_ESTRELLA,
        A.YEAR;
    """

    # 5. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas
    data = pd.DataFrame(session.sql(query).collect())

    # 6. Filtrar el DataFrame para empresas que superan el umbral de exportación
    dfcuenta = data[data['VALOR_USD'] > umbral]

    # 7. Contar el número de empresas únicas
    empresas_unicas = dfcuenta['NIT_EXPORTADOR'].nunique()

    # 8. Devolver el número de empresas únicas
    return empresas_unicas

# Función para obtener los datos de empresas con NIT, nombre y sector estrella
def get_data_exportaciones_empresas(session, continentes=None,zonas_geograficas=None, paises=None, departamentos=None, hubs=None, tlcs=None, tipo_tlcss=None, tipos=None, years=None):
    """
    Extrae datos de exportaciones desde Snowflake aplicando filtros específicos y obtiene información de empresas y totales de exportación.

    Parámetros:
    session (snowflake.snowpark.Session): Sesión activa en Snowflake.
    continentes (list): Lista de continentes a filtrar.
    zonas_geograficas (list): Lista de zonas geográficas a filtrar.
    paises (list): Lista de países a filtrar.
    departamentos (list): Lista de departamentos a filtrar.
    hubs (list): Lista de hubs a filtrar.
    tlcs (list): Lista de tratados de libre comercio a filtrar.
    tipo_tlcss (list): Lista de tipos de acuerdos comerciales a filtrar.
    tipos (list): Lista de tipos de posición arancelaria a filtrar.
    years (list): Lista de años a filtrar.
    umbral (int): Umbral mínimo de exportación en USD para considerar una empresa.

    Pasos del proceso:
    1. Verificar que los parámetros son listas o None.
    2. Construir la consulta SQL para datos de empresas.
    3. Añadir condiciones a la consulta SQL según los parámetros proporcionados.
    4. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas.
    5. Construir la consulta SQL para datos totales.
    6. Añadir condiciones a la consulta SQL según los parámetros proporcionados.
    7. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas.
    8. Devolver los DataFrames de empresas y totales.

    Retorna:
    tuple: DataFrames con los datos de empresas y los totales de exportación.
    """
    
    # 1. Verificar que los parámetros son listas o None
    for param in [continentes, zonas_geograficas, paises, departamentos, hubs, tlcs, tipo_tlcss, tipos, years]:
        if param is not None and not isinstance(param, list):
            raise ValueError("Todos los parámetros deben ser listas o None")
        
    # 2. Construir la consulta SQL de datos de empresas
    query1 = """
    SELECT A.NIT_EXPORTADOR,
        A.RAZON_SOCIAL,
        A.SECTOR_ESTRELLA,
        A.YEAR, 
        SUM(A.Valor_USD) AS VALOR_USD
    FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.BASE_EXPORTACIONES AS A
    WHERE A.TIPO = 'No Mineras' 
        AND A.TIPO_ESTRELLA = 'No Mineras' 
        AND A.NIT_EXPORTADOR NOT IN ('-1')
    """
    
    # 3. Añadir condiciones a la consulta SQL según los parámetros proporcionados

    # Agrupación geográfica: 
    if continentes:
        query1 += f""" AND A.CONTINENTE IN ({','.join([f"'{continente}'" for continente in continentes])})"""
    if zonas_geograficas:
        query1 += f""" AND A.ZONA_GEOGRAFICA IN ({','.join([f"'{zona}'" for zona in zonas_geograficas])})"""
    if paises:
        query1 += f""" AND A.PAIS_DESTINO IN ({','.join([f"'{pais}'" for pais in paises])})"""
    if departamentos:
        query1 += f""" AND A.DPTO_MAS_EXPORTA_ESTRELLA IN ({','.join([f"'{departamento}'" for departamento in departamentos])})"""
        
    # Agrupación administrativa:
    if hubs:
        query1 += f""" AND A.HUB IN ({','.join([f"'{hub}'" for hub in hubs])})"""
    
    # Acuerdo comercial:
    if tlcs:
        query1 += f""" AND A.TLCS IN ({','.join([f"'{tlc}'" for tlc in tlcs])})"""
    if tipo_tlcss:
        query1 += f""" AND A.TIPO_ACUERDO IN ({','.join([f"'{tipo_tlcs}'" for tipo_tlcs in tipo_tlcss])})"""
    
    # Posición arancelaria:
    if tipos:
        query1 += f""" AND A.TIPO IN ({','.join([f"'{tipo}'" for tipo in tipos])})"""

    # Agrupación de fechas (dos fechas siempre):
    if years:
        query1 += f""" AND A.YEAR IN ({','.join([f"'{year}'" for year in years])})"""    
    
    # Añadir group by
    query1 += """
    GROUP BY A.NIT_EXPORTADOR,
        A.RAZON_SOCIAL,
        A.SECTOR_ESTRELLA,
        A.YEAR;
    """

    # 4. Construir la consulta SQL del total
    query2 = """
    SELECT A.YEAR, 
        SUM(A.Valor_USD) AS VALOR_USD
    FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.BASE_EXPORTACIONES AS A
    WHERE A.TIPO = 'No Mineras' 
    """
    
    # 5. Añadir condiciones a la consulta SQL según los parámetros proporcionados

    # Agrupación geográfica: 
    if continentes:
        query2 += f""" AND A.CONTINENTE IN ({','.join([f"'{continente}'" for continente in continentes])})"""
    if zonas_geograficas:
        query2 += f""" AND A.ZONA_GEOGRAFICA IN ({','.join([f"'{zona}'" for zona in zonas_geograficas])})"""
    if paises:
        query2 += f""" AND A.PAIS_DESTINO IN ({','.join([f"'{pais}'" for pais in paises])})"""
    if departamentos:
        query2 += f""" AND A.DPTO_MAS_EXPORTA_ESTRELLA IN ({','.join([f"'{departamento}'" for departamento in departamentos])})"""
        
    # Agrupación administrativa:
    if hubs:
        query2 += f""" AND A.HUB IN ({','.join([f"'{hub}'" for hub in hubs])})"""
    
    # Acuerdo comercial:
    if tlcs:
        query2 += f""" AND A.TLCS IN ({','.join([f"'{tlc}'" for tlc in tlcs])})"""
    if tipo_tlcss:
        query2 += f""" AND A.TIPO_ACUERDO IN ({','.join([f"'{tipo_tlcs}'" for tipo_tlcs in tipo_tlcss])})"""
    
    # Posición arancelaria:
    if tipos:
        query2 += f""" AND A.TIPO IN ({','.join([f"'{tipo}'" for tipo in tipos])})"""

    # Agrupación de fechas (dos fechas siempre):
    if years:
        query2 += f""" AND A.YEAR IN ({','.join([f"'{year}'" for year in years])})"""    
    
    # Añadir group by
    query2 += """
    GROUP BY A.YEAR;
    """
    
    # 6. Ejecutar la consulta SQL de empresas
    df_empresas = pd.DataFrame(session.sql(query1).collect())

    # 7. Ejecutar la consulta SQL de totales
    df_totales = pd.DataFrame(session.sql(query2).collect())
    
    # 8. Devolver los DataFrames de empresas y totales
    return df_empresas, df_totales

# Función para transformar el año para nombres de columnas
def transform_year_column_name(col_name):
    """
    Transforma el nombre de una columna de año para un formato más amigable.
    
    Parámetros:
    col_name (str): Nombre de la columna que contiene el año y el período.

    Pasos del proceso:
    1. Verifica si el nombre de la columna contiene paréntesis.
    2. Si contiene paréntesis, separa el año y el período.
    3. Formatea el nombre de la columna en el nuevo formato 'período año'.
    4. Si no contiene paréntesis, retorna el nombre original de la columna.

    Retorna:
    str: El nombre de la columna transformado si contiene paréntesis, de lo contrario, el nombre original.
    """
    
    # Verificar si el nombre de la columna contiene paréntesis
    if '(' in col_name and ')' in col_name:
        # Separar el año y el período
        year, period = col_name.split('(')
        period = period.replace(')', '')
        
        # Formatear el nombre de la columna en el nuevo formato 'período año'
        return f'{period} {year.strip()}'
    
    # Si no contiene paréntesis, retornar el nombre original de la columna
    return col_name

# Función para extraer el año de periodos corridos
def transform_year(col_name):
    """
    Extrae el año de un nombre de columna que contiene el año y el período.
    
    Parámetros:
    col_name (str): Nombre de la columna que contiene el año y el período.

    Pasos del proceso:
    1. Verifica si el nombre de la columna contiene paréntesis.
    2. Si contiene paréntesis, separa el año y el período.
    3. Retorna el año extraído.
    4. Si no contiene paréntesis, retorna el nombre original.

    Retorna:
    str: El año extraído si contiene paréntesis, de lo contrario, el nombre original.
    """
    
    # Verificar si el nombre de la columna contiene paréntesis
    if '(' in col_name and ')' in col_name:
        # Separar el año y el período
        year, period = col_name.split('(')
        return year.strip()
    
    # Si no contiene paréntesis, retornar el nombre original de la columna
    return f'{col_name}'

# Función para obtener tabla resumen por categoría elegida
def generar_tabla_resumen(df, categoria, valor, top_n = None):

    """
    Genera una tabla resumen a partir de un DataFrame de exportaciones, agregando columnas de variación porcentual y participación.
    
    Parámetros:
    df (DataFrame): DataFrame proveniente de la función get_data_exportaciones().
    categoria (str): Variable categórica para la cual se generará la tabla de resumen (e.g., 'PAIS_DESTINO', 'SECTOR').
    valor (str): Variable de valor a agregar ('VALOR_USD' o PESO_KG_NETO).
    top_n: Número de categorías top a filtrar.Por defecto no se filtran.

    Pasos del proceso:
    1. Crear una tabla pivote sumando el valor por categoría y año.
    2. Ordenar las columnas de año para manejar períodos correctamente.
    3. Filtrar el top 5 y agrupar los demás en 'Otros'.
    4. Agregar una fila con los totales generales.
    5. Calcular la variación entre el penúltimo y el último año.
    6. Calcular la participación en el último año.
    7. Reordenar las columnas para tener los años, la variación y la participación.
    8. Ordenar categorías por valor del último año, excepto 'Otros' y 'Total'.
    9. Formatear los valores y porcentajes.
    10. Asegurar que los nombres de las columnas no tengan múltiples niveles.
    11. Renombrar la columna de categoría.
    12. Transformar los nombres de las columnas de año y agrega formatos decimales en español.

    Retorna:
    DataFrame: Tabla resumen con las categorías ordenadas, variación y participación calculadas y formateadas.
    """

    # 1. Crear tabla pivote sumando el valor por categoría y año
    pivot_table = df.pivot_table(values=valor, index=categoria, columns='YEAR', aggfunc='sum', fill_value=0)

    # Ordenar las columnas de año para manejar períodos correctamente
    ordered_years = sorted(pivot_table.columns, key=lambda x: (int(x.split('(')[0]) if '(' in x else int(x)))
    pivot_table = pivot_table[ordered_years]

    # 2. Filtrar el top_n y agrupar los demás en 'Otros'
    if top_n:
        # Obtener las 5 categorías principales basadas en el valor del último año disponible
        topn = pivot_table.nlargest(top_n, ordered_years[-1])
        # Agrupar las demás categorías bajo 'Otros'
        others = pivot_table.drop(topn.index)
        others_sum = others.sum().to_frame().T
        others_sum[categoria] = 'Otros'
        pivot_table = pd.concat([topn, others_sum.set_index(categoria)])

    
    # 3. Agregar fila con los totales generales
    totals = pivot_table.sum().to_frame().T
    totals[categoria] = 'Total'
    pivot_table = pd.concat([pivot_table, totals.set_index(categoria)])

    # 4. Calcular la variación entre años
    year_prev = ordered_years[-2]
    year_curr = ordered_years[-1]
    pivot_table['Variación (%)'] = (pivot_table[year_curr] - pivot_table[year_prev]) / pivot_table[year_prev] * 100

    # 5. Calcular la participación en el último año
    total_last_year = pivot_table.loc['Total', year_curr]
    pivot_table[f'Participación {year_curr} (%)'] = (pivot_table[year_curr] / total_last_year) * 100

    # 6. Reordenar las columnas para tener los años, la variación y la participación
    ordered_columns = ordered_years + ['Variación (%)', f'Participación {year_curr} (%)']
    pivot_table = pivot_table[ordered_columns]

    # 7. Ordenar categorías por valor del último año, excepto 'Otros' y 'Total'
    pivot_table = pivot_table.reset_index()
    pivot_table = pivot_table.sort_values(by=year_curr, ascending=False)
    if 'Otros' in pivot_table[categoria].values:
        otros_row = pivot_table[pivot_table[categoria] == 'Otros']
        pivot_table = pivot_table[pivot_table[categoria] != 'Otros']
        pivot_table = pd.concat([pivot_table, otros_row])
    total_row = pivot_table[pivot_table[categoria] == 'Total']
    pivot_table = pivot_table[pivot_table[categoria] != 'Total']
    pivot_table = pd.concat([pivot_table, total_row])
    pivot_table = pivot_table.set_index(categoria)

    # 8. Formatear los valores y porcentajes
    pivot_table[ordered_years] = pivot_table[ordered_years].applymap('{:,.0f}'.format)
    pivot_table[['Variación (%)', f'Participación {year_curr} (%)']] = pivot_table[['Variación (%)', f'Participación {year_curr} (%)']].applymap('{:.1f}%'.format)

    # 9. Asegurar que los nombres de las columnas no tengan múltiples niveles
    pivot_table.columns.name = None
    pivot_table.reset_index(inplace=True)

    # 10. Renombrar la columna de categoría
    column_names_dict = {
        'TIPO': 'Tipo de exportación',
        'CADENA': 'Cadena',
        'SECTOR': 'Sector',
        'SUBSECTOR': 'Subsector',
        'PAIS_DESTINO': 'País destino',
        'HUB': 'HUB',
        'CONTINENTE': 'Continente',
        'ZONA_GEOGRAFICA': 'Zona geográfica',
        'TLCS': 'Tratados de Libre Comercio',
        'DEPARTAMENTO_ORIGEN': 'Departamento de origen',
        'MEDIO_TRANSPORTE': 'Medio de transporte',
        'YEAR': 'Año'}
    if categoria in column_names_dict:
        pivot_table.rename(columns={categoria: column_names_dict[categoria]}, inplace=True)

    # 11. Transformar los nombres de las columnas de año
    new_columns = {}
    for col in pivot_table.columns:
        if 'Participación' in col:
            new_columns[col] = f'Participación {transform_year(year_curr)} (%)'
        else:
            new_columns[col] = col
    pivot_table.rename(columns=new_columns, inplace=True)  

    # 12. Agregar USD FOB a las columnas de años
    if valor == 'VALOR_USD':
        pivot_table.rename(columns={pivot_table.columns[1]: f'{transform_year_column_name(pivot_table.columns[1])} (USD FOB)'}, inplace=True)
        pivot_table.rename(columns={pivot_table.columns[2]: f'{transform_year_column_name(pivot_table.columns[2])} (USD FOB)'}, inplace=True)
    if valor == 'PESO_KG_NETO':
        pivot_table.rename(columns={pivot_table.columns[1]: f'{transform_year_column_name(pivot_table.columns[1])} (KG NETO)'}, inplace=True)
        pivot_table.rename(columns={pivot_table.columns[2]: f'{transform_year_column_name(pivot_table.columns[2])} (KG NETO)'}, inplace=True)

    # Convertir separadores de decimales y miles
    pivot_table = pivot_table.astype(str)
    for col in pivot_table.columns[1:]:
        pivot_table[col] = pivot_table[col].apply(lambda x: x.replace(',', 'X').replace('.', ',').replace('X', '.'))
    
    # 13. Resultado
    return pivot_table

# Función para generar la tabla de empresas con NITs, razón social y sector estrella
def generar_tabla_empresas(df_empresas, df_totales, year1, year2):
    """
    Genera un resumen completo de exportaciones por NIT, incluyendo los valores de exportación de los años especificados,
    la variación porcentual entre esos años y la participación en el último año. También agrega filas para 'Otros' 
    y 'Total' en la tabla final.

    Parámetros:
    df_empresas (DataFrame): DataFrame con los datos de exportaciones por empresa.
    df_totales (DataFrame): DataFrame con los totales de exportación por año.
    year1 (int): Año inicial para el cálculo de variaciones.
    year2 (int): Año final para el cálculo de variaciones.

    Pasos del proceso:
    1. Crear una tabla pivote con los valores de exportación por empresa y año.
    2. Asegurarse de que los años especificados existen en la tabla pivote.
    3. Calcular la participación porcentual del último año.
    4. Calcular la variación porcentual entre los años especificados.
    5. Resetear el índice de la tabla pivote para facilitar su manejo.
    6. Seleccionar las columnas deseadas para el resumen.
    7. Filtrar el top 5 de empresas y agrupar las demás en 'Otros'.
    8. Crear la fila de totales de exportación.
    9. Calcular los valores de 'Otros' restando los del top 5 a los totales.
    10. Combinar las filas de top 5, 'Otros' y 'Total' en el DataFrame final.
    11. Renombrar las columnas y formatear los valores y porcentajes.
    
    Retorna:
    DataFrame: DataFrame con el resumen completo de exportaciones.
    """
    
    # 1. Crear la tabla pivote con los valores de exportación por empresa y año
    df_pivot = df_empresas.pivot_table(values='VALOR_USD', index=['NIT_EXPORTADOR', 'RAZON_SOCIAL', 'SECTOR_ESTRELLA'], columns='YEAR', aggfunc='sum', fill_value=0)
    
    # 2. Asegurarse de que los años especificados existen en la tabla pivote
    if year1 not in df_pivot.columns:
        df_pivot[year1] = 0
    if year2 not in df_pivot.columns:
        df_pivot[year2] = 0
    
    # 3. Calcular la participación porcentual del último año
    total_year2 = df_pivot[year2].sum()
    df_pivot['Participación (%)'] = (df_pivot[year2] / total_year2) * 100

    # 4. Calcular la variación porcentual entre los años especificados
    df_pivot['Variación (%)'] = ((df_pivot[year2] - df_pivot[year1]) / df_pivot[year1].replace(0, np.nan)) * 100
    
    # 5. Resetear el índice de la tabla pivote para facilitar su manejo
    df_pivot.reset_index(inplace=True)
    
    # 6. Seleccionar las columnas deseadas para el resumen
    df_pivot = df_pivot[['NIT_EXPORTADOR', 'RAZON_SOCIAL', 'SECTOR_ESTRELLA', year1, year2, 'Variación (%)', 'Participación (%)']]
    
    # 7. Filtrar el top 5 de empresas y agrupar las demás en 'Otros'
    top5 = df_pivot.nlargest(5, year2)
    
    # 8. Crear la fila de totales de exportación
    total_row = df_totales[df_totales['YEAR'] == year2].copy()
    total_row['NIT_EXPORTADOR'] = 'Total'
    total_row['RAZON_SOCIAL'] = ''
    total_row['SECTOR_ESTRELLA'] = ''
    total_row['Variación (%)'] = 100.0
    total_row['Participación (%)'] = 100.0
    
    # Asegurarse de que los años existen en el DataFrame de totales
    if year1 not in df_totales['YEAR'].values:
        total_row[year1] = 0
    else:
        total_row[year1] = df_totales[df_totales['YEAR'] == year1]['VALOR_USD'].values[0]
    
    total_row[year2] = df_totales[df_totales['YEAR'] == year2]['VALOR_USD'].values[0]
    
    total_row = total_row[['NIT_EXPORTADOR', 'RAZON_SOCIAL', 'SECTOR_ESTRELLA', year1, year2, 'Variación (%)', 'Participación (%)']]
    
    # Convertir total_row en DataFrame si es una serie
    if isinstance(total_row, pd.Series):
        total_row = total_row.to_frame().T
    
    # 9. Calcular los valores de 'Otros' restando los del top 5 a los totales
    others_sum = total_row.copy()
    others_sum.iloc[0, others_sum.columns.get_loc(year1)] -= top5[year1].sum()
    others_sum.iloc[0, others_sum.columns.get_loc(year2)] -= top5[year2].sum()
    others_sum['NIT_EXPORTADOR'] = 'Otros'
    others_sum['RAZON_SOCIAL'] = ''
    others_sum['SECTOR_ESTRELLA'] = ''
    others_sum['Participación (%)'] = (others_sum[year2] / total_row[year2].values[0]) * 100
    others_sum['Variación (%)'] = ((others_sum[year2] - others_sum[year1]) / others_sum[year1].replace(0, np.nan)) * 100
    
    # 10. Combinar las filas de top 5, 'Otros' y 'Total' en el DataFrame final
    final_df = pd.concat([top5, others_sum, total_row], ignore_index=True)
    
    # 11. Renombrar las columnas y formatear los valores y porcentajes
    column_names_dict = {
        'NIT_EXPORTADOR': 'NIT',
        'RAZON_SOCIAL': 'Empresa',
        'SECTOR_ESTRELLA': 'Sector',
        year1: f'{transform_year_column_name(year1)} (USD FOB)',
        year2: f'{transform_year_column_name(year2)} (USD FOB)',
        'Variación (%)': 'Variación (%)',
        'Participación (%)': f'Participación {transform_year(year2)} (%)'
    }
    final_df.rename(columns=column_names_dict, inplace=True)
    
    # Formatear los valores y porcentajes
    final_df[f'{transform_year_column_name(year1)} (USD FOB)'] = final_df[f'{transform_year_column_name(year1)} (USD FOB)'].map('{:,.0f}'.format)
    final_df[f'{transform_year_column_name(year2)} (USD FOB)'] = final_df[f'{transform_year_column_name(year2)} (USD FOB)'].map('{:,.0f}'.format)
    final_df['Variación (%)'] = final_df['Variación (%)'].map('{:.1f}%'.format)
    final_df[f'Participación {transform_year(year2)} (%)'] = final_df[f'Participación {transform_year(year2)} (%)'].map('{:.1f}%'.format)

    # Convertir separadores de decimales y miles
    final_df = final_df.astype(str)
    for col in final_df.columns[3:]:
        final_df[col] = final_df[col].apply(lambda x: x.replace(',', 'X').replace('.', ',').replace('X', '.'))
    
    return final_df

# Función para generar la tabla de subsectores con mayor crecimiento
def generar_tabla_subsectores(df, year1, year2):
    """
    Genera una tabla de resumen de exportaciones por subsector para dos años específicos,
    incluyendo la diferencia y variación porcentual entre esos años, y filtra el top 5 de subsectores 
    con mayor diferencia.

    Parámetros:
    df (DataFrame): DataFrame con los datos de exportaciones.
    year1 (int): Año inicial para el cálculo de diferencias y variaciones.
    year2 (int): Año final para el cálculo de diferencias y variaciones.

    Pasos del proceso:
    1. Filtrar los datos por los años especificados.
    2. Pivotear los datos para tener los valores de exportación por año y subsector.
    3. Asegurarse de que los años especificados existan en el DataFrame.
    4. Unir las tablas de ambos años.
    5. Calcular los totales por subsector y año.
    6. Calcular la diferencia y la variación porcentual entre los años.
    7. Seleccionar las columnas deseadas para el resumen.
    8. Filtrar el top 5 de subsectores con mayor diferencia.
    9. Renombrar las columnas y formatear los valores y porcentajes.

    Retorna:
    DataFrame: DataFrame con el resumen de exportaciones por subsector.
    """

    # 1. Filtrar los datos por los años especificados
    df_year1 = df[df['YEAR'] == year1].copy()
    df_year2 = df[df['YEAR'] == year2].copy()

    # 2. Pivotear los datos para tener los valores de exportación por año y subsector
    pivot_table_year1 = df_year1.pivot_table(values='VALOR_USD', index='SUBSECTOR', columns='PAIS_DESTINO', aggfunc='sum', fill_value=0)
    pivot_table_year2 = df_year2.pivot_table(values='VALOR_USD', index='SUBSECTOR', columns='PAIS_DESTINO', aggfunc='sum', fill_value=0)

    # 3. Asegurarse de que los años especificados existan en el DataFrame
    pivot_table_year1.columns = [f'{year1}({col})' for col in pivot_table_year1.columns]
    pivot_table_year2.columns = [f'{year2}({col})' for col in pivot_table_year2.columns]

    # 4. Unir las tablas de ambos años
    pivot_table = pivot_table_year1.join(pivot_table_year2, how='outer').fillna(0)
    pivot_table.reset_index(inplace=True)

    # 5. Calcular los totales por subsector y año
    pivot_table[year1] = pivot_table.filter(like=f'{year1}(').sum(axis=1)
    pivot_table[year2] = pivot_table.filter(like=f'{year2}(').sum(axis=1)

    # 6. Calcular la diferencia y la variación porcentual entre los años
    pivot_table['Diferencia'] = pivot_table[year2] - pivot_table[year1]
    pivot_table['Variación (%)'] = ((pivot_table[year2] - pivot_table[year1]) / pivot_table[year1].replace(0, np.nan)) * 100

    # 7. Seleccionar las columnas deseadas para el resumen
    pivot_table = pivot_table[['SUBSECTOR', year1, year2, 'Diferencia', 'Variación (%)']]

    # 8. Filtrar el top 5 de subsectores con mayor diferencia
    pivot_table = pivot_table.nlargest(5, 'Diferencia')

    # 9. Renombrar las columnas y formatear los valores y porcentajes
    column_names_dict = {
        'SUBSECTOR': 'Subsector',
        year1: f'{transform_year_column_name(year1)} (USD FOB)',
        year2: f'{transform_year_column_name(year2)} (USD FOB)',
        'Diferencia': 'Diferencia (USD FOB)',
        'Variación (%)': 'Variación (%)'
    }
    pivot_table.rename(columns=column_names_dict, inplace=True)

    # Formatear los valores y porcentajes
    pivot_table[f'{transform_year_column_name(year1)} (USD FOB)'] = pivot_table[f'{transform_year_column_name(year1)} (USD FOB)'].map('{:,.0f}'.format)
    pivot_table[f'{transform_year_column_name(year2)} (USD FOB)'] = pivot_table[f'{transform_year_column_name(year2)} (USD FOB)'].map('{:,.0f}'.format)
    pivot_table['Diferencia (USD FOB)'] = pivot_table['Diferencia (USD FOB)'].map('{:,.0f}'.format)
    pivot_table['Variación (%)'] = pivot_table['Variación (%)'].map('{:.1f}%'.format)

    # Convertir separadores de decimales y miles
    pivot_table = pivot_table.astype(str)
    for col in pivot_table.columns[1:]:
        pivot_table[col] = pivot_table[col].apply(lambda x: x.replace(',', 'X').replace('.', ',').replace('X', '.'))

    return pivot_table

# Función para obtener datos agregados, empresas y subsectores por año cerrado y año corrido
def obtener_datos_exportaciones(session, continentes=None, zonas_geograficas=None, paises=None, departamentos=None, 
                                hubs=None, tlcs=None, tipo_tlcss=None, tipos=None, years_cerrado=None, years_corrido=None, umbral=10000):
    """
    Obtiene los datos de exportaciones totales, número de empresas y datos de empresas para años cerrados y corridos.

    Parámetros:
    session (snowflake.snowpark.Session): Sesión activa en Snowflake.
    continentes (list): Lista de continentes a filtrar.
    zonas_geograficas (list): Lista de zonas geográficas a filtrar.
    paises (list): Lista de países a filtrar.
    departamentos (list): Lista de departamentos a filtrar.
    hubs (list): Lista de hubs a filtrar.
    tlcs (list): Lista de tratados de libre comercio a filtrar.
    tipo_tlcss (list): Lista de tipos de acuerdos comerciales a filtrar.
    tipos (list): Lista de tipos de posición arancelaria a filtrar.
    years_cerrado (list): Lista de años cerrados a filtrar.
    years_corrido (list): Lista de años corridos a filtrar.
    umbral (int): Umbral mínimo de exportación en USD para considerar una empresa.

    Retorna:
    tuple: Ocho DataFrames con los datos de exportaciones y empresas para años cerrados y corridos.
    """
    
    # 1. Obtener datos para año cerrado
    # Exportaciones totales
    df_exportaciones_cerrado = get_data_exportaciones(session, continentes, zonas_geograficas, paises, departamentos, 
                                                      hubs, tlcs, tipo_tlcss, tipos, years_cerrado)
    # Número de empresas
    df_numero_empresas_cerrado = get_data_exportaciones_numero_empresas(session, continentes, zonas_geograficas, paises, departamentos, 
                                                                        hubs, tlcs, tipo_tlcss, tipos, years_cerrado, umbral)
    # Datos de empresas
    df_empresas_cerrado, df_totales_cerrado = get_data_exportaciones_empresas(session, continentes, zonas_geograficas, paises, 
                                                                              departamentos, hubs, tlcs, tipo_tlcss, tipos, years_cerrado)
    
    # 2. Obtener datos para año corrido
    # Exportaciones totales
    df_exportaciones_corrido = get_data_exportaciones(session, continentes, zonas_geograficas, paises, departamentos, 
                                                      hubs, tlcs, tipo_tlcss, tipos, years_corrido)
    # Número de empresas
    df_numero_empresas_corrido = get_data_exportaciones_numero_empresas(session, continentes, zonas_geograficas, paises, departamentos, 
                                                                        hubs, tlcs, tipo_tlcss, tipos, years_corrido, umbral)
    # Datos de empresas
    df_empresas_corrido, df_totales_corrido = get_data_exportaciones_empresas(session, continentes, zonas_geograficas, paises, 
                                                                              departamentos, hubs, tlcs, tipo_tlcss, tipos, years_corrido)

    # Devolver los DataFrames generados
    return (df_exportaciones_cerrado, df_numero_empresas_cerrado, df_empresas_cerrado, df_totales_cerrado,
            df_exportaciones_corrido, df_numero_empresas_corrido, df_empresas_corrido, df_totales_corrido)

# Función para generar todas las tablas resumen por categorias
def generar_todas_tablas_resumen(df, categorias, valores, top_n=None):
    """
    Genera todas las tablas resumen para cada combinación de categorías y valores y asigna nombres descriptivos.

    Parámetros:
    df (DataFrame): DataFrame proveniente de la función get_data_exportaciones().
    categorias (list): Lista de variables categóricas para las cuales se generarán las tablas de resumen.
    valores (list): Lista de variables de valor a agregar ('VALOR_USD' o 'PESO_KG_NETO').

    Retorna:
    tuple: Dos listas de tuplas, cada tupla contiene el nombre de la tabla y el DataFrame con la tabla resumen.
    """

    # Inicializar las listas para almacenar las tablas resumen
    tablas_resumen_usd = []
    tablas_resumen_kg = []

    # Iterar sobre cada categoría y valor
    for categoria in categorias:
        for valor in valores:
            # Generar la tabla resumen
            tabla_resumen = generar_tabla_resumen(df, categoria, valor, top_n)

            # Crear un nombre descriptivo para la tabla
            nombre_tabla = f"RESUMEN_{categoria}_{valor}"

            # Clasificar la tabla en la lista correspondiente
            if valor == 'VALOR_USD':
                tablas_resumen_usd.append((nombre_tabla, tabla_resumen))
            elif valor == 'PESO_KG_NETO':
                tablas_resumen_kg.append((nombre_tabla, tabla_resumen))

    # Retornar las listas de tablas resumen
    return tablas_resumen_usd, tablas_resumen_kg

# Función para generar la lista completa de tablas de exportaciones
def generar_listas_tablas_definitivas_exportaciones(session, continentes=None, zonas_geograficas=None, paises=None, departamentos=None, 
                                      hubs=None, tlcs=None, tipo_tlcss=None, tipos=None, years_cerrado=None, 
                                      years_corrido=None, umbral=10000, categorias=None, valores=None, top_n = None):
    """
    Genera las listas de tablas definitivas de exportaciones, tanto en USD como en KG, a partir de las funciones
    obtener_datos_exportaciones, generar_todas_tablas_resumen, generar_tabla_empresas y generar_tabla_subsectores.

    Parámetros:
    session (snowflake.snowpark.Session): Sesión activa en Snowflake.
    continentes (list): Lista de continentes a filtrar.
    zonas_geograficas (list): Lista de zonas geográficas a filtrar.
    paises (list): Lista de países a filtrar.
    departamentos (list): Lista de departamentos a filtrar.
    hubs (list): Lista de hubs a filtrar.
    tlcs (list): Lista de tratados de libre comercio a filtrar.
    tipo_tlcss (list): Lista de tipos de acuerdos comerciales a filtrar.
    tipos (list): Lista de tipos de posición arancelaria a filtrar.
    years_cerrado (list): Lista de años cerrados a filtrar.
    years_corrido (list): Lista de años corridos a filtrar.
    umbral (int): Umbral mínimo de exportación en USD para considerar una empresa.
    categorias (list): Lista de variables categóricas para las cuales se generarán las tablas de resumen.
    valores (list): Lista de variables de valor a agregar ('VALOR_USD' o 'PESO_KG_NETO').
    top_n: Número de categorías top a filtrar. Por defecto no se filtran.
    Retorna:
    dict: Diccionario con las listas de tablas definitivas.
    """

    # Transformar lista de parámetros de elección en streamlit 
    continentes = [continentes] if continentes else []
    hubs = [hubs] if hubs else []
    paises = [paises] if paises else []
    departamentos = [departamentos] if departamentos else []


    # Obtener los datos de exportaciones para años cerrados y corridos
    (df_exportaciones_cerrado, df_numero_empresas_cerrado, df_empresas_cerrado, df_totales_cerrado,
     df_exportaciones_corrido, df_numero_empresas_corrido, df_empresas_corrido, df_totales_corrido) = obtener_datos_exportaciones(
        session, continentes, zonas_geograficas, paises, departamentos, hubs, tlcs, tipo_tlcss, tipos, years_cerrado, years_corrido, umbral
    )
    
    # Generar todas las tablas resumen para años cerrados
    tablas_resumen_usd_cerrado, tablas_resumen_kg_cerrado = generar_todas_tablas_resumen(df_exportaciones_cerrado, categorias, valores, top_n)

    # Generar todas las tablas resumen para años corridos
    tablas_resumen_usd_corrido, tablas_resumen_kg_corrido = generar_todas_tablas_resumen(df_exportaciones_corrido, categorias, valores, top_n)
    
    # Generar las tablas de empresas para años cerrados y corridos
    tabla_empresas_cerrado = generar_tabla_empresas(df_empresas_cerrado, df_totales_cerrado, years_cerrado[0], years_cerrado[-1])
    tabla_empresas_corrido = generar_tabla_empresas(df_empresas_corrido, df_totales_corrido, years_corrido[0], years_corrido[-1])

    # Generar las tablas de subsectores para años cerrados y corridos
    tabla_subsectores_cerrado = generar_tabla_subsectores(df_exportaciones_cerrado, years_cerrado[0], years_cerrado[-1])
    tabla_subsectores_corrido = generar_tabla_subsectores(df_exportaciones_corrido, years_corrido[0], years_corrido[-1])

    # Crear las listas de resultados
    resumen_usd_cerrado = [(nombre, tabla) for nombre, tabla in tablas_resumen_usd_cerrado]
    resumen_usd_corrido = [(nombre, tabla) for nombre, tabla in tablas_resumen_usd_corrido]
    resumen_kg_cerrado = [(nombre, tabla) for nombre, tabla in tablas_resumen_kg_cerrado]
    resumen_kg_corrido = [(nombre, tabla) for nombre, tabla in tablas_resumen_kg_corrido]

    numero_empresas_cerrado = [("Numero de Empresas Año Cerrado", df_numero_empresas_cerrado)]
    numero_empresas_corrido = [("Numero de Empresas Año Corrido", df_numero_empresas_corrido)]

    tablas_empresas_cerrado = [("Tabla Empresas Año Cerrado", tabla_empresas_cerrado)]
    tablas_empresas_corrido = [("Tabla Empresas Año Corrido", tabla_empresas_corrido)]

    tablas_subsectores_cerrado = [("Tabla Subsectores Año Cerrado", tabla_subsectores_cerrado)]
    tablas_subsectores_corrido = [("Tabla Subsectores Año Corrido", tabla_subsectores_corrido)]

    # Crear un diccionario para organizar los resultados
    resultados = {
        "Resumen USD Cerrado": resumen_usd_cerrado,
        "Resumen USD Corrido": resumen_usd_corrido,
        "Resumen KG Cerrado": resumen_kg_cerrado,
        "Resumen KG Corrido": resumen_kg_corrido,
        "Numero de Empresas Cerrado": numero_empresas_cerrado,
        "Numero de Empresas Corrido": numero_empresas_corrido,
        "Tablas Empresas Cerrado": tablas_empresas_cerrado,
        "Tablas Empresas Corrido": tablas_empresas_corrido,
        "Tablas Subsectores Cerrado": tablas_subsectores_cerrado,
        "Tablas Subsectores Corrido": tablas_subsectores_corrido,
    }

    # Retornar el diccionario con los resultados
    return resultados