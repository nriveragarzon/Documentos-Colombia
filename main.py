#######################
# 0. Importar librerias
#######################

# Selectores
import selectores as selectores
# Exportaciones
import datos_exportaciones as exportaciones

# Documentos
import documentos as doc

# Liberias
import streamlit as st
import pandas as pd
import io
# from docx2pdf import convert
# import pythoncom

# # Configuración página web
st.set_page_config(page_title="Documentos Tres Ejes", page_icon = ':bar_chart:', layout="wide",  initial_sidebar_state="expanded")

# Datos de sesión de Snowflake
connection = st.connection("snowflake")
sesion_activa = connection.session()


######################################
# 1. Definir el flujo de la aplicación
######################################

def main():
    ## Título en todas las páginas
    st.title('Documentos Tres Ejes', anchor= None)    
       
    ## Menú de navegación
    ### Logo ProColombia
    with st.sidebar:
        url = r'Insumos/Procolombia/PRO_PRINCIPAL_HORZ_PNG.png'
        st.image(url, caption=None, use_column_width="always")
        st.markdown('#')     
    ## Páginas
    ### Opciones con iconos
    options = {
        "Portada :arrow_backward:": "Portada",
        "Documentos :arrow_backward:": "Documentos",
        "Fuentes :arrow_backward:": "Fuentes"
    }
    #### Configuración del sidebar
    page = st.sidebar.radio("Elija una página", list(options.keys()))
    selected_option = options.get(str(page))  # Usar get para manejar None de manera segura
    if selected_option:
        if selected_option == "Portada":
            page_portada()
        elif selected_option == "Documentos":
            documentos()
        elif selected_option == "Fuentes":
            page_fuentes()
    ### Logo MINCIT
    with st.sidebar:
        ### Elaborado por la Coordinación de Analítica
        st.markdown('#')
        st.subheader("Elaborado por:") 
        st.write("Coordinación de Analítica, Gerencia de Inteligencia Comercial, ProColombia.") 
        st.markdown('#') 
        url2 = r'Insumos/Logo Ministerio Comercio, Industria y Turismo/Logo MinCit_Mesa de trabajo 1.png'
        st.image(url2, caption=None, use_column_width="always")

###############
# 2. PARÁMETROS
###############

# PARÁMETROS DE FECHAS (ACTUALIZAR MENSUALMENTE O ANUALMENTE)
# MENUSUALMENTE
year_corrido = 'enero - marzo 2024'
fecha_actualizacion = 'MARZO 2024'
years_corrido = ['2023(Ene-Mar)', '2024(Ene-Mar)']
# ANUALMENTE
year_cerrado = '2023'
years_cerrado = ['2022', '2023']

# Parámetros fijos (no cambian a menos que la estructura del documento cambie )
# Geografía
zonas_geograficas = None
tlcs = None
tipo_tlcss = None
# Tipos
tipos_total = ['No Mineras', 'Mineras']
tipos_nme = ['No Mineras']
# Umbral de 10000 USD para cuenta de empresa
umbral = 10000
# Categorías para tener en cuenta
categorias_total = ['TIPO']
categorias_nme = ['SECTOR', 'SUBSECTOR', 'PAIS_DESTINO', 'DEPARTAMENTO_ORIGEN', 'CADENA', 'ZONA_GEOGRAFICA', 'TLCS']
# Valores a generar tablas
valores = ['VALOR_USD', 'PESO_KG_NETO']
# Número de categorías para top x
top_n_total = None
top_n_nme = 5
# Básicos
continentes_base = None
paises_base = None
departamentos_base = None
hubs_base = None

# Función para volver a pdf con entorno web
# def convert_to_pdf(docx_path, pdf_path):
#     pythoncom.CoInitialize()  # Inicializar COM
#     try:
#         convert(docx_path, pdf_path)
#     finally:
#         pythoncom.CoUninitialize()  # Desinicializar COM

##############
# 3. Contenido
##############

#########
# Portada
#########

def page_portada():
    st.header('Bienvenido')
    st.write('Esta aplicación le permitirá generar y descargar a demanda informes que resuman las cifras más relevantes de los tres ejes de negocio de ProColombia.')
    st.write('Para su construcción se definen las siguientes argupaciones:')
    st.write(
        """
    - **Continentes:** la información se agrupa por continente.
    - **HUBs:** la información se agrupa por HUB.
    - **TLC´S:** la información se agrupa por tratado de libre comercio.
    - **País:** la información se agrupa por país con el que Colombia tiene relaciones comerciales.
    - **Colombia:** muestra toda las cifras de los tres ejes para Colombia.
    - **Departamentos:** la información se agrupa por departamento.
        """
    )

############
# Documentos
############

def documentos():
    st.header("Descarga de documentos")
    # Elección del usuario entre diferentes agrupaciones de datos
    eleccion_usuario = st.radio("Seleccione una opción: por favor, elija entre las siguientes opciones para descargar el informe de interés:", 
                                # Opciones: 
                                ('**Continente:** Explore un informe organizado por continente a nivel mundial.', 
                                 '**HUB:** Explore un informe organizado por HUB.',
                                 '**País:** Explore un informe organizado por país.',
                                  '**Colombia:** Explore un informe organizado de Colombia.',
                                 '**Departamento:** Explore un informe organizado por departamento.'),
                                # Aclaración
                                help = "Seleccione una de las opciones para mostrar el contenido relacionado.")
    
    # Continente (eleccion del usuario)
    if eleccion_usuario == "**Continente:** Explore un informe organizado por continente a nivel mundial.":
        continente_elegido = st.selectbox('Seleccione un continente:', selectores.selector_continentes(sesion_activa), index=None, placeholder='Elija una opción', help = 'Aquí puede elegir el continente para descargar el informe de interés. Seleccione un único continente para refinar su búsqueda.', key = 'widget_continentes')
        # Después de que el usuario haya elegido un continente se inicia el proceso de carga de datos y generación del informe automáticamente
        if continente_elegido:
            # Importar bases de datos para el informe
            # Base total
            df_total = exportaciones.generar_listas_tablas_definitivas_exportaciones(session=sesion_activa, continentes=continente_elegido, zonas_geograficas=zonas_geograficas,
                                                                                 paises=paises_base, departamentos=departamentos_base, hubs= hubs_base, 
                                                                                 tlcs=tlcs, tipo_tlcss=tipo_tlcss, tipos=tipos_total, 
                                                                                 years_cerrado=years_cerrado, years_corrido=years_corrido, umbral=umbral, 
                                                                                 categorias=categorias_total, valores=valores, top_n=top_n_total)
            # Base NME
            df_nme = exportaciones.generar_listas_tablas_definitivas_exportaciones(session=sesion_activa, continentes=continente_elegido, zonas_geograficas=zonas_geograficas,
                                                                                 paises=paises_base, departamentos=departamentos_base, hubs= hubs_base, 
                                                                                 tlcs=tlcs, tipo_tlcss=tipo_tlcss, tipos=tipos_nme, 
                                                                                 years_cerrado=years_cerrado, years_corrido=years_corrido, umbral=umbral, 
                                                                                 categorias=categorias_nme, valores=valores, top_n=top_n_nme)
            # Generar documento
            file_path = f"Tres Ejes Continentes - {continente_elegido}.docx"
            doc.create_document_continentes(df_total=df_total, df_nme=df_nme, file_path=file_path, titulo=continente_elegido, 
                                               fecha=fecha_actualizacion, 
                                               header_image_left=r'Insumos/Procolombia/PRO_PRINCIPAL_HORZ_PNG.png', 
                                               header_image_right=r'Insumos/Logo Ministerio Comercio, Industria y Turismo/Logo MinCit_Mesa de trabajo 1.png', 
                                               footer_image=r'Insumos/Logo Marca País/Logo_MP_EPDLB2.png', 
                                               year_cerrado=year_cerrado, year_corrido=year_corrido)
            with open(file_path, 'rb') as f:
                doc_bytes = io.BytesIO(f.read())
            st.download_button(
                label="Descargar Documento en Microsoft Word",
                data=doc_bytes,
                file_name=file_path,
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
            # Generar el pdf
            # pdf_file_path = file_path.replace('.docx', '.pdf')
            # convert_to_pdf(file_path, pdf_file_path)
            # # Cargar el documento PDF para descarga
            # with open(pdf_file_path, 'rb') as f:
            #     pdf_bytes = io.BytesIO(f.read())
            # st.download_button(
            #     label="Descargar Documento en PDF",
            #     data=pdf_bytes,
            #     file_name=pdf_file_path,
            #     mime="application/pdf"
            # )


    # HUB
    if eleccion_usuario == "**HUB:** Explore un informe organizado por HUB.":
        hub_elegido = st.selectbox('Seleccione un HUB:', selectores.selector_hubs(sesion_activa), index=None, placeholder='Elija una opción', help = 'Aquí puede elegir el HUB para descargar el informe de interés. Seleccione un único HUB para refinar su búsqueda.', key = 'widget_hubs')
        # Después de que el usuario haya elegido un hub se inicia el proceso de carga de datos y generación del informe automáticamente
        if hub_elegido:
            # Importar bases de datos para el informe
            # Base total
            df_total = exportaciones.generar_listas_tablas_definitivas_exportaciones(session=sesion_activa, continentes=continentes_base, zonas_geograficas=zonas_geograficas,
                                                                                 paises=paises_base, departamentos=departamentos_base, hubs= hub_elegido, 
                                                                                 tlcs=tlcs, tipo_tlcss=tipo_tlcss, tipos=tipos_total, 
                                                                                 years_cerrado=years_cerrado, years_corrido=years_corrido, umbral=umbral, 
                                                                                 categorias=categorias_total, valores=valores, top_n=top_n_total)
            # Base NME
            df_nme = exportaciones.generar_listas_tablas_definitivas_exportaciones(session=sesion_activa, continentes=continentes_base, zonas_geograficas=zonas_geograficas,
                                                                                 paises=paises_base, departamentos=departamentos_base, hubs= hub_elegido, 
                                                                                 tlcs=tlcs, tipo_tlcss=tipo_tlcss, tipos=tipos_nme, 
                                                                                 years_cerrado=years_cerrado, years_corrido=years_corrido, umbral=umbral, 
                                                                                 categorias=categorias_nme, valores=valores, top_n=top_n_nme)
            # Generar documento
            file_path = f"Tres Ejes HUBs - {hub_elegido}.docx"
            doc.create_document_hub(df_total=df_total, df_nme=df_nme, file_path=file_path, titulo=hub_elegido, 
                                               fecha=fecha_actualizacion, 
                                               header_image_left=r'Insumos/Procolombia/PRO_PRINCIPAL_HORZ_PNG.png', 
                                               header_image_right=r'Insumos/Logo Ministerio Comercio, Industria y Turismo/Logo MinCit_Mesa de trabajo 1.png', 
                                               footer_image=r'Insumos/Logo Marca País/Logo_MP_EPDLB2.png', 
                                               year_cerrado=year_cerrado, year_corrido=year_corrido)
            with open(file_path, 'rb') as f:
                doc_bytes = io.BytesIO(f.read())
            st.download_button(
                label="Descargar Documento en Microsoft Word",
                data=doc_bytes,
                file_name=file_path,
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
            
    # País
    if eleccion_usuario == "**País:** Explore un informe organizado por país.":
        continente_pais = st.selectbox('Seleccione un continente:', selectores.selector_continentes(sesion_activa), index=None, placeholder='Elija una opción', help = 'Aquí puede elegir el continente para descargar el informe de interés. Seleccione un único continente para refinar su búsqueda.', key = 'widget_continentes_pais')
        pais_elegido = st.selectbox('Seleccione un país:', selectores.selector_paises(sesion_activa, continente_pais), index=None, placeholder='Elija una opción', help = 'Aquí puede elegir el país para descargar el informe de interés. Seleccione un único país para refinar su búsqueda.', key = 'widget_pais')
        # Después de que el usuario haya elegido un país se inicia el proceso de carga de datos y generación del informe automáticamente
        if pais_elegido:
            # Importar bases de datos para el informe
            # Base total
            df_total = exportaciones.generar_listas_tablas_definitivas_exportaciones(session=sesion_activa, continentes=continentes_base, zonas_geograficas=zonas_geograficas,
                                                                                 paises=pais_elegido, departamentos=departamentos_base, hubs= hubs_base, 
                                                                                 tlcs=tlcs, tipo_tlcss=tipo_tlcss, tipos=tipos_total, 
                                                                                 years_cerrado=years_cerrado, years_corrido=years_corrido, umbral=umbral, 
                                                                                 categorias=categorias_total, valores=valores, top_n=top_n_total)
            # Base NME
            df_nme = exportaciones.generar_listas_tablas_definitivas_exportaciones(session=sesion_activa, continentes=continentes_base, zonas_geograficas=zonas_geograficas,
                                                                                 paises=pais_elegido, departamentos=departamentos_base, hubs= hubs_base, 
                                                                                 tlcs=tlcs, tipo_tlcss=tipo_tlcss, tipos=tipos_nme, 
                                                                                 years_cerrado=years_cerrado, years_corrido=years_corrido, umbral=umbral, 
                                                                                 categorias=categorias_nme, valores=valores, top_n=top_n_nme)
            # Generar documento
            file_path = f"Tres Ejes Países - {pais_elegido}.docx"
            doc.create_document_pais(df_total=df_total, df_nme=df_nme, file_path=file_path, titulo=pais_elegido, 
                                               fecha=fecha_actualizacion, 
                                               header_image_left=r'Insumos/Procolombia/PRO_PRINCIPAL_HORZ_PNG.png', 
                                               header_image_right=r'Insumos/Logo Ministerio Comercio, Industria y Turismo/Logo MinCit_Mesa de trabajo 1.png', 
                                               footer_image=r'Insumos/Logo Marca País/Logo_MP_EPDLB2.png', 
                                               year_cerrado=year_cerrado, year_corrido=year_corrido)
            with open(file_path, 'rb') as f:
                doc_bytes = io.BytesIO(f.read())
            st.download_button(
                label="Descargar Documento en Microsoft Word",
                data=doc_bytes,
                file_name=file_path,
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
            

    # Colombia 
    if eleccion_usuario =="**Colombia:** Explore un informe organizado de Colombia.":
        # Importar bases de datos para el informe
            # Base total
            df_total = exportaciones.generar_listas_tablas_definitivas_exportaciones(session=sesion_activa, continentes=continentes_base, zonas_geograficas=zonas_geograficas,
                                                                                 paises=paises_base, departamentos=departamentos_base, hubs= hubs_base, 
                                                                                 tlcs=tlcs, tipo_tlcss=tipo_tlcss, tipos=tipos_total, 
                                                                                 years_cerrado=years_cerrado, years_corrido=years_corrido, umbral=umbral, 
                                                                                 categorias=categorias_total, valores=valores, top_n=top_n_total)
            # Base NME
            df_nme = exportaciones.generar_listas_tablas_definitivas_exportaciones(session=sesion_activa, continentes=continentes_base, zonas_geograficas=zonas_geograficas,
                                                                                 paises=paises_base, departamentos=departamentos_base, hubs= hubs_base, 
                                                                                 tlcs=tlcs, tipo_tlcss=tipo_tlcss, tipos=tipos_nme, 
                                                                                 years_cerrado=years_cerrado, years_corrido=years_corrido, umbral=umbral, 
                                                                                 categorias=categorias_nme, valores=valores, top_n=top_n_nme)
            # Generar documento
            file_path = f"Tres Ejes Colombia.docx"
            doc.create_document_colombia(df_total=df_total, df_nme=df_nme, file_path=file_path,
                                               fecha=fecha_actualizacion, 
                                               header_image_left=r'Insumos/Procolombia/PRO_PRINCIPAL_HORZ_PNG.png', 
                                               header_image_right=r'Insumos/Logo Ministerio Comercio, Industria y Turismo/Logo MinCit_Mesa de trabajo 1.png', 
                                               footer_image=r'Insumos/Logo Marca País/Logo_MP_EPDLB2.png', 
                                               year_cerrado=year_cerrado, year_corrido=year_corrido)
            with open(file_path, 'rb') as f:
                doc_bytes = io.BytesIO(f.read())
            st.download_button(
                label="Descargar Documento en Microsoft Word",
                data=doc_bytes,
                file_name=file_path,
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
            

    # Departamento
    if eleccion_usuario == "**Departamento:** Explore un informe organizado por departamento.":
        departamento_elegido = st.selectbox('Seleccione un departamento:', selectores.selector_departamento(sesion_activa), index=None, placeholder='Elija una opción', help = 'Aquí puede elegir el departamento para descargar el informe de interés. Seleccione un único departamento para refinar su búsqueda.', key = 'widget_departamentos')
        # Después de que el usuario haya elegido un país se inicia el proceso de carga de datos y generación del informe automáticamente
        if departamento_elegido:
            # Importar bases de datos para el informe
            # Base total
            df_total = exportaciones.generar_listas_tablas_definitivas_exportaciones(session=sesion_activa, continentes=continentes_base, zonas_geograficas=zonas_geograficas,
                                                                                 paises=paises_base, departamentos=departamento_elegido, hubs= hubs_base, 
                                                                                 tlcs=tlcs, tipo_tlcss=tipo_tlcss, tipos=tipos_total, 
                                                                                 years_cerrado=years_cerrado, years_corrido=years_corrido, umbral=umbral, 
                                                                                 categorias=categorias_total, valores=valores, top_n=top_n_total)
            # Base NME
            df_nme = exportaciones.generar_listas_tablas_definitivas_exportaciones(session=sesion_activa, continentes=continentes_base, zonas_geograficas=zonas_geograficas,
                                                                                 paises=paises_base, departamentos=departamento_elegido, hubs= hubs_base, 
                                                                                 tlcs=tlcs, tipo_tlcss=tipo_tlcss, tipos=tipos_nme, 
                                                                                 years_cerrado=years_cerrado, years_corrido=years_corrido, umbral=umbral, 
                                                                                 categorias=categorias_nme, valores=valores, top_n=top_n_nme)
            # Generar documento
            file_path = f"Tres Ejes Departamentos - {departamento_elegido}.docx"
            doc.create_document_departamento(df_total=df_total, df_nme=df_nme, file_path=file_path, titulo=departamento_elegido, 
                                               fecha=fecha_actualizacion, 
                                               header_image_left=r'Insumos/Procolombia/PRO_PRINCIPAL_HORZ_PNG.png', 
                                               header_image_right=r'Insumos/Logo Ministerio Comercio, Industria y Turismo/Logo MinCit_Mesa de trabajo 1.png', 
                                               footer_image=r'Insumos/Logo Marca País/Logo_MP_EPDLB2.png', 
                                               year_cerrado=year_cerrado, year_corrido=year_corrido)
            with open(file_path, 'rb') as f:
                doc_bytes = io.BytesIO(f.read())
            st.download_button(
                label="Descargar Documento en Microsoft Word",
                data=doc_bytes,
                file_name=file_path,
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
            
#########
# Fuentes
#########

def page_fuentes():
    st.header("Fuentes")
    st.write("""
    - **Exportaciones:** DANE - DIAN - Cálculos ProColombia.
             """)

########################################
# Mostrar contenido de todas las páginas
########################################
if __name__ == "__main__":
    main()