import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer
import pandas as pd
import streamlit as st

def sf_check_snowflake_connection():
    """
    Verifica la conexión con Snowflake ejecutando una consulta para obtener la versión actual del servidor.
    
    Esta función intenta establecer una conexión con Snowflake usando las configuraciones almacenadas en st.secrets,
    ejecuta una consulta para determinar la versión actual del servidor y cierra adecuadamente todos los recursos.
    
    Returns:
        str: Devuelve la versión actual de Snowflake si la conexión es exitosa. En caso de error, retorna un mensaje indicativo.
    
    Note:
        Esta función es útil para verificar la correcta configuración y operatividad de las conexiones a Snowflake.
        Las credenciales y configuraciones se obtienen del archivo .streamlit/secrets.toml.
    """
    conn = None
    cur = None
    try:
        # Crear la conexión utilizando las credenciales y configuraciones almacenadas en st.secrets
        conn = snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            account=st.secrets["snowflake"]["account"]
        )
        
        # Crear un cursor para ejecutar consultas
        cur = conn.cursor()
        
        # Ejecutar consulta para obtener la versión actual de Snowflake
        cur.execute("SELECT current_version()")
        one_row = cur.fetchone()
        
        # Devolver la versión actual de Snowflake
        return one_row[0] if one_row else "No se pudo obtener la versión."
    
    except Exception as e:
        # Manejo de excepciones si ocurre un error durante la conexión o ejecución
        return f"Error al conectar o ejecutar la consulta: {e}"
    
    finally:
        # Asegurarse de cerrar el cursor y la conexión
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Conexión cerrada correctamente.")

def st_query_to_snowflake_and_return_dataframe(query: str, limit: int = None, expected_types: dict = None) -> pd.DataFrame:
    """
    Ejecuta una consulta SQL en Snowflake y devuelve los resultados en un DataFrame de Pandas.

    Args:
        query (str): La consulta SQL a ejecutar en Snowflake.
        sf_config (dict): Un diccionario que contiene la configuración de conexión a Snowflake.
                          Debe incluir las siguientes claves:
                          - 'user': El nombre de usuario de Snowflake.
                          - 'password': La contraseña del usuario de Snowflake.
                          - 'account': El identificador de cuenta de Snowflake.
                          - 'warehouse': El nombre del almacén de datos en Snowflake (opcional).
                          - 'database': El nombre de la base de datos en Snowflake (opcional).
                          - 'schema': El nombre del esquema en Snowflake (opcional).
        limit (int, optional): El número máximo de filas a devolver. Si se proporciona, se agrega un límite
                               a la consulta SQL. Por defecto es None, lo que significa que no se aplica límite.
        expected_types (dict, optional): Un diccionario que mapea nombres de columnas a sus tipos de datos esperados.

    Returns:
        pd.DataFrame: Un DataFrame de Pandas que contiene los resultados de la consulta SQL.

    Raises:
        snowflake.connector.errors.ProgrammingError: Si hay un error al ejecutar la consulta SQL en Snowflake.
    """
    try:
        # Establecer la conexión a Snowflake utilizando la configuración proporcionada
        conn = snowflake.connector.connect(
            user=st.secrets["snowflake"]['user'],
            password=st.secrets["snowflake"]['password'],
            account=st.secrets["snowflake"]['account'],
            warehouse=st.secrets["snowflake"]['warehouse'],  # Opcional, aunque útil para llevar seguimiento.
            database=st.secrets["snowflake"]['database'],    # Opcional
            schema=st.secrets["snowflake"]['schema']         # Opcional
        )

        # Crear un cursor para ejecutar la consulta
        with conn.cursor() as cs:
            # Agregar límite a la consulta solo si se proporciona un valor para limit
            if limit is not None:
                query += f" LIMIT {limit}"

            # Imprimir la consulta para depuración
            print("Executing query:", query)

            # Ejecutar la consulta SQL
            cs.execute(query)

            # Obtener los nombres de las columnas
            column_names = [desc[0] for desc in cs.description]

            # Obtener los resultados de la consulta
            results = cs.fetchall()

            # Crear un DataFrame de Pandas con los resultados
            df = pd.DataFrame(results, columns=column_names)

            # Aplicar tipos de datos esperados al DataFrame si se proporcionan
            if expected_types:
                for col_name, col_type in expected_types.items():
                    if col_name in df.columns:
                        df[col_name] = df[col_name].astype(col_type)

        return df

    except snowflake.connector.errors.ProgrammingError as e:
        # Manejar errores específicos de Snowflake
        print(f"Error executing query: {e}")
        raise e

    finally:
        # Cerrar la conexión a Snowflake
        conn.close()
    """
    Ejecuta una consulta SQL en Snowflake y devuelve los resultados en un DataFrame de Pandas,
    conservando los tipos de datos de las columnas.

    Args:
        query (str): La consulta SQL a ejecutar en Snowflake.
        sf_config (dict): Un diccionario que contiene la configuración de conexión a Snowflake.
                          Debe incluir las siguientes claves:
                          - 'user': El nombre de usuario de Snowflake.
                          - 'password': La contraseña del usuario de Snowflake.
                          - 'account': El identificador de cuenta de Snowflake.
                          - 'warehouse': El nombre del almacén de datos en Snowflake (opcional).
                          - 'database': El nombre de la base de datos en Snowflake (opcional).
                          - 'schema': El nombre del esquema en Snowflake (opcional).
        limit (int, optional): El número máximo de filas a devolver. Si se proporciona, se agrega un límite
                               a la consulta SQL. Por defecto es None, lo que significa que no se aplica límite.

    Returns:
        pd.DataFrame: Un DataFrame de Pandas que contiene los resultados de la consulta SQL,
                      con los tipos de datos de las columnas conservados.

    Raises:
        snowflake.connector.errors.ProgrammingError: Si hay un error al ejecutar la consulta SQL en Snowflake.
    """
    try:
        # Establecer la conexión a Snowflake utilizando la configuración proporcionada
        conn = snowflake.connector.connect(
            user=st.secrets["snowflake"]['user'],
            password=st.secrets["snowflake"]['password'],
            account=st.secrets["snowflake"]['account'],
            warehouse=st.secrets["snowflake"]['warehouse'],  # Opcional, aunque útil para llevar seguimiento.
            database=st.secrets["snowflake"]['database'],    # Opcional
            schema=st.secrets["snowflake"]['schema']         # Opcional
        )

        # Crear un cursor para ejecutar la consulta
        with conn.cursor() as cs:
            # Agregar límite a la consulta solo si se proporciona un valor para limit
            if limit is not None:
                query += f" LIMIT {limit}"

            # Ejecutar la consulta SQL
            cs.execute(query)

            # Obtener los nombres de las columnas y sus tipos de datos
            column_names = [desc[0] for desc in cs.description]
            column_types = [desc[1] for desc in cs.description]

            # Mapeo de tipos de datos de Snowflake a tipos de datos de Pandas
            snowflake_to_pandas_map = {
                'FIXED': 'int64', 'REAL': 'float64', 'TEXT': 'object',
                'BOOLEAN': 'bool', 'TIMESTAMP_NTZ': 'datetime64[ns]', 'DATE': 'datetime64[ns]',
                'ARRAY': 'object', 'OBJECT': 'object', 'VARIANT': 'object', 'BLOB': 'object'
            }

            # Obtener los resultados de la consulta
            results = cs.fetchall()

            # Crear un DataFrame de Pandas con los resultados y especificar los tipos de datos de las columnas
            df = pd.DataFrame(results, columns=column_names)
            for i, col_type in enumerate(column_types):
                df.iloc[:, i] = df.iloc[:, i].astype(snowflake_to_pandas_map.get(col_type, 'object'))

        return df

    except snowflake.connector.errors.ProgrammingError as e:
        # Manejar errores específicos de Snowflake
        raise e

    finally:
        # Cerrar la conexión a Snowflake
        conn.close()