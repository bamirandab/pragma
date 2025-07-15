import pymssql
import os

class AzureSQLClient:
    def __init__(self, connection_params: dict = None):
        """
        connection_params: dict con las claves:
            - server: nombre del servidor (ej: 'mi-servidor.database.windows.net')
            - database: nombre de la base de datos
            - user: nombre de usuario
            - password: contraseña
        Si no se pasa este diccionario, se intentará obtener todo desde variables de entorno:
        SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD
        """
        self.server = connection_params.get("server") if connection_params else os.getenv("SQL_SERVER")
        self.database = connection_params.get("database") if connection_params else os.getenv("SQL_DATABASE")
        self.user = connection_params.get("user") if connection_params else os.getenv("SQL_USER")
        self.password = connection_params.get("password") if connection_params else os.getenv("SQL_PASSWORD")

        if not all([self.server, self.database, self.user, self.password]):
            raise ValueError("Faltan parámetros de conexión para Azure SQL.")

        self.conn = None

    def connect(self):
        """Establece la conexión con la base de datos"""
        self.conn = pymssql.connect(
            server=self.server,
            user=self.user,
            password=self.password,
            database=self.database,
            port=1433,
            timeout=10,
            charset='UTF-8'
        )
        return self.conn

    def execute(self, query: str, params: tuple = ()):
        """
        Ejecuta un query con parámetros opcionales. Retorna el cursor.
        """
        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        self.conn.commit()
        return cursor

    def fetchall(self, query: str, params: tuple = ()):
        """
        Ejecuta un SELECT y retorna todos los resultados como lista de tuplas.
        """
        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()

    def close(self):
        """Cierra la conexión"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def test_connection(self):
        try:
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            print("✅ Conexión exitosa. Resultado:", result)
        except Exception as e:
            print("❌ Error al conectar a la base de datos:", e)
        finally:
            if self.conn:
                self.conn.close()