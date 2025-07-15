import pandas as pd
import os
from datetime import datetime
import numpy as np

class CSVPipeline:
    def __init__(self, db_client):
        self.db = db_client

    def update_running_stats(self, price: float):
        existing_rows = self.db.fetchall("SELECT total_count, sum_price, min_price, max_price FROM pragma.running_stats")

        if not existing_rows:
            # Primera ejecución: insertamos los valores iniciales
            self.db.execute("""
                INSERT INTO pragma.running_stats (total_count, sum_price, min_price, max_price, updated_at)
                VALUES (%s, %s, %s, %s, SYSDATETIME())
            """, (1, price, price, price))
        else:
            total_count, total_sum, min_price, max_price = existing_rows[0]

            new_count = total_count + 1
            new_sum = total_sum + price
            new_min = min(price, min_price) if min_price is not None else price
            new_max = max(price, max_price) if max_price is not None else price

            self.db.execute("""
                UPDATE pragma.running_stats
                SET total_count = %s,
                    sum_price = %s,
                    min_price = %s,
                    max_price = %s,
                    updated_at = SYSDATETIME()
            """, (new_count, new_sum, new_min, new_max))

        return self.db.fetchall("SELECT total_count, sum_price, min_price, max_price, avg_price FROM pragma.running_stats")[0]


    def load_csv_file(self, filepath: str):
        df = pd.read_csv(filepath)
        user_avg = df.groupby('user_id')['price'].mean()


        def fill_missing_price(row):
            if pd.isna(row['price']):
                uid = row['user_id']
                if uid in user_avg and not pd.isna(user_avg[uid]):
                    return user_avg[uid]
                else:
                    return np.nan  
            return row['price']

        # Reemplazar precios faltantes
        df['price'] = df.apply(fill_missing_price, axis=1)


        n = df.shape[0]
        filename = os.path.basename(filepath)
        rows_inserted = 0
        price_min = None
        price_max = None
        price_sum = 0.0
        load_timestamp = datetime.utcnow()

        # 1. Insertar registro inicial en load_tracking
        self.db.execute("""
            INSERT INTO pragma.load_tracking (source_file, load_timestamp, rows_loaded)
            VALUES (%s, %s, %s)
        """, (filename, load_timestamp, n,))

        # Obtener el ID del registro insertado (si hay autoincremento o IDENTITY)
        load_id_row = self.db.fetchall("SELECT load_id FROM pragma.load_tracking WHERE source_file = %s and load_timestamp = %s", (filename,load_timestamp, ))
        load_id = load_id_row[0][0]


        # 2. Procesar el archivo fila por fila
        for _, row in df.iterrows():
            timestamp = pd.to_datetime(row['timestamp'])
            price = float(row['price'])
            user_id = str(row['user_id'])

            self.db.execute("""
                INSERT INTO pragma.transactions (timestamp, price, user_id, load_id)
                VALUES (%s, %s, %s, %s)
            """, (timestamp, price, user_id, load_id,))

            total_count, sum_price, min_price, max_price, avg_price = self.update_running_stats(price)

            print(f"🔄 Para el registro ({timestamp}, {price}, {user_id}) las estadísticas actualizadas → count: {total_count}, sum: {sum_price}, min: {min_price}, max: {max_price}, avg: {avg_price:.2f}")

            price_min = price if price_min is None else min(price, price_min)
            price_max = price if price_max is None else max(price, price_max)
            price_sum += price
            rows_inserted += 1



        return print(f"El archivo {filepath} ha sido procesado")