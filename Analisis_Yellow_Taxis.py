import pyarrow.parquet as pq
import pandas as pd
trips = pq.read_table('yellow_tripdata_2023-01.parquet')
trips = trips.to_pandas()


# convertir las columnas de fecha y hora a formato datetime
trips['tpep_pickup_datetime'] = pd.to_datetime(trips['tpep_pickup_datetime'])
trips['tpep_dropoff_datetime'] = pd.to_datetime(trips['tpep_dropoff_datetime'])
# duracion del viaje en tiempo (en minutos)
trips['trip_duration'] = (trips['tpep_dropoff_datetime'] - trips['tpep_pickup_datetime']).dt.total_seconds() / 60

# Calcular la duradion media de los viajes
average_duration = trips['trip_duration'].mean()
print(f'Average trip duration: {average_duration:.2f} minutes')
tpep_pickup_datetime = trips['tpep_pickup_datetime']

# Calcular el costo medio de los viajes por hora del día en dolares y con dos decimales. Ordenado por el total de viajes por hora de mayor a menor. 
average_cost_by_hour = trips.groupby(tpep_pickup_datetime.dt.hour)['total_amount'].mean().round(2).sort_values(ascending=False) 
print('Average cost by hour of the day:')
print(average_cost_by_hour)

# Calcular el número total de viajes por hora del día. Ordenado por orden de hora del dia. Separado por hora del día. en formato de tabla con formato de hora 24 horas.
trips_by_hour = trips.groupby(tpep_pickup_datetime.dt.hour).size().reset_index(name='total_trips')
trips_by_hour['tpep_pickup_datetime'] = trips_by_hour['tpep_pickup_datetime'].apply(lambda x: f'{x:02d}:00')
print('Total trips by hour of the day:')
print(trips_by_hour)

