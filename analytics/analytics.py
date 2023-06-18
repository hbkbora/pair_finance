from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from geopy.distance import geodesic
from datetime import datetime, timedelta

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgreSQL successful.')

while True:
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to MySQL successful.')

def calculate_distance(loc1, loc2):
    lat1, lon1 = loc1["latitude"], loc1["longitude"]
    lat2, lon2 = loc2["latitude"], loc2["longitude"]
    distance = geodesic((lat1, lon1), (lat2, lon2)).kilometers
    return distance

def calculate_aggregations():
    # Get the current hour
    current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)

    # Calculate the start of the hour
    hour_start = current_hour - timedelta(hours=1)

    # Retrieve data from PostgreSQL for the last hour
    query = f"""
        SELECT device_id, temperature, location, time
        FROM devices
        WHERE time >= '{hour_start}' AND time < '{current_hour}'
    """
    df = pd.read_sql(query, psql_engine)

    # Aggregation dictionaries
    max_temperatures = {}
    data_points = {}
    total_distances = {}

    for index, row in df.iterrows():
        device_id = row["device_id"]
        temperature = row["temperature"]
        location = row["location"]
        time = row["time"]

        # Calculate maximum temperature
        if device_id not in max_temperatures:
            max_temperatures[device_id] = temperature
        else:
            max_temperatures[device_id] = max(max_temperatures[device_id], temperature)

        # Calculate data points
        if device_id not in data_points:
            data_points[device_id] = 1
        else:
            data_points[device_id] += 1

        # Calculate total distance
        if device_id not in total_distances:
            total_distances[device_id] = 0.0
        if index > 0:
            prev_location = df.loc[index - 1, "location"]
            distance = calculate_distance(prev_location, location)
            total_distances[device_id] += distance

    # Store the aggregated data in MySQL
    for device_id in max_temperatures:
        max_temperature = max_temperatures[device_id]
        num_data_points = data_points[device_id]
        distance = total_distances[device_id]

        insert_query = f"""
            INSERT INTO aggregated_data (device_id, hour_start, max_temperature, data_points, total_distance)
            VALUES ('{device_id}', '{hour_start}', {max_temperature}, {num_data_points}, {distance})
            ON DUPLICATE KEY UPDATE
            max_temperature = VALUES(max_temperature),
            data_points = VALUES(data_points),
            total_distance = VALUES(total_distance)
        """
        mysql_engine.execute(insert_query)

    print('Data aggregation for the last hour completed.')
    sleep(3600)  # Sleep for 1 hour before performing the next aggregation
