from flask import Flask, request, jsonify
from flask_cors import CORS
from kafka import KafkaProducer, KafkaConsumer
import json
import threading
import time
import os
import sys
import uuid

os.environ["JAVA_HOME"] = "/Users/jedrzej/Library/Java/JavaVirtualMachines/openjdk-23.0.2/Contents/Home"
os.environ["JDK_JAVA_OPTIONS"] = "-Djava.security.manager=allow"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import Row
import pickle

app = Flask(__name__)
CORS(app)

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'flight-predictions'

MODEL_A_PATH = 'model_a_gbt'
MODEL_B_PATH = 'model_b_lr'
STATS_FILE = 'aggregated_stats.pkl'

PREDICTIONS_STORE = {}

spark = SparkSession.builder \
    .appName("FlightDelayPredictionAPI") \
    .config("spark.driver.memory", "4g") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    from pyspark.ml import PipelineModel

    model_a = PipelineModel.load(MODEL_A_PATH)
    model_b = PipelineModel.load(MODEL_B_PATH)

    with open(STATS_FILE, 'rb') as f:
        stats = pickle.load(f)
        CARRIER_STATS = stats.get('carrier_stats', {})
        HOUR_STATS = stats.get('hour_stats', {})
        ROUTE_STATS = stats.get('route_stats', {})
        ORIGIN_STATS = stats.get('origin_stats', {})
        DEST_STATS = stats.get('dest_stats', {})
        CARRIER_HOUR_STATS = stats.get('carrier_hour_stats', {})
        ROUTE_DISTANCES = stats.get('route_distances', {})
    print("Models and Stats loaded successfully.")
except Exception as e:
    print(f"Critical Error loading resources: {e}")
    model_a, model_b = None, None
    CARRIER_STATS = {}

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    print(f"Kafka Producer Error: {e}")
    producer = None

def calculate_aggregated_features(flight_data):
    carrier = flight_data.get('UniqueCarrier')
    dep_hour = flight_data.get('DepHour')
    route = f"{flight_data.get('Origin')}-{flight_data.get('Dest')}"

    return {
        'AvgCarrierDelay': CARRIER_STATS.get(carrier, 60.0),
        'AvgHourDelay': HOUR_STATS.get(dep_hour, 55.0),
        'AvgRouteDelay': ROUTE_STATS.get(route, 58.0),
        'AvgOriginDelay': ORIGIN_STATS.get(flight_data.get('Origin'), 57.0),
        'AvgDestDelay': DEST_STATS.get(flight_data.get('Dest'), 57.0),
        'AvgCarrierHourDelay': CARRIER_HOUR_STATS.get(f"{carrier}_{dep_hour}", 58.0),
        'CarrierFlightCount': 10000,
        'RouteFlightCount': 5000
    }


def preprocess_input(flight_data, include_dep_delay=False):
    dep_hour = flight_data.get('DepHour', 12)

    if 6 <= dep_hour <= 11:
        time_of_day = "Morning"
    elif 12 <= dep_hour <= 17:
        time_of_day = "Afternoon"
    elif 18 <= dep_hour <= 21:
        time_of_day = "Evening"
    else:
        time_of_day = "Night"

    dist = flight_data.get('Distance', 1000)
    if dist < 500:
        dist_cat = "Short"
    elif dist <= 1500:
        dist_cat = "Medium"
    else:
        dist_cat = "Long"

    agg = calculate_aggregated_features(flight_data)

    row_dict = {
        'DayOfWeek': flight_data.get('DayOfWeek', 1),
        'Month': flight_data.get('Month', 1),
        'Day': flight_data.get('Day', 15),
        'DepHour': dep_hour,
        'IsWeekend': 1 if flight_data.get('DayOfWeek') in [6, 7] else 0,
        'IsRushHour': 1 if (6 <= dep_hour <= 9) or (17 <= dep_hour <= 20) else 0,
        'Distance': dist,
        'CRSElapsedTime': flight_data.get('CRSElapsedTime', 120),
        'CRSArrHour': flight_data.get('CRSArrHour', 14),
        'UniqueCarrier': flight_data.get('UniqueCarrier', 'WN'),
        'Origin': flight_data.get('Origin', 'ORD'),
        'Dest': flight_data.get('Dest', 'LAX'),
        'TimeOfDay': time_of_day,
        'DistanceCategory': dist_cat,
        **agg
    }

    if include_dep_delay:
        row_dict['DepDelay'] = flight_data.get('DepDelay', 0)
        row_dict['TaxiOut'] = flight_data.get('TaxiOut', 15)

    return row_dict


def predict_delay(flight_data, model_type='A'):
    if not model_a or not model_b:
        return {"error": "Models not loaded"}

    try:
        include_dep = (model_type == 'B')
        processed_data = preprocess_input(flight_data, include_dep_delay=include_dep)

        df = spark.createDataFrame([Row(**processed_data)])
        model = model_b if include_dep else model_a

        result = model.transform(df)
        prediction_val = result.select('prediction').first()[0]

        return {
            'predicted_delay': round(prediction_val, 2),
            'model_type': model_type,
            'status': 'success'
        }
    except Exception as e:
        print(f"Prediction Error: {e}")
        return {'error': str(e), 'status': 'error'}


def kafka_consumer_worker():
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='backend_processor_group'
        )
        print("✓ Kafka Consumer Thread Started.")

        for message in consumer:
            data = message.value
            req_id = data.get('request_id')

            if req_id:
                print(f"[Kafka] Processing request: {req_id}")

                result = predict_delay(data, data.get('model_type', 'A'))

                PREDICTIONS_STORE[req_id] = result
                print(f"[Kafka] Result ready for {req_id}: {result.get('predicted_delay')}")

    except Exception as e:
        print(f"Kafka Consumer Thread Error: {e}")


threading.Thread(target=kafka_consumer_worker, daemon=True).start()



@app.route('/predict-kafka', methods=['POST'])
def predict_kafka():
    if not producer:
        return jsonify({'error': 'Kafka Producer unavailable'}), 500

    try:
        data = request.json
        req_id = str(uuid.uuid4())
        data['request_id'] = req_id

        producer.send(KAFKA_TOPIC, data)
        producer.flush()

        return jsonify({
            'status': 'queued',
            'request_id': req_id,
            'message': 'Zadanie wysłane do Kafki. Odpytuj endpoint /result/<id>'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/result/<request_id>', methods=['GET'])
def get_result(request_id):
    result = PREDICTIONS_STORE.get(request_id)

    if result:
        del PREDICTIONS_STORE[request_id]
        return jsonify({'status': 'ready', 'data': result})
    else:
        return jsonify({'status': 'processing'})


@app.route('/distance', methods=['GET'])
def get_distance():
    origin = request.args.get('origin')
    dest = request.args.get('dest')
    key = f"{origin}-{dest}"
    dist = ROUTE_DISTANCES.get(key, ROUTE_DISTANCES.get(f"{dest}-{origin}"))

    if dist: return jsonify({'found': True, 'distance': dist})
    return jsonify({'found': False, 'distance': 1000})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)