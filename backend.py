from flask import Flask, request, jsonify
from flask_cors import CORS
from kafka import KafkaProducer, KafkaConsumer
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import threading
import os
import sys
import uuid
import time

os.environ["JAVA_HOME"] = "change location"
os.environ["JDK_JAVA_OPTIONS"] = "-Djava.security.manager=allow"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.ml import PipelineModel
import pickle

app = Flask(__name__)
CORS(app)

KAFKA_BROKER = 'localhost:9092'

TOPIC_INPUT_REGRESSION = 'input-regression'
TOPIC_INPUT_CLASSIFICATION = 'input-classification'
TOPIC_OUTPUT_REGRESSION = 'output-regression'
TOPIC_OUTPUT_CLASSIFICATION = 'output-classification'

RESPONSE_TIMEOUT = 10.0

MODEL_REGRESSION_A_PATH = 'model_a_gbt'
MODEL_REGRESSION_B_PATH = 'model_b_lr'
MODEL_CLASSIFICATION_30_PATH = 'model_classification_30'
MODEL_CLASSIFICATION_60_PATH = 'model_classification_60'
STATS_FILE = 'aggregated_stats.pkl'

spark = SparkSession.builder \
    .appName("FlightDelayPredictionAPI") \
    .config("spark.driver.memory", "4g") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    model_reg_a = PipelineModel.load(MODEL_REGRESSION_A_PATH)
    model_reg_b = PipelineModel.load(MODEL_REGRESSION_B_PATH)
    print("Regression models loaded")
except Exception as e:
    print(f"Error loading regression models: {e}")
    model_reg_a, model_reg_b = None, None

try:
    model_cls_30 = PipelineModel.load(MODEL_CLASSIFICATION_30_PATH)
    model_cls_60 = PipelineModel.load(MODEL_CLASSIFICATION_60_PATH)
    print("Classification models loaded")
except Exception as e:
    print(f"Error loading classification models: {e}")
    model_cls_30, model_cls_60 = None, None

try:
    with open(STATS_FILE, 'rb') as f:
        stats = pickle.load(f)
        CARRIER_STATS = stats.get('carrier_stats', {})
        HOUR_STATS = stats.get('hour_stats', {})
        ROUTE_STATS = stats.get('route_stats', {})
        ORIGIN_STATS = stats.get('origin_stats', {})
        DEST_STATS = stats.get('dest_stats', {})
        CARRIER_HOUR_STATS = stats.get('carrier_hour_stats', {})
        ROUTE_DISTANCES = stats.get('route_distances', {})
    print("Stats loaded")
except Exception as e:
    print(f"Error loading stats: {e}")
    CARRIER_STATS, HOUR_STATS, ROUTE_STATS = {}, {}, {}
    ORIGIN_STATS, DEST_STATS, CARRIER_HOUR_STATS = {}, {}, {}
    ROUTE_DISTANCES = {}


## Kafka

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Kafka Producer connected")
except Exception as e:
    print(f"Kafka Producer Error: {e}")
    producer = None


## Helpers

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


## Prediction

def predict_regression(flight_data, model_type='A'):
    if not model_reg_a or not model_reg_b:
        return {"error": "Regression models not loaded", "status": "error"}

    try:
        include_dep = (model_type == 'B')
        processed_data = preprocess_input(flight_data, include_dep_delay=include_dep)

        df = spark.createDataFrame([Row(**processed_data)])
        model = model_reg_b if include_dep else model_reg_a

        result = model.transform(df)
        prediction_val = result.select('prediction').first()[0]

        return {
            'predicted_delay_minutes': round(prediction_val, 2),
            'model_type': model_type,
            'status': 'success'
        }
    except Exception as e:
        print(f"Regression error: {e}")
        return {'error': str(e), 'status': 'error'}


def predict_classification(flight_data):
    if not model_cls_30 or not model_cls_60:
        return {"error": "Classification models not loaded", "status": "error"}

    try:
        processed_data = preprocess_input(flight_data, include_dep_delay=False)
        df = spark.createDataFrame([Row(**processed_data)])

        result_30 = model_cls_30.transform(df)
        prob_30_row = result_30.select('probability').first()[0]
        prob_over_30 = float(prob_30_row[1]) * 100

        result_60 = model_cls_60.transform(df)
        prob_60_row = result_60.select('probability').first()[0]
        prob_over_60 = float(prob_60_row[1]) * 100

        return {
            'probability_over_30min': round(prob_over_30, 1),
            'probability_over_60min': round(prob_over_60, 1),
            'status': 'success'
        }
    except Exception as e:
        print(f"Classification error: {e}")
        return {'error': str(e), 'status': 'error'}


## Kafka Consumers

def regression_consumer_worker():
    try:
        consumer = KafkaConsumer(
            TOPIC_INPUT_REGRESSION,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='regression_processor_group'
        )
        print(f"Consumer started: {TOPIC_INPUT_REGRESSION}")

        for message in consumer:
            data = message.value
            req_id = data.get('request_id')

            if req_id:
                print(f"[Regression] Processing: {req_id}")
                result = predict_regression(data, data.get('model_type', 'A'))
                result['request_id'] = req_id

                producer.send(TOPIC_OUTPUT_REGRESSION, result)
                producer.flush()
                print(f"[Regression] Done: {req_id}")

    except Exception as e:
        print(f"Regression consumer error: {e}")


def classification_consumer_worker():
    try:
        consumer = KafkaConsumer(
            TOPIC_INPUT_CLASSIFICATION,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='classification_processor_group'
        )
        print(f"Consumer started: {TOPIC_INPUT_CLASSIFICATION}")

        for message in consumer:
            data = message.value
            req_id = data.get('request_id')

            if req_id:
                print(f"[Classification] Processing: {req_id}")
                result = predict_classification(data)
                result['request_id'] = req_id

                producer.send(TOPIC_OUTPUT_CLASSIFICATION, result)
                producer.flush()
                print(f"[Classification] Done: {req_id}")

    except Exception as e:
        print(f"Classification consumer error: {e}")


threading.Thread(target=regression_consumer_worker, daemon=True).start()
threading.Thread(target=classification_consumer_worker, daemon=True).start()


## Response waiting

def wait_for_response(topic: str, request_id: str, timeout: float):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='latest',
        enable_auto_commit=False,
        group_id=None,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    deadline = time.time() + timeout
    result = None

    try:
        while time.time() < deadline and result is None:
            msg_pack = consumer.poll(timeout_ms=500)

            for tp, messages in msg_pack.items():
                for msg in messages:
                    data = msg.value
                    if data.get('request_id') == request_id:
                        result = data
                        break
                if result:
                    break
    finally:
        consumer.close()

    return result


## API Endpoints

@app.route('/predict', methods=['POST'])
def predict():
    if not producer:
        return jsonify({'error': 'Kafka Producer unavailable'}), 500

    try:
        data = request.json
        req_id = str(uuid.uuid4())
        data['request_id'] = req_id

        producer.send(TOPIC_INPUT_REGRESSION, data)
        producer.send(TOPIC_INPUT_CLASSIFICATION, data)
        producer.flush()

        print(f"[API] Sent request: {req_id}")

        results = {}

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(wait_for_response, TOPIC_OUTPUT_REGRESSION, req_id, RESPONSE_TIMEOUT): 'regression',
                executor.submit(wait_for_response, TOPIC_OUTPUT_CLASSIFICATION, req_id, RESPONSE_TIMEOUT): 'classification'
            }

            for future in as_completed(futures):
                model_type = futures[future]
                try:
                    result = future.result()
                    if result:
                        results[model_type] = result
                except Exception as e:
                    print(f"Error waiting for {model_type}: {e}")

        if 'regression' in results and 'classification' in results:
            return jsonify({
                'status': 'success',
                'request_id': req_id,
                'regression': results['regression'],
                'classification': results['classification']
            })
        elif results:
            return jsonify({
                'status': 'partial',
                'request_id': req_id,
                **results
            })
        else:
            return jsonify({
                'status': 'timeout',
                'error': 'No response from models'
            }), 504

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/predict-regression', methods=['POST'])
def predict_regression_only():
    if not producer:
        return jsonify({'error': 'Kafka Producer unavailable'}), 500

    try:
        data = request.json
        req_id = str(uuid.uuid4())
        data['request_id'] = req_id

        producer.send(TOPIC_INPUT_REGRESSION, data)
        producer.flush()

        result = wait_for_response(TOPIC_OUTPUT_REGRESSION, req_id, RESPONSE_TIMEOUT)

        if result:
            return jsonify({'status': 'success', 'data': result})
        else:
            return jsonify({'status': 'timeout', 'error': 'No response'}), 504

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/predict-classification', methods=['POST'])
def predict_classification_only():
    if not producer:
        return jsonify({'error': 'Kafka Producer unavailable'}), 500

    try:
        data = request.json
        req_id = str(uuid.uuid4())
        data['request_id'] = req_id

        producer.send(TOPIC_INPUT_CLASSIFICATION, data)
        producer.flush()

        result = wait_for_response(TOPIC_OUTPUT_CLASSIFICATION, req_id, RESPONSE_TIMEOUT)

        if result:
            return jsonify({'status': 'success', 'data': result})
        else:
            return jsonify({'status': 'timeout', 'error': 'No response'}), 504

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/distance', methods=['GET'])
def get_distance():
    origin = request.args.get('origin')
    dest = request.args.get('dest')
    key = f"{origin}-{dest}"
    dist = ROUTE_DISTANCES.get(key, ROUTE_DISTANCES.get(f"{dest}-{origin}"))

    if dist:
        return jsonify({'found': True, 'distance': dist})
    return jsonify({'found': False, 'distance': 1000})


@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'ok',
        'models': {
            'regression_a': model_reg_a is not None,
            'regression_b': model_reg_b is not None,
            'classification_30': model_cls_30 is not None,
            'classification_60': model_cls_60 is not None
        },
        'kafka': producer is not None
    })


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)