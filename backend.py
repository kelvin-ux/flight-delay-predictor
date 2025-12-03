from flask import Flask, request, jsonify
from flask_cors import CORS
from kafka import KafkaProducer, KafkaConsumer
import json
import threading
import time
import os
import sys

# Konfiguracja PySpark
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

MODEL_A_PATH = 'model_a_gbt'  # Model bez DepDelay
MODEL_B_PATH = 'model_b_lr'   # Model z DepDelay

CARRIER_STATS = {}
ROUTE_DISTANCES = {}
HOUR_STATS = {}
ROUTE_STATS = {}
ORIGIN_STATS = {}
DEST_STATS = {}
CARRIER_HOUR_STATS = {}


spark = SparkSession.builder \
    .appName("FlightDelayPredictionAPI") \
    .config("spark.driver.memory", "4g") \
    .master("local[*]") \
    .getOrCreate()

print("Loading models...")
try:
    from pyspark.ml import PipelineModel
    model_a = PipelineModel.load(MODEL_A_PATH)
    model_b = PipelineModel.load(MODEL_B_PATH)
    print("Models loaded successfully")
except Exception as e:
    print(f"Warning: Could not load models: {e}")
    print("Run training script first to save models!")
    model_a = None
    model_b = None

try:
    with open('aggregated_stats.pkl', 'rb') as f:
        stats = pickle.load(f)
        CARRIER_STATS = stats['carrier_stats']
        HOUR_STATS = stats['hour_stats']
        ROUTE_STATS = stats['route_stats']
        ORIGIN_STATS = stats['origin_stats']
        DEST_STATS = stats['dest_stats']
        CARRIER_HOUR_STATS = stats['carrier_hour_stats']
        ROUTE_DISTANCES = stats.get('route_distances', {})
    print("Aggregated stats loaded")
except Exception as e:
    print(f"Could not load stats: {e}")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def calculate_aggregated_features(flight_data):
    carrier = flight_data.get('UniqueCarrier')
    dep_hour = flight_data.get('DepHour')
    route = f"{flight_data.get('Origin')}-{flight_data.get('Dest')}"
    origin = flight_data.get('Origin')
    dest = flight_data.get('Dest')

    return {
        'AvgCarrierDelay': CARRIER_STATS.get(carrier, 60.0),
        'AvgHourDelay': HOUR_STATS.get(dep_hour, 55.0),
        'AvgRouteDelay': ROUTE_STATS.get(route, 58.0),
        'AvgOriginDelay': ORIGIN_STATS.get(origin, 57.0),
        'AvgDestDelay': DEST_STATS.get(dest, 57.0),
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

    distance = flight_data.get('Distance', 1000)
    if distance < 500:
        distance_cat = "Short"
    elif distance <= 1500:
        distance_cat = "Medium"
    else:
        distance_cat = "Long"

    is_rush_hour = 1 if (6 <= dep_hour <= 9) or (17 <= dep_hour <= 20) else 0
    is_weekend = 1 if flight_data.get('DayOfWeek', 1) in [6, 7] else 0
    agg_features = calculate_aggregated_features(flight_data)

    processed = {
        'DayOfWeek': flight_data.get('DayOfWeek', 1),
        'Month': flight_data.get('Month', 1),
        'Day': flight_data.get('Day', 1),
        'DepHour': dep_hour,
        'IsWeekend': is_weekend,
        'IsRushHour': is_rush_hour,
        'Distance': distance,
        'CRSElapsedTime': flight_data.get('CRSElapsedTime', 120),
        'CRSArrHour': flight_data.get('CRSArrHour', 14),
        'UniqueCarrier': flight_data.get('UniqueCarrier', 'WN'),
        'Origin': flight_data.get('Origin', 'ORD'),
        'Dest': flight_data.get('Dest', 'LAX'),
        'TimeOfDay': time_of_day,
        'DistanceCategory': distance_cat,
        **agg_features
    }
    
    if include_dep_delay:
        processed['DepDelay'] = flight_data.get('DepDelay', 0)
        processed['TaxiOut'] = flight_data.get('TaxiOut', 15)
    
    return processed

def predict_delay(flight_data, model_type='A'):
    if model_a is None or model_b is None:
        return {"error": "Models not loaded"}
    
    try:
        if model_type == 'A':
            processed = preprocess_input(flight_data, include_dep_delay=False)
            model = model_a
        else:
            processed = preprocess_input(flight_data, include_dep_delay=True)
            model = model_b

        df = spark.createDataFrame([Row(**processed)])
        
        predictions = model.transform(df)
        predicted_delay = predictions.select('prediction').first()[0]
        
        return {
            'predicted_delay': round(predicted_delay, 2),
            'model_type': f'Model {model_type}',
            'status': 'success'
        }
    except Exception as e:
        return {
            'error': str(e),
            'status': 'error'
        }

def kafka_consumer_worker():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='flight-delay-predictor'
    )
    
    print(f"✓ Kafka consumer started, listening on topic: {KAFKA_TOPIC}")
    
    for message in consumer:
        try:
            flight_data = message.value
            request_id = flight_data.get('request_id')
            model_type = flight_data.get('model_type', 'A')
            
            print(f"Processing request {request_id} with Model {model_type}...")

            result = predict_delay(flight_data, model_type)
            result['request_id'] = request_id
            
            print(f"Prediction: {result['predicted_delay']} minutes")
            
        except Exception as e:
            print(f"Error processing message: {e}")

consumer_thread = threading.Thread(target=kafka_consumer_worker, daemon=True)
consumer_thread.start()

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'kafka_broker': KAFKA_BROKER,
        'models_loaded': model_a is not None and model_b is not None
    })

@app.route('/predict', methods=['POST'])
def predict_endpoint():
    try:
        flight_data = request.json
        model_type = flight_data.get('model_type', 'A')
        
        result = predict_delay(flight_data, model_type)
        return jsonify(result)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/predict-kafka', methods=['POST'])
def predict_kafka_endpoint():
    try:
        flight_data = request.json
        request_id = f"req_{int(time.time() * 1000)}"
        flight_data['request_id'] = request_id

        producer.send(KAFKA_TOPIC, flight_data)
        producer.flush()
        
        return jsonify({
            'status': 'request_sent',
            'request_id': request_id,
            'message': 'Prediction request sent to Kafka queue'
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/carriers', methods=['GET'])
def get_carriers():
    carriers = ['WN', 'AA', 'DL', 'UA', 'B6', 'AS', 'F9', 'NK', 'HA']
    return jsonify(carriers)

@app.route('/airports', methods=['GET'])
def get_airports():
    airports = {
        'ORD': 'Chicago O\'Hare',
        'LAX': 'Los Angeles',
        'ATL': 'Atlanta',
        'DFW': 'Dallas/Fort Worth',
        'JFK': 'New York JFK',
        'DEN': 'Denver',
        'SFO': 'San Francisco',
        'LAS': 'Las Vegas',
        'PHX': 'Phoenix',
        'MIA': 'Miami'
    }
    return jsonify(airports)


@app.route('/distance', methods=['GET'])
def get_distance():
    origin = request.args.get('origin')
    dest = request.args.get('dest')

    if not origin or not dest:
        return jsonify({'error': 'Missing origin or dest'}), 400

    key_direct = f"{origin}-{dest}"
    distance = ROUTE_DISTANCES.get(key_direct)

    if distance is None:
        key_reverse = f"{dest}-{origin}"
        distance = ROUTE_DISTANCES.get(key_reverse)

    if distance:
        return jsonify({
            'distance': distance,
            'found': True,
            'source': 'database'
        })
    else:
        print(f"Trasa nieznana: {origin}-{dest}")
        return jsonify({
            'distance': 1000,
            'found': False,
            'message': 'Route not found, using default'
        })


if __name__ == '__main__':
    print("\n" + "="*80)
    print("🚀 Flight Delay Prediction API")
    print("="*80)
    print(f"Kafka Broker: {KAFKA_BROKER}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"Models loaded: {model_a is not None and model_b is not None}")
    print("\nEndpoints:")
    print("  GET  /health           - Health check")
    print("  GET  /carriers         - Lista carriers")
    print("  GET  /airports         - Lista lotnisk")
    print("  POST /predict          - Bezpośrednia predykcja")
    print("  POST /predict-kafka    - Predykcja przez Kafkę")
    print("="*80 + "\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000)