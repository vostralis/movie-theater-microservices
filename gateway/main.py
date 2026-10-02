import json, os, pika
from flask import Flask, jsonify, request


app = Flask(__name__)

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.getenv('RABBITMQ_DEFAULT_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_DEFAULT_PASS', 'guest')


def get_connection():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=credentials
    )

    return pika.BlockingConnection(parameters)


@app.route('/movies/upload', methods=['POST'])
def upload_movie():
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body is missing or contains invalid JSON'}), 400 

    try:
        connection = get_connection()
        channel = connection.channel()

        channel.queue_declare(queue='movies.new', durable=True)
        channel.basic_publish(
            exchange='',
            routing_key='movies.new',
            body=json.dumps(data),
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
        )
        
        connection.close()

        return jsonify({
            'status': 'accepted',
            'message': 'Movie upload request queued successfully'
        }), 202

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)

