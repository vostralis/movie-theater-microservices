import os, json, pika, sys


RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.getenv('RABBITMQ_DEFAULT_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_DEFAULT_PASS', 'guest')

# Next film ID
movie_id_sequence = 1


def process_new_movie(ch, method, properties, body):
    global movie_id_sequence

    movie_data = json.loads(body)
   
    # Database insert imitation
    assigned_id = movie_id_sequence
    movie_id_sequence += 1
    
    print(f"\nMovie saved to database: ID={assigned_id}, title='{movie_data['title']}'")

    transcode_task = {
        'movie_id': assigned_id,
        'title': movie_data['title'],
        'duration': movie_data['duration'],
        'resolution': movie_data['resolution']
    }

    ch.queue_declare(queue='movies.transcode', durable=True)
    ch.basic_publish(
        exchange='',
        routing_key='movies.transcode',
        body=json.dumps(transcode_task),
        properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
    )

    print(f'Transcoding task for movie #{assigned_id} published to queue.')

    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=credentials
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    channel.queue_declare(queue='movies.new', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue='movies.new',
        on_message_callback=process_new_movie
    )

    print('Service started. Waiting for movies from "movies.new"...')

    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
