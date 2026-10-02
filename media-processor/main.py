import os, json, pika, sys


RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.getenv('RABBITMQ_DEFAULT_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_DEFAULT_PASS', 'guest')


def process_transcode_task(ch, method, properties, body):
    task = json.loads(body)

    print(f"\nTask accepted: movie #{task['movie_id']} '{task['title']}'")
    print(f"Parameters: resolution {task['resolution']}, duration {task['duration']}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=credentials
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.queue_declare(queue='movies.transcode', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue='movies.transcode',
        on_message_callback=process_transcode_task
    )
    
    print("Service started. Waiting for tasks from 'movies.transcode'...")

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
