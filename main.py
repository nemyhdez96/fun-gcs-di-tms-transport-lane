import functions_framework
import json, os
import gzip
import logging

from google.cloud import storage
from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

publisher = pubsub_v1.PublisherClient()


topic_name = os.environ.get("PUBSUB_TOPIC")
project_name = os.environ.get("GCP_PROJECT")
if not topic_name or not project_name:
    logging.error(f"Error: La variable de entorno PUBSUB_TOPIC ={topic_name} o GCP_PROJECT = {project_name}.")
    raise ValueError(f"Error: La variable de entorno PUBSUB_TOPIC ={topic_name} o GCP_PROJECT = {project_name}.")

topic_path = publisher.topic_path(project_name, topic_name)



@functions_framework.cloud_event
def main_gcs(cloud_event):    

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]
    logging.info(f"Pricezando el evento {event_id}. Tipo: {event_type}")

    data = cloud_event.data


    bucket_name = data['bucket']
    file_name = data['name']
    logging.info(f"Procesando archivo: gs://{bucket_name}/{file_name}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    try:
        compressed_data = blob.download_as_bytes()
        decompressed_data = gzip.decompress(compressed_data).decode('utf-8')
        logging.info(f"Data descompimida: {decompressed_data}")

        data_transportlane = []
        for line in decompressed_data.strip().split('\n'):
            if line:
                try:
                    json_object = json.loads(line)
                    if json_object:
                        data_transportlane.append(
                            {
                                "operation": json_object["source_metadata"]["change_type"],
                                "timestamp": json_object["read_timestamp"],
                                "data": json_object["payload"]
                            }
                        )
                except json.JSONDecodeError as e:
                    logging.error(f"Error al decodificar JSON en la línea: {line} - {e}")
       
        if data_transportlane:
            logging.info(f"Data a enviar: {data_transportlane}")
            message_json = json.dumps(data_transportlane).encode("utf-8")
            future = publisher.publish(topic_path, message_json)
            logging.info(f"Mensaje publicado con ID: {future.result()}")

    except Exception as e:
        logging.error(f"Error al leer el archivo gs://{bucket_name}/{file_name}: {e}")
