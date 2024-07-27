from dotenv import load_dotenv
from faker import Faker
from google.cloud import pubsub_v1
import json
import time
import os


load_dotenv()
os.getenv("GOOGLE_AUTH_FILE")

GCS_AUTH = os.environ.get("GOOGLE_AUTH_FILE")

# Set the environment variable for authentication
if GCS_AUTH:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_AUTH

fake = Faker()

project_id = 'superb-webbing-314701'
topic_id = 'first-topic'

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


def generate_fake_data ():
    return {
        "name": fake.name(),
        "address": fake.address(),
        "email": fake.email(),
        "phone_number": fake.phone_number(),
        "birthdate": fake.date_of_birth().isoformat(),
        "created_at": fake.date_time_this_year().isoformat(),

    }



def publishers(data):
    data_str = json.dumps(data)

    data_bytes = data_str.encode("utf-8")

    future = publisher.publish(topic_path, data_bytes)
    print(f"Published message ID: {future.result()}")


def main():
    while True:
        user_data = generate_fake_data()
        publishers(user_data)
        time.sleep(5)

if __name__ == "__main__":
    main()