import time
import requests
import pandas as pd
from confluent_kafka import Producer
from io import StringIO


# Download the file from the given URL and load it into a pandas DataFrame
def download_and_read_csv(url):
    response = requests.get(url)
    response.raise_for_status()  # Check if the request was successful
    csv_content = response.text
    # Use pandas to read the CSV content into a DataFrame
    df = pd.read_csv(StringIO(csv_content))
    return df

# Define a function to produce records to Kafka
def produce_to_kafka(csv_url, kafka_topic, kafka_broker='localhost:9092'):
    # Initialize the Kafka producer
    producer = Producer({'bootstrap.servers': kafka_broker})

    record_count = 0
    batch_size = 10
    max_records = 1000
    sleep_interval = 10  # seconds between batches

    # Download and read the CSV file into a pandas DataFrame
    df = download_and_read_csv(csv_url)

    # Convert the DataFrame into a list of records (e.g., as dictionaries or strings)
    records = df.to_dict(orient='records')

    try:
        while record_count < max_records:
            # Create a batch of 10 records
            batch = []
            for _ in range(batch_size):
                if record_count >= max_records or not records:
                    break
                record = records.pop(0)  # Get the next record from the list
                batch.append(record)
                record_count += 1

            # Send each batch to Kafka
            for record in batch:
                # Convert record to JSON or string depending on your use case
                producer.produce(kafka_topic, value=str(record))  # Here, we convert to a string
            producer.flush()  # Ensure all messages in the producer buffer are sent

            print(f'Batch of {len(batch)} records written to Kafka.')

            if record_count >= max_records:
                print("Reached 1000 records. Stopping producer.")
                break

            # Sleep between batches
            if batch:
                time.sleep(sleep_interval)

    except Exception as e:
        print(f"Error producing records to Kafka: {e}")
    finally:
        # Close the producer
        producer.close()

# Example usage
if __name__ == "__main__":
    csv_url = 'https://storage.googleapis.com/test_iitm_bucket_gokul_gcp/Tesla.csv'
    kafka_topic = 'test-topic'  # Replace with your Kafka topic name
    produce_to_kafka(csv_url, kafka_topic)
