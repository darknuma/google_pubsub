from google.cloud import pubsub_v1

def create_topic(project_id: str, topic_id: str, service_account_file: str) -> None:

    """Creating a new Pub/Sub topic."""
    # Creating Pub/Sub Publisher Client, Specifying path of Topic
    publisher = pubsub_v1.PublisherClient.from_service_account_file(service_account_file)
    topic_path = publisher.topic_path(project_id, topic_id)

    # Creating a Topic Object at the topic_path
    topic = publisher.create_topic(request={"name": topic_path})

    # Printing the created Topic
    print(f"Created topic: {topic.name}")



def create_push_subscription(project_id: str, topic_id: str, subscription_id: str, endpoint: str, service_account_file: str) -> None:
    
    """Create a new push subscription on the given topic."""
    # [START pubsub_create_push_subscription]
    

    # TODO(developer)
    # project_id = "your-project-id"
    # topic_id = "your-topic-id"
    # subscription_id = "your-subscription-id"
    # endpoint = "https://my-test-project.appspot.com/push"

    publisher = pubsub_v1.PublisherClient.from_service_account_file(service_account_file)
    subscriber = pubsub_v1.SubscriberClient.from_service_account_file(service_account_file)
    topic_path = publisher.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    push_config = pubsub_v1.types.PushConfig(push_endpoint=endpoint)

    # Wrap the subscriber in a 'with' block to automatically call close() to
    # close the underlying gRPC channel when done.
    with subscriber:
        subscription = subscriber.create_subscription(
            request={
                "name": subscription_path,
                "topic": topic_path,
                "push_config": push_config,
            }
        )

    print(f"Push subscription created: {subscription}.")
    print(f"Endpoint for subscription is: {endpoint}")
    # [END pubsub_create_push_subscription]

def publish_messages(project_id: str, topic_id: str, service_account_file: str) -> None:
    
    """Publishes multiple messages to a Pub/Sub topic."""
    # [START pubsub_quickstart_publisher]
    # [START pubsub_publish]
    from google.cloud import pubsub_v1

    # TODO(developer)
    # project_id = "your-project-id"
    # topic_id = "your-topic-id"

    publisher = pubsub_v1.PublisherClient.from_service_account_file(service_account_file)
    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_id}`
    topic_path = publisher.topic_path(project_id, topic_id)

    for n in range(1, 10):
        data_str = f"Message number {n}"
        # Data must be a bytestring
        data = data_str.encode("utf-8")
        # When you publish a message, the client returns a future.
        future = publisher.publish(topic_path, data)
        print(future.result())

    print(f"Published messages to {topic_path}.")
    # [END pubsub_quickstart_publisher]
    # [END pubsub_publish]

def read_pubsub_messages(project_id: str, subscription_id: str, service_account_file: str):
    
    from google.cloud import pubsub_v1    
    # Initialize the Pub/Sub client
    subscriber = pubsub_v1.SubscriberClient.from_service_account_file(service_account_file)

    # Define the subscription path
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message):
        # Process the received message
        print(f"Received message: {message.data}")
        # Acknowledge the message to mark it as processed
        message.ack()

    # Subscribe to the specified subscription and start receiving messages
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

    print(f"Listening for messages on {subscription_path}...\n")

    # Keep the script running to continue receiving messages
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()


if __name__ == "__main__":
    create_topic()