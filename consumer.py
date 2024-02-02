from cassandra.cluster import Cluster
from kafka import KafkaConsumer
from json import loads, dumps

# Initialize Kafka consumer
try:
    consumer = KafkaConsumer('demo_test',
                             bootstrap_servers=['<Your Public IP>:9092'],
                             value_deserializer=lambda x: loads(x.decode('utf-8')))
except Exception as e:
    # Handle Kafka consumer initialization error
    print("An error occurred while initializing the Kafka consumer:", e)
    consumer = None

# Initialize Cassandra cluster and session
try:
    cluster = Cluster(['<Your Public IP>'])
    session = cluster.connect()
    # Create or connect to the 'stockmarket' keyspace
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS stockmarket WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")
    session.set_keyspace("stockmarket")
    # Create or connect to the 'stock_market_data' table
    session.execute('''CREATE TABLE IF NOT EXISTS stock_market_data (
						                        id int PRIMARY KEY, 
                                                "index" varchar,
                                                date varchar,
                                                open float,
                                                high float,
                                                low float,
                                                close float,
                                                "adj close" float,
                                                volume bigint,
                                                closeUSD float
                                                );''')
except Exception as e:
    # Handle Cassandra session or keyspace/table creation error
    print("An error occurred while initializing the Cassandra session or creating the keyspace or table:", e)
    session = None

# If Kafka consumer and Cassandra session are successfully initialized
if consumer and session is not None:
    message_id = 0  # Initialize message ID counter
    # Continuously consume messages from Kafka
    for message in consumer:
        if (message.value):  # Check if message has a value
            try:
                message_id += 1  # Increment message ID
                # Create a new dictionary with message ID and message value
                new_data = {'Id': message_id}
                new_data.update(message.value)
                final_data = dumps(new_data)  # Convert dictionary to JSON string
                # Insert JSON data into the 'stock_market_data' table
                session.execute(
                    f"INSERT INTO stock_market_data JSON'{final_data}';")
      
