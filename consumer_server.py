from kafka import KafkaConsumer

class ConsumerServer(KafkaConsumer):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
if __name__ == "__main__":
    consumer = ConsumerServer(bootstrap_servers='localhost:9092',
                              auto_offset_reset='earliest',
                              consumer_timeout_ms=1000)
    
    consumer.subscribe(['crime-reports.v1'])
    
    for msg in consumer:
        print(msg.value)