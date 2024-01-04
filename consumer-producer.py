from kafka import KafkaConsumer, KafkaProducer
import json
import time
import random

# Inicijalizacija Kafka consumera za čitanje podataka sa prvog topica
consumer = KafkaConsumer('topic1', bootstrap_servers='localhost:9092', group_id='my-group', max_poll_records=5)

# Inicijalizacija Kafka producera za slanje obradjenih podataka na drugi topic
producer = KafkaProducer(bootstrap_servers='localhost:9092', acks='all')

# Kafka topic za slanje obradjenih podataka
output_topic = 'topic2'


 # Inicijalizacija promenljive za praćenje primljenih podataka
received_data_count = 0


# Obrada i slanje podataka
for message in consumer:
    weather_data = message.value.decode('utf-8')
    weather_data_json = json.loads(weather_data)

    # Dodaj primljene podatke u promenljive
    received_data_count += 1
    temperature = weather_data_json.get('main', {}).get('temp', 0)
    humidity = weather_data_json.get('main', {}).get('humidity', 5)
    mintemp = weather_data_json.get('main', {}).get('temp_min', 2)
    maxtemp = weather_data_json.get('main', {}).get('temp_max', 3) 
    razlika = (maxtemp - mintemp) + random.randrange(0,300)
   
    print(f"Poruka broj: {received_data_count} Temperatura: {temperature}  Vlaznost vazduha: {humidity}  Minimalna temperatura:{mintemp}  Maksimalna temperatura:{maxtemp}")
     
    # Slanje obrađenih podataka na drugi Kafka topic
    processed_data = {'difference': razlika}
    producer.send(output_topic, value=json.dumps(processed_data).encode('utf-8'))

    # Ispisivanje obrađenih podataka
    print(f"Razlika izmedju maksimalne i  minimalne vrednosti : {razlika}")



# Zatvaranje consumera i producera (ova linija neće biti dostignuta u ovom primeru)
consumer.close()
producer.close()
