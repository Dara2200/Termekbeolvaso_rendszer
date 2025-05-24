import os
import subprocess
import time
import threading
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient
import base64

# Configuration: Az ip cimek és elérési útvonalak egyedi értékek 
CONFIG = {
    "topic_id": "Szakdolgozat",
    "bash_starter": "cd /c/kafka_2.13-3.8.0/kafka_2.13-3.8.0 && bin/kafka-server-start.sh config/kraft/server_otthon_win.properties",
    "broker_id": "1",
    "bootstrap_servers": "172.16.16.196:9092",
    "controller.socket.timeout.ms": "60000",
    "advertised.listeners": "PLAINTEXT://172.16.16.210:9092",
    "controller.quorum.voters": "1@172.16.16.210:9094,2@172.16.16.196:9094",
    "listeners": "PLAINTEXT://172.16.16.210:9092,CONTROLLER://172.16.16.210:9094",
    "log_dirs": "/tmp/kraft-combined-logs",
    "kafka_config_path": r"C:\kafka_2.13-3.8.0\kafka_2.13-3.8.0\config\kraft\server_otthon_win.properties",
    "kafka_stop_script": r"C:\kafka_2.13-3.8.0\kafka_2.13-3.8.0\bin\kafka-server-stop.sh",
}

# Kafka consumer configuration
kafka_consumer_config = {
    'bootstrap.servers': CONFIG['bootstrap_servers'],
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
}

def update_server_properties(config):
    try:
        file_path = config["kafka_config_path"]
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"{file_path} not found!")

        with open(file_path, "r") as file:
            lines = file.readlines()

        updated_lines = []
        for line in lines:
            if line.startswith("controller.quorum.voters="):
                updated_lines.append(f"controller.quorum.voters={config['controller.quorum.voters']}\n")
            elif line.startswith("advertised.listeners="):
                updated_lines.append(f"advertised.listeners={config['advertised.listeners']}\n")
            elif line.startswith("controller.socket.timeout.ms="):
                updated_lines.append(f"controller.socket.timeout.ms={config['controller.socket.timeout.ms']}\n")
            elif line.startswith("broker.id="):
                updated_lines.append(f"broker.id={config['broker_id']}\n")
            elif line.startswith("log.dirs="):
                updated_lines.append(f"log.dirs={config['log_dirs']}\n")
            elif line.startswith("listeners="):
                updated_lines.append(f"listeners={config['listeners']}\n")
            else:
                updated_lines.append(line)

        with open(file_path, "w") as file:
            file.writelines(updated_lines)

        print(f"{file_path} updated successfully!")
    except Exception as e:
        print(f"Error updating server.properties: {e}")

def check_and_start_kafka_kraft(config):
    try:
        print(f"Starting Kafka broker ({config['bootstrap_servers']}) in Kraft mode...")
        subprocess.Popen(
            [r'C:\Program Files\Git\bin\bash.exe', '-c', config['bash_starter']],
            shell=True
        )
        time.sleep(15)
        print("Kafka broker started in Kraft mode.")
    except Exception as e:
        print(f"Error starting Kafka in Kraft mode: {e}")

def is_kafka_running(config, retries=5, delay=5):
    for attempt in range(retries):
        try:
            admin_client = AdminClient({'bootstrap.servers': config["bootstrap_servers"]})
            brokers = admin_client.list_topics(timeout=2).brokers
            if brokers:
                return True
        except Exception as e:
            print(f"Error checking Kafka broker (attempt {attempt+1}/{retries}): {e}")
        time.sleep(delay)
    print("Kafka broker is not running.")
    return False

def stop_kafka_kraft(config):
    try:
        print(f"Stopping Kafka broker ({config['bootstrap_servers']}) in Kraft mode...")
        subprocess.Popen(
            [r'C:\Program Files\Git\bin\bash.exe', '-c', config['kafka_stop_script']],
            shell=True)
        print("Kafka broker stopped in Kraft mode.")
    except Exception as e:
        print(f"Error stopping Kafka in Kraft mode: {e}")

def start_consumer(config, running_event):
    consumer = Consumer(kafka_consumer_config)
    consumer.subscribe([config["topic_id"]])

    try:
        while running_event.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            data = msg.value().decode('utf-8')
            try:
                weight, dist1, dist2, image_b64 = data.split(",", 3)
                weight = weight.strip()
                dist1 = dist1.strip()
                dist2 = dist2.strip()

            
                with open("output.txt", "a", encoding="utf-8") as f:
                    f.write(f"Súly: {weight}, Távolság1: {dist1}, Távolság2: {dist2}\n")

            
                with open("received_image.jpg", "wb") as f_img:
                    f_img.write(base64.b64decode(image_b64.strip()))

                print(f"Adatok kiírva és kép mentve.")

            except Exception as e:
                print(f"Hiba az adatfeldolgozásnál: {e}")

    except KeyboardInterrupt:
        print("Consumer leállítása kérése...")
    finally:
        consumer.close()

def monitor_kafka(config, running_event):
    while running_event.is_set():
        if not is_kafka_running(config):
            print("Kafka broker is down. Restarting...")
            check_and_start_kafka_kraft(config)
        time.sleep(5)

def monitor_user_input(running_event, config):
    while running_event.is_set():
        user_input = input("Írd be, hogy 'stop' a program leállításához: ").strip().lower()
        if user_input == "stop":
            print("Program leállítása...")
            stop_kafka_kraft(config)
            running_event.clear()
            break

if __name__ == "__main__":
    try:
        admin_client = AdminClient({'bootstrap.servers': CONFIG['bootstrap_servers']})
        print("Kapcsolódás a Kafka brokerhez sikeres.")
    except Exception as e:
        print(f"Hiba a Kafka brokerhez való kapcsolódáskor: {e}")

    running_event = threading.Event()
    running_event.set()

    update_server_properties(CONFIG)
    check_and_start_kafka_kraft(CONFIG)

    consumer_thread = threading.Thread(target=start_consumer, args=(CONFIG, running_event), daemon=True)
    consumer_thread.start()

    kafka_monitor_thread = threading.Thread(target=monitor_kafka, args=(CONFIG, running_event), daemon=True)
    kafka_monitor_thread.start()

    input_thread = threading.Thread(target=monitor_user_input, args=(running_event, CONFIG), daemon=True)
    input_thread.start()

    
    input_thread.join()

    print("Program leállt.")
