import threading
import time
import sys
import cv2
import board
import busio
import io
import base64
import RPi.GPIO as GPIO
from hx711 import HX711
import subprocess
import statistics
from picamera2 import Picamera2
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

barcode = ""
finalMessage = ""
waiter = False
start_read = True
DIR_PIN_1 = 17    
STEP_PIN_1 = 27     
ENABLE_PIN_1 = 26   
CW = 1           
CCW = 0          
STEPS_PER_REVOLUTION = 3400  


kafka_config_producer = {
    'bootstrap.servers': '172.16.16.196:9092,172.16.16.210:9092',
}
topic = 'Szakdolgozat'  # Kafka tema neve

kafka_config_consumer = {
    'bootstrap.servers': '172.16.16.196:9092,172.16.16.210:9092',
    'group.id': 'RaspConsumer',  # Konsumer csoport azonosito
    'auto.offset.reset': 'latest'  # Az offset beallitas: csak az uj uzeneteket olvassuk
}

stop_flag = threading.Event()


def stepper_motor_rotate(steps, direction, delay, step_pin, dir_pin):
    """
    Rotates the stepper motor a specific number of steps.

    :param steps: Number of steps to move.
    :param direction: Direction of rotation (CW or CCW).
    :param delay: Delay between steps in seconds (controls speed).
    """
    GPIO.output(dir_pin, direction)
    for _ in range(steps):
        GPIO.output(step_pin, GPIO.LOW)
        time.sleep(delay)
        GPIO.output(step_pin, GPIO.HIGH)
        time.sleep(delay)

def handle_message(message):
    """Feldolgozza az uzenetet."""
    msg = message.value().decode("utf-8")
    print(f' {message.value().decode("utf-8")}')
    return msg

def delivery_report(err, msg):
    """Kezbesitesi jelentest kuld a Kafka-ra valo kuldes utan."""
    if err is not None:
        print(f'Failed to deliver message: {msg}')

def capture_and_send_image():
    """ Captures an image using Raspberry Pi Camera Module 3 and sends it via Kafka """
    
    picam2 = Picamera2()
    config = picam2.create_still_configuration(main={"size": (1280, 720)})
    picam2.configure(config)
    picam2.start()
    
    time.sleep(1)

    img_stream = io.BytesIO()
    picam2.capture_file(img_stream, format="jpeg")
    
    img_data = img_stream.getvalue()
    img_base64 = base64.b64encode(img_data).decode('utf-8')

    picam2.stop()  # Ensure camera is stopped
    picam2.close()  # Ensure camera is closed
    del picam2  # Force object deletion to free memory
    
    time.sleep(1)
    
    return img_base64 

def detection(producer):
    TRIG3 = 7
    ECHO3 = 8
    SAMPLE_COUNT = 5
    THRESHOLD_DISTANCE = 100
    while True:
        distances = []
        for _ in range(SAMPLE_COUNT):
            dist = measure_distance(TRIG3, ECHO3)
            if dist is not None:
                distances.append(dist)
            time.sleep(0.05)

        if len(distances) == SAMPLE_COUNT:
            avg_distance = sum(distances) / SAMPLE_COUNT
            if avg_distance <= THRESHOLD_DISTANCE:
                break
        time.sleep(0.1)
    #start_to_middle()
    start_measuring (producer)


def start_to_middle():
    steps = 1700  
    direction = CW
    delay = 0.0001
    #stepper_motor_rotate(steps, direction, delay, STEP_PIN_2, DIR_PIN_2)
    time.sleep(1)

def middle_to_end():
    steps = 1700  
    direction = CW
    delay = 0.0001
    #stepper_motor_rotate(steps, direction, delay, STEP_PIN_3, DIR_PIN_3)
    time.sleep(1)

def end_to_start():
    steps = 3400  
    direction = CCW
    delay = 0.0001
    #stepper_motor_rotate(steps, direction, delay, STEP_PIN_4, DIR_PIN_4)
    time.sleep(1)


def measure_distance(trig_pin, echo_pin):
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(trig_pin, GPIO.OUT)
    GPIO.setup(echo_pin, GPIO.IN)
    GPIO.output(trig_pin, False)
    time.sleep(0.06)
    
    while True:
        start_time = 0
        end_time = 0
        GPIO.output(trig_pin, False)
        time.sleep(0.0001)  
        GPIO.output(trig_pin, True)
        time.sleep(0.00001)  
        GPIO.output(trig_pin, False)

        start1_time = time.time()
        while GPIO.input(echo_pin) == 0:
            start_time = time.time()
                
        while GPIO.input(echo_pin) == 1:
            end_time = time.time()

        elapsed_time = end_time - start_time
        distance = (elapsed_time * 34200) / 2
        if distance > 1 and 45 > distance:
            print(distance)
            return distance
        

def get_average_distance(trig_pin, echo_pin, samples):
    distances = []
    for _ in range(samples):
        distance = measure_distance(trig_pin, echo_pin)
        if distance is not None:
            distances.append(distance)
        time.sleep(0.01)  # Short delay between samples
    distances.sort()
    trimmed = distances[3:-3]  # Remove highest 3 and lowest 3 to reduce noise
    median = statistics.median(trimmed)
    return median
    

def start_measuring (pro_ducer):
    #MESAURE DATA
    distance_2 = 0.0
    distance_1 = 0.0
    weight = 0.0
    
    #PINS
    dt_pin = 22# DOUT pin
    sck_pin = 23  # SCK pin

    TRIG1 = 5
    ECHO1 = 6
    TRIG2 = 20
    ECHO2 = 21
    TRIG3 = 7
    ECHO3 = 8

    GPIO.setmode(GPIO.BCM)
    GPIO.setup(TRIG1, GPIO.OUT)
    GPIO.setup(ECHO1, GPIO.IN)
    GPIO.setup(TRIG2, GPIO.OUT)
    GPIO.setup(ECHO2, GPIO.IN)
    GPIO.setup(TRIG3, GPIO.OUT)
    GPIO.setup(ECHO3, GPIO.IN)

    #CODE

    hx = HX711(dt_pin, sck_pin)
    calibration_factor = 230
    hx.set_reading_format("MSB", "MSB")
    hx.set_reference_unit(calibration_factor)
    hx.set_offset(831184)



    try:
        weight += hx.get_weight(5)
        print(f"Meres Suly: {weight:.2f} g")
        time.sleep(0.1)
        hx.power_down()
        print("Meresek befejezve.")

    except KeyboardInterrupt:
        print("Meres megszakitva.")

    finally:
        print(weight)
        GPIO.cleanup()
        
    try:

        GPIO.setmode(GPIO.BCM)
        GPIO.setup(DIR_PIN_1, GPIO.OUT)

        GPIO.setup(STEP_PIN_1, GPIO.OUT)
        GPIO.setup(ENABLE_PIN_1, GPIO.OUT)
        
        time.sleep(0.1)
        
        GPIO.output(ENABLE_PIN_1, GPIO.HIGH)  

        distance_1 = 44 - get_average_distance(TRIG1, ECHO1, 10)
        
        distance_1 -= get_average_distance(TRIG2, ECHO2, 10)
        GPIO.output(STEP_PIN_1, GPIO.LOW)
        GPIO.output(ENABLE_PIN_1, GPIO.LOW)  # Enable motor driver
        time.sleep(0.05)
        
    
        stepper_motor_rotate(STEPS_PER_REVOLUTION, CW, 0.0001,STEP_PIN_1,DIR_PIN_1)
        
        time.sleep(1)
        
        GPIO.output(ENABLE_PIN_1, GPIO.HIGH)  

        distance_2 = 44 - get_average_distance(TRIG1, ECHO1, 10)

        distance_2 -= get_average_distance(TRIG2, ECHO2, 10)
        GPIO.output(ENABLE_PIN_1, GPIO.LOW)  # Enable motor driver
        stepper_motor_rotate(STEPS_PER_REVOLUTION, CCW, 0.0001,STEP_PIN_1,DIR_PIN_1)
        time.sleep(0.2)
        
        GPIO.output(ENABLE_PIN_1, GPIO.HIGH)  # Enable motor driver
    
    except KeyboardInterrupt:
        print("\nExiting program.")

    image = capture_and_send_image()
    adatok = (f"{weight}g, {distance_1}mm, {distance_2}, {image}")
    print(adatok)

    producer.produce(topic, adatok.encode('utf-8'), callback=delivery_report)
    producer.flush()
    
if __name__ == "__main__":
    
    
    subprocess.Popen([
    'bash',
    '-c',
    'cd /home/balint/Downloads/kafka_2.12-3.8.0 && bin/kafka-server-start.sh config/kraft/server2_rasp.properties'])
    
    time.sleep(25)
    producer = Producer(kafka_config_producer)

    while True:
        # Setup GPIO
        GPIO.setmode(GPIO.BCM)
        GPIO.setup(DIR_PIN_1, GPIO.OUT)

        GPIO.setup(STEP_PIN_1, GPIO.OUT)
        GPIO.setup(ENABLE_PIN_1, GPIO.OUT)
        
        start_read = True
        detection(producer)
        #middle_to_end()
        #end_to_start()
        GPIO.output(ENABLE_PIN_1, GPIO.HIGH)  # Disable motor driver
        time.sleep(2)
