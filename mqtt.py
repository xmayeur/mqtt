import json
import logging
import sys
from logging.handlers import RotatingFileHandler
from time import sleep

import oyaml
import paho.mqtt.client as mqtt
import redis

project = "mqtt"
LOG_file = project + ".log"
INI_file = project + ".conf" if len(sys.argv) <= 1 else project + ".conf.local"
connect_flag = False


def open_log(name):
    # Set up the log handlers to stdout and file.
    log_ = logging.getLogger(name)
    log_.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    handler_stdout = logging.StreamHandler(sys.stdout)
    handler_stdout.setLevel(logging.DEBUG)
    handler_stdout.setFormatter(formatter)
    log_.addHandler(handler_stdout)
    handler_file = RotatingFileHandler(
        LOG_file, mode="a", maxBytes=200000, backupCount=9, encoding="UTF-8", delay=True
    )
    handler_file.setLevel(logging.DEBUG)
    handler_file.setFormatter(formatter)
    log_.addHandler(handler_file)
    return log_


log = open_log(project)

# Open config file
try:
    config = oyaml.load(open(INI_file, "r"), Loader=oyaml.Loader)
except IOError:
    log.critical("configuration file is missing")
    config = None
    exit(-1)


def get_vault(uid):
    global config
    host = config["redis"]["host"]
    port = config["redis"]["port"]
    vaultdb = config["redis"]["vaultdb"]
    vault = redis.Redis(host=host, port=port, db=vaultdb)
    _s = vault.get(uid)
    _id = json.loads(_s)
    if id:
        _username = _id["username"]
        _password = _id["password"]
    else:
        _username = ""
        _password = ""
    return _username, _password


verbose = config["mqtt"]["verbose"]


def do_mqtt_connect(client, host, port):
    global connect_flag
    try:
        client.connect(host, port=port)
        while not client.connected_flag:
            print("+", end="")
            sys.stdout.flush()
            sleep(1)
    # except mqtt.MQTT_ERR_ACL_DENIED:
    #     print('Invalid username or password')
    #     log.critical('Invalid username or password')

    except Exception as e:
        print("Cannot connect to mqtt broker -  error: " + str(e))
        log.critical("Cannot connect to mqtt broker - retrying")
        client.connected_flag = False


def do_publish(client, topic, key, value, qos=0, retain=False):
    try:
        if isinstance(value, dict):
            value = json.dumps(value)
        else:
            value = str(value)
        client.publish(topic + "/" + key, value, qos=qos, retain=retain)
        return True
    except Exception as e:
        print("Cannot publish to mqtt broker -  error: " + str(e))
        log.critical("Cannot publish to mqtt broker - retrying")
        return False


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        if verbose:
            print(f"connected ok - data: {userdata}  - flags: {flags}")
        client.connected_flag = True
        # subscribe after connection OK
        do_subscribe(client)
    else:
        log.info("mqtt on_connect return code is: " + str(rc))


def do_subscribe(client):
    # subscribe to any topic setting a value to a device
    # client.subscribe(project + '/+/setValue')
    pass


block = False


def on_message(client, userdata, message):
    global verbose, block
    if block:
        return
    block = True
    topic = message.topic
    msg = str(message.payload.decode("utf-8"))
    print("Received message: " + topic + "/" + msg + f"\n userdata {userdata}")
    sys.stdout.flush()
    if topic == project + "/getStatus":
        # do_mqtt_publish(client, project + "/status", "alive", qos=0, retain=False)
        pass
    elif "verbose" in topic:
        if msg == "1":
            verbose = True
        else:
            verbose = False
    client.on_message = on_message


def ha_config(client):
    topic = "homeassistant/sensor/m5stick"
    payload = [
        {
            "device_class": "temperature",
            "name": "m5stick_temp",
            "state_topic": "homeassistant/sensor/m5stick/state",
            "unit_of_measurement": "°C",
            "value_template": "{{ value_json.temperature}}",
            "unique_id": "temp01ae",
            "device": {"identifiers": "m5stick01ae", "name": "m5stick"},
        },
        {
            "device_class": "humidity",
            "name": "m5stick_hum",
            "state_topic": "homeassistant/sensor/m5stick/state",
            "unit_of_measurement": "%",
            "value_template": "{{ value_json.humidity}}",
            "unique_id": "hum01ae",
            "device": {"identifiers": "m5stick01ae", "name": "m5stick"},
        },
        {
            "device_class": "pressure",
            "name": "m5stick_pres",
            "state_topic": "homeassistant/sensor/m5stick/state",
            "unit_of_measurement": "hPa",
            "value_template": "{{ value_json.pressure}}",
            "unique_id": "press01ae",
            "device": {"identifiers": "m5stick01ae", "name": "m5stick"},
        },
    ]

    r = do_publish(client, topic + "T", "config", json.dumps(payload[0]))
    sleep(2)
    r = do_publish(client, topic + "H", "config", json.dumps(payload[1]))
    sleep(2)
    r = do_publish(client, topic + "P", "config", json.dumps(payload[2]))
    sleep(2)
    # r = do_publish(client, topic, "config", json.dumps(payload))
    print(r)
    # do_publish(client, topic, "config", "")


def main():
    # Connect to mqtt bus
    uid = config["mqtt"]["uid"]
    host = config["mqtt"]["host"]
    port = config["mqtt"]["port"]

    uname, pwd = get_vault(uid)
    client = mqtt.Client(project)
    client.connected_flag = False
    client.username_pw_set(username=uname, password=pwd)
    client.on_connect = on_connect
    client.on_message = on_message
    duration = int(config["mqtt"]["duration"])
    print("Duration: " + str(duration))
    client.loop_start()
    do_mqtt_connect(client, host, port)
    ha_config(client)

    while True:
        client.on_message = on_message
        # Get device list and state
        topic = "homeassistant/sensor/m5stick"
        key = "state"
        payload = {"temperature": 20.0, "humidity": 50.0}
        r = do_publish(client, topic, key, payload)
        if not r:
            print("reconnecting")
            do_mqtt_connect(client, host, port)
            r = do_publish(client, topic, key, payload)

        print(".", end="")
        sys.stdout.flush()
        if duration == 0:
            sys.exit(0)
        else:
            sleep(duration)


if __name__ == "__main__":
    main()
