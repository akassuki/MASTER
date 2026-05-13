# test_mqtt.py

import paho.mqtt.client as mqtt

MQTT_HOST  = "14.224.150.7"
MQTT_PORT  = 1883
MQTT_USER  = "mqtt"
MQTT_PASS  = "thanhcong"
MQTT_TOPIC = "gateway/data/node_01"

def on_connect(c, u, f, rc, prop=None):
    if rc == 0:
        print(f"[OK] Kết nối thành công")
        r = c.publish(MQTT_TOPIC, "test_payload_123", qos=1)
        print(f"[OK] Publish  topic={MQTT_TOPIC}  rc={r.rc}")
    else:
        print(f"[FAIL] Kết nối thất bại rc={rc}")

def on_publish(c, u, mid, rc=None, prop=None):
    print(f"[OK] Server xác nhận nhận  mid={mid}")
    c.disconnect()

c = mqtt.Client(client_id="test_mqtt")
c.username_pw_set(MQTT_USER, MQTT_PASS)
c.on_connect = on_connect
c.on_publish  = on_publish

print(f"Đang kết nối {MQTT_HOST}:{MQTT_PORT}...")
c.connect(MQTT_HOST, MQTT_PORT, keepalive=10)
c.loop_forever()
