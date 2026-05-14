# config.py

from typing import Dict, List

# ══════════════════════════════════════════════════════════════
# SERIAL / LORA E32
# ══════════════════════════════════════════════════════════════
LORA_PORT    = "/dev/ttyUSB0"
LORA_BAUD    = 9600
LORA_TIMEOUT = 4.0
LORA_CH      = 20

# ══════════════════════════════════════════════════════════════
# SERIAL / LORA E32 — OTA (USB1)
# ══════════════════════════════════════════════════════════════
OTA_LORA_PORT = "/dev/ttyUSB1"
OTA_LORA_BAUD = 115200

# ══════════════════════════════════════════════════════════════
# USB_LORA ADDRESS & ESP32 ADDRESS
# ══════════════════════════════════════════════════════════════
GW_ADDH = 0x00 
GW_ADDL = 0x00

# ══════════════════════════════════════════════════════════════
# COMMAND BYTES
# ══════════════════════════════════════════════════════════════
CMD_POLL        = 0x01
CMD_DATA        = 0x02
CMD_ACK         = 0x03
CMD_OTA         = 0x10
CMD_HEARTBEAT   = 0x20
CMD_ERROR       = 0xFF

# ══════════════════════════════════════════════════════════════
# NODES
# ══════════════════════════════════════════════════════════════
# NODES: List[Dict] = [
#     {"name": "node_1", "addh": 0x00, "addl": 0x02},
#     {"name": "node_2", "addh": 0x00, "addl": 0x03},
# ]
NODES: List[Dict] = [
    {"name": "node_1", "addh": 0x00, "addl": 0x02},
]

# ══════════════════════════════════════════════════════════════
# POLLING
# ══════════════════════════════════════════════════════════════
POLL_INTERVAL = 5.0
MAX_RETRY     = 3

# ══════════════════════════════════════════════════════════════
# MQTT
# ══════════════════════════════════════════════════════════════
MQTT_HOST  = "14.224.150.7"
MQTT_PORT  = 1883
MQTT_USER  = "mqtt"
MQTT_PASS  = "thanhcong"
MQTT_QOS   = 1
 
# topic publish: gateway/data/<node_name>
MQTT_TOPIC_DATA = "gateway/data"

# ══════════════════════════════════════════════════════════════
# SERVER
# ══════════════════════════════════════════════════════════════
OTA_SERVER_URL     = "http://14.224.150.7:5000/version.json" # ← đổi IP:port
OTA_CHECK_INTERVAL = 30.0           # giây giữa 2 lần check
OTA_BINARY_PATH    = "./bin/sender"
OTA_FIRMWARE_DIR   = "/home/viettien/MASTER/ota_cache"  # thư mục lưu firmware tải về
OTA_PUBLIC_KEY     = "./bin/public.pem"
OTA_LOCAL_JSON = "./json/version.json"
