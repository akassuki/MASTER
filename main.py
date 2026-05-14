# main.py

import logging
import os
import queue
import signal
import sys
import threading

from config import (
    GW_ADDH, GW_ADDL, LORA_BAUD, LORA_CH, LORA_PORT, LORA_TIMEOUT,
    MAX_RETRY, MQTT_HOST, MQTT_PORT, NODES,
    OTA_BINARY_PATH, OTA_CHECK_INTERVAL, OTA_LORA_PORT, OTA_SERVER_URL,
    POLL_INTERVAL,
)
from lora_transport import LoRaSerial
from workers import OtaWorker, PollWorker

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler("logs/gateway.log", encoding="utf-8", mode="w"),
    ],
)

ota_logger = logging.getLogger("worker.ota")
ota_logger.propagate = False  # ← không ghi vào gateway.log
ota_logger.setLevel(logging.DEBUG)
ota_logger.addHandler(logging.FileHandler("logs/ota.log", encoding="utf-8", mode="w"))

logging.getLogger("urllib3").propagate = False
log = logging.getLogger("main")


def main():
    log.info("═" * 56)
    log.info("  LoRa Gateway — Raspberry Pi 5")
    log.info("═" * 56)
    log.info(f"  Poll serial : {LORA_PORT} @ {LORA_BAUD} baud  CH=0x{LORA_CH:02X}")
    log.info(f"  OTA serial  : {OTA_LORA_PORT}")
    log.info(f"  Gateway     : 0x{GW_ADDH:02X}:0x{GW_ADDL:02X}")
    log.info(f"  Poll        : interval={POLL_INTERVAL}s  timeout={LORA_TIMEOUT}s  retry={MAX_RETRY}x")
    log.info(f"  MQTT        : {MQTT_HOST}:{MQTT_PORT}")
    log.info(f"  OTA server  : {OTA_SERVER_URL}  check={OTA_CHECK_INTERVAL}s")
    log.info(f"  OTA binary  : {OTA_BINARY_PATH}")
    log.info(f"  Nodes ({len(NODES)}):")
    for n in NODES:
        log.info(f"    {n['name']}  ADDH=0x{n['addh']:02X}  ADDL=0x{n['addl']:02X}")
    log.info("═" * 56)

    # Mở serial poll
    lora = LoRaSerial()
    if not lora.open():
        sys.exit(1)

    # Shared objects giữa poll_worker và ota_worker
    stop           = threading.Event()
    ota_queue      = queue.Queue()
    event_ota_done = threading.Event()

    poll_worker = PollWorker(
        lora=lora,
        stop=stop,
        ota_queue=ota_queue,
        event_ota_done=event_ota_done,
    )
    ota_worker = OtaWorker(
        stop=stop,
        ota_queue=ota_queue,
        event_ota_done=event_ota_done,
    )

    def _sig(sig, _):
        log.info("Signal received, shutting down...")
        stop.set()

    signal.signal(signal.SIGINT,  _sig)
    signal.signal(signal.SIGTERM, _sig)

    poll_worker.start()
    ota_worker.start()
    log.info("Tất cả worker đã khởi động")

    poll_worker.join()
    ota_worker.join()
    lora.close()
    log.info("Shutdown complete.")


if __name__ == "__main__":
    main()
