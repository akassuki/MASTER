# main.py

import logging
import os
import signal
import sys
import threading

from config import GW_ADDH, GW_ADDL, LORA_BAUD, LORA_CH, LORA_PORT, LORA_TIMEOUT, MAX_RETRY, NODES, POLL_INTERVAL
from lora_transport import LoRaSerial
from workers import PollWorker

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        # logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/gateway.log", encoding="utf-8",mode="w"),
    ],
)
log = logging.getLogger("main")


def main():
    log.info("═" * 52)
    log.info("  LoRa Gateway — Poll Worker debug")
    log.info("═" * 52)
    log.info(f"  Serial   : {LORA_PORT} @ {LORA_BAUD} baud  CH=0x{LORA_CH:02X}")
    log.info(f"  Gateway  : 0x{GW_ADDH:02X}:0x{GW_ADDL:02X}")
    log.info(f"  Poll     : interval={POLL_INTERVAL}s  timeout={LORA_TIMEOUT}s  retry={MAX_RETRY}x")
    log.info(f"  Nodes ({len(NODES)}):")
    for n in NODES:
        log.info(f"    {n['name']}  →  ADDH=0x{n['addh']:02X}  ADDL=0x{n['addl']:02X}")
    log.info("═" * 52)

    lora = LoRaSerial()
    if not lora.open():
        sys.exit(1)

    stop   = threading.Event()
    worker = PollWorker(lora=lora, stop=stop)

    def _sig(sig, _):
        log.info("Signal received, shutting down...")
        stop.set()

    signal.signal(signal.SIGINT,  _sig)
    signal.signal(signal.SIGTERM, _sig)

    worker.start()
    worker.join()
    lora.close()
    log.info("Shutdown complete.")


if __name__ == "__main__":
    main()

