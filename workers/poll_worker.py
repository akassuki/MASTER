# workers/poll_worker.py

import json
import logging
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

import paho.mqtt.client as mqtt

from config import (
    CMD_OTA, MAX_RETRY, MQTT_HOST, MQTT_PASS, MQTT_PORT,
    MQTT_QOS, MQTT_TOPIC_DATA, MQTT_USER, NODES, POLL_INTERVAL,
)
from protocol import build_ack_frame, build_ota_frame, build_poll_frame, parse_fragment
from lora_transport import LoRaSerial

log = logging.getLogger("worker.poll")

FRAG_RECV_TIMEOUT = 12.0
FRAG_RETRY_MAX    = 5


class PollWorker:
    def __init__(
        self,
        lora:            LoRaSerial,
        stop:            threading.Event,
        ota_queue:       "queue.Queue",
        event_ota_done:  threading.Event,
    ):
        self._lora           = lora
        self._stop           = stop
        self._ota_queue      = ota_queue
        self._event_ota_done = event_ota_done
        self._skip_nodes:    Set[str] = set()   # node đang OTA
        self._mqtt           = self._connect_mqtt()
        self._thread = threading.Thread(
            target=self._run, name="PollWorker", daemon=True)

    def start(self): self._thread.start()
    def join(self):  self._thread.join()

    # ── MQTT ───────────────────────────────────────────────────
    def _connect_mqtt(self) -> mqtt.Client:
        client = mqtt.Client(client_id="poll_worker")
        client.username_pw_set(MQTT_USER, MQTT_PASS)
        client.on_connect    = lambda c, u, f, rc, *_: log.info(f"MQTT connected rc={rc}")
        client.on_disconnect = lambda c, u, rc, *_:    log.warning(f"MQTT disconnected rc={rc}")
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_start()
        except Exception as e:
            log.error(f"MQTT connect failed: {e}")
        return client

    def _publish(self, node_name: str, data: Dict):
        topic   = f"{MQTT_TOPIC_DATA}/{node_name}"
        payload = json.dumps({
            "node": node_name,
            "ts":   datetime.now(timezone.utc).isoformat(),
            **data,
        })
        result = self._mqtt.publish(topic, payload, qos=MQTT_QOS)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            log.info(f"MQTT OK  {topic}  {payload}")
        else:
            log.warning(f"MQTT publish fail rc={result.rc}")

    # ── Vòng lặp chính ─────────────────────────────────────────
    def _run(self):
        while not self._stop.is_set():
            # Kiểm tra OTA request trước mỗi vòng poll
            self._check_ota_queue()

            for node in NODES:
                if self._stop.is_set():
                    break
                if node["name"] in self._skip_nodes:
                    log.debug(f"[{node['name']}] đang OTA, bỏ qua")
                    continue
                self._poll_node(node)
                time.sleep(2.0)

            self._stop.wait(timeout=POLL_INTERVAL)

        self._mqtt.loop_stop()
        self._mqtt.disconnect()

    # ── Xử lý OTA request ──────────────────────────────────────
    def _check_ota_queue(self):
        try:
            req = self._ota_queue.get_nowait()
        except queue.Empty:
            return

        name = req.node_name
        addh = req.addh
        addl = req.addl
        log.info(f"[{name}] OTA request nhận được, gửi CMD_OTA")

        # Xóa node khỏi polling
        self._skip_nodes.add(name)

        # Gửi CMD_OTA xuống node
        frame = build_ota_frame(addh, addl)
        ok    = self._lora.send_only(frame)
        if ok:
            log.info(f"[{name}] CMD_OTA đã gửi")
        else:
            log.error(f"[{name}] CMD_OTA gửi thất bại")
            self._skip_nodes.discard(name)
            return

        # Báo ota_worker có thể bắt đầu flash
        self._event_ota_done.set()
        log.info(f"[{name}] event_ota_done set, ota_worker bắt đầu flash")

        # Chờ OTA xong rồi thêm node lại (ota_worker sẽ clear event khi xong)
        # Poll worker không block ở đây — ota_worker tự quản lý
        # Node sẽ tự reboot và quay lại polling sau khi flash xong
        threading.Thread(
            target=self._wait_ota_complete,
            args=(name,),
            daemon=True,
        ).start()

    def _wait_ota_complete(self, node_name: str):
        """Chờ OTA xong rồi thêm node lại vào polling."""
        log.info(f"[{node_name}] Chờ OTA hoàn thành...")
        # Chờ event_ota_done được clear (ota_worker clear sau khi flash xong)
        while self._event_ota_done.is_set():
            time.sleep(1.0)
        self._skip_nodes.discard(node_name)
        log.info(f"[{node_name}] OTA xong, thêm lại vào polling")

    # ── Poll một node ───────────────────────────────────────────
    def _poll_node(self, node: Dict):
        name = node["name"]
        addh = node["addh"]
        addl = node["addl"]

        raw = self._send_and_recv(build_poll_frame(addh, addl), name)
        if not raw:
            return

        frag = parse_fragment(raw, addh, addl)
        if frag is None:
            log.warning(f"[{name}] parse fragment 0 thất bại")
            return

        frag_total = frag["frag_total"]
        fragments: List[Optional[bytes]] = [None] * frag_total
        fragments[frag["frag_idx"]] = frag["payload"]
        log.info(f"[{name}] fragment {frag['frag_idx']}/{frag_total - 1} nhận OK")

        expected_idx = 0
        while expected_idx < frag_total:
            ack = build_ack_frame(addh, addl, expected_idx)
            self._lora.send_only(ack)
            log.info(f"[{name}] ACK({expected_idx}) đã gửi")

            if expected_idx == frag_total - 1:
                break

            next_frag = self._recv_fragment_with_retry(name, addh, addl, expected_idx + 1)
            if next_frag is None:
                return

            fragments[next_frag["frag_idx"]] = next_frag["payload"]
            log.info(f"[{name}] fragment {next_frag['frag_idx']}/{frag_total - 1} nhận OK")
            expected_idx = next_frag["frag_idx"]

        if any(f is None for f in fragments):
            log.error(f"[{name}] thiếu fragment, huỷ")
            return

        full_payload = b"".join(fragments)
        log.info(f"[{name}] ghép {frag_total} fragments ({len(full_payload)}B)")

        try:
            data = json.loads(full_payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            log.error(f"[{name}] JSON lỗi: {e}")
            return

        self._publish(name, data)

    # ── Helpers ────────────────────────────────────────────────
    def _send_and_recv(self, frame: bytes, name: str) -> Optional[bytes]:
        for attempt in range(1, MAX_RETRY + 1):
            raw = self._lora.send_then_recv(frame)
            if raw:
                return raw
            log.warning(f"[{name}] thử {attempt}/{MAX_RETRY} không có dữ liệu")
            if attempt < MAX_RETRY:
                time.sleep(0.3)
        log.warning(f"[{name}] không phản hồi sau {MAX_RETRY} lần thử")
        return None

    def _recv_fragment_with_retry(
        self, name: str, addh: int, addl: int, expected_idx: int,
    ) -> Optional[Dict]:
        attempt = 0
        while attempt < FRAG_RETRY_MAX:
            raw = self._lora.recv_only(timeout=FRAG_RECV_TIMEOUT)

            if not raw:
                attempt += 1
                log.warning(f"[{name}] timeout chờ fragment {expected_idx} (lần {attempt}/{FRAG_RETRY_MAX})")
                log.warning(f"[{name}] in_waiting={self._lora.in_waiting()}")
                continue

            frag = parse_fragment(raw, addh, addl)
            if frag is None:
                attempt += 1
                log.warning(f"[{name}] parse thất bại fragment {expected_idx} (lần {attempt}/{FRAG_RETRY_MAX})")
                continue

            recv_idx = frag["frag_idx"]
            if recv_idx == expected_idx:
                return frag

            if recv_idx < expected_idx:
                log.warning(f"[{name}] fragment cũ idx={recv_idx}, gửi lại ACK")
                self._lora.send_only(build_ack_frame(addh, addl, recv_idx))
                continue

            log.error(f"[{name}] fragment lệch thứ tự: nhận {recv_idx}, cần {expected_idx}")
            return None

        log.error(f"[{name}] mất fragment {expected_idx} sau {FRAG_RETRY_MAX} lần chờ")
        return None
    