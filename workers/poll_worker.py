# workers/poll_worker.py

import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import paho.mqtt.client as mqtt

from config import (
    MAX_RETRY, MQTT_HOST, MQTT_PASS, MQTT_PORT,
    MQTT_QOS, MQTT_TOPIC_DATA, MQTT_USER, NODES, POLL_INTERVAL,
)
from protocol import build_ack_frame, build_poll_frame, parse_fragment
from lora_transport import LoRaSerial

log = logging.getLogger("worker.poll")

FRAG_RECV_TIMEOUT = 12.0   # giây chờ mỗi fragment
FRAG_RETRY_MAX    = 5     # số lần retry khi mất fragment


class PollWorker:
    def __init__(self, lora: LoRaSerial, stop: threading.Event):
        self._lora   = lora
        self._stop   = stop
        self._mqtt   = self._connect_mqtt()
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
            for node in NODES:
                if self._stop.is_set():
                    break
                self._poll_node(node)
                time.sleep(2.0)
            self._stop.wait(timeout=POLL_INTERVAL)

        self._mqtt.loop_stop()
        self._mqtt.disconnect()

    # ── Poll một node ───────────────────────────────────────────
    def _poll_node(self, node: Dict):
        name = node["name"]
        addh = node["addh"]
        addl = node["addl"]

        # 1. Gửi POLL, chờ fragment đầu tiên (có retry)
        raw = self._send_and_recv(build_poll_frame(addh, addl), name)
        if not raw:
            return

        # 2. Parse fragment đầu tiên
        frag = parse_fragment(raw, addh, addl)
        if frag is None:
            log.warning(f"[{name}] parse fragment 0 thất bại")
            return

        frag_total = frag["frag_total"]
        fragments: List[Optional[bytes]] = [None] * frag_total
        fragments[frag["frag_idx"]] = frag["payload"]
        log.info(f"[{name}] fragment {frag['frag_idx']}/{frag_total - 1} nhận OK")

        # 3. ACK fragment đầu, rồi nhận các fragment còn lại
        expected_idx = 0
        while expected_idx < frag_total:

            # Gửi ACK cho fragment vừa nhận
            ack = build_ack_frame(addh, addl, expected_idx)
            self._lora.send_only(ack)
            log.info(f"[{name}] ACK({expected_idx}) đã gửi")

            # Nếu đây là fragment cuối → xong
            if expected_idx == frag_total - 1:
                break

            # Chờ fragment tiếp theo, có retry
            next_idx = expected_idx + 1
            frag = self._recv_fragment_with_retry(name, addh, addl, next_idx)
            if frag is None:
                return  # log đã in trong _recv_fragment_with_retry

            # Nhận được — lưu payload và tiếp tục
            fragments[frag["frag_idx"]] = frag["payload"]
            log.info(f"[{name}] fragment {frag['frag_idx']}/{frag_total - 1} nhận OK")
            expected_idx = frag["frag_idx"]

        # 4. Kiểm tra đủ fragments
        if any(f is None for f in fragments):
            log.error(f"[{name}] thiếu fragment, huỷ")
            return

        # 5. Ghép và publish
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
        """Gửi POLL, retry MAX_RETRY lần nếu không có phản hồi."""
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
        self,
        name: str,
        addh: int,
        addl: int,
        expected_idx: int,
    ) -> Optional[Dict]:
        attempt = 0
        while attempt < FRAG_RETRY_MAX:

            raw = self._lora.recv_only(timeout=FRAG_RECV_TIMEOUT)

            if not raw:
                attempt += 1
                log.warning(
                    f"[{name}] timeout chờ fragment {expected_idx} "
                    f"(lần {attempt}/{FRAG_RETRY_MAX})"
                )
                log.warning(f"[{name}] in_waiting={self._lora.in_waiting()}")
                continue

            frag = parse_fragment(raw, addh, addl)
            if frag is None:
                attempt += 1
                log.warning(
                    f"[{name}] parse thất bại khi chờ fragment {expected_idx} "
                    f"(lần {attempt}/{FRAG_RETRY_MAX}), thử lại"
                )
                continue

            recv_idx = frag["frag_idx"]

            if recv_idx == expected_idx:
                return frag

            if recv_idx < expected_idx:
                # Node retry fragment cũ do ACK bị mất
                # → Gửi lại ACK, KHÔNG tăng attempt
                log.warning(
                    f"[{name}] nhận fragment cũ idx={recv_idx} "
                    f"(cần {expected_idx}), gửi lại ACK"
                )
                ack = build_ack_frame(addh, addl, recv_idx)
                self._lora.send_only(ack)
                continue  # attempt không đổi

            # recv_idx > expected_idx: lệch thứ tự nghiêm trọng
            log.error(
                f"[{name}] fragment lệch thứ tự: nhận {recv_idx}, cần {expected_idx}"
            )
            return None

        log.error(f"[{name}] mất fragment {expected_idx} sau {FRAG_RETRY_MAX} lần chờ")
        return None
        