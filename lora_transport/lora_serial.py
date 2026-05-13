# lora_transport/lora_serial.py

import logging
import threading
import time
from typing import Optional

import serial

from config import LORA_BAUD, LORA_PORT, LORA_TIMEOUT, CMD_DATA

log = logging.getLogger("lora.serial")

HEADER_LEN   = 6     # CMD + ADDH + ADDL + frag_idx + frag_total + payload_len
PAYLOAD_MAX  = 52    # byte payload tối đa mỗi fragment


class LoRaSerial:
    def __init__(self):
        self._ser:     Optional[serial.Serial] = None
        self._lock     = threading.Lock()
        self._pending  = b""   # byte đọc thừa chưa xử lý

    # ── Mở / đóng ──────────────────────────────────────────────
    def open(self) -> bool:
        try:
            self._ser = serial.Serial(
                port=LORA_PORT, baudrate=LORA_BAUD,
                timeout=0.1,                   # non-blocking nhỏ, deadline tự quản
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
            )
            log.info(f"Serial OK  {LORA_PORT} @ {LORA_BAUD} baud")
            return True
        except serial.SerialException as e:
            log.error(f"Không mở được serial: {e}")
            return False

    def close(self):
        if self._ser and self._ser.is_open:
            self._ser.close()
            log.info("Serial đã đóng")

    @property
    def is_open(self) -> bool:
        return self._ser is not None and self._ser.is_open

    # ── Public API ─────────────────────────────────────────────
    def send_then_recv(self, frame: bytes) -> Optional[bytes]:
        """Gửi POLL và chờ fragment đầu tiên."""
        try:
            with self._lock:
                self._flush_pending()
                self._ser.reset_input_buffer()
                self._ser.write(frame)
                self._ser.flush()
                log.info(f"TX  {frame.hex(' ').upper()}")
                return self._read_frame(deadline=time.monotonic() + LORA_TIMEOUT)
        except serial.SerialException as e:
            log.error(f"Serial error: {e}")
            return None

    def recv_only(self, timeout: float = 6.0) -> Optional[bytes]:
        """Chờ đọc fragment tiếp theo — không gửi gì."""
        try:
            with self._lock:
                self._flush_pending()
                return self._read_frame(deadline=time.monotonic() + timeout)
        except serial.SerialException as e:
            log.error(f"Serial error: {e}")
            return None

    def send_only(self, frame: bytes) -> bool:
        """Gửi ACK, không đọc phản hồi."""
        try:
            with self._lock:
                self._ser.write(frame)
                self._ser.flush()
                log.info(f"TX  {frame.hex(' ').upper()}")
                time.sleep(0.3)
                return True
        except serial.SerialException as e:
            log.error(f"Serial send error: {e}")
            return False

    # ── Core framing ───────────────────────────────────────────
    def _read_frame(self, deadline: float) -> Optional[bytes]:
        """
        Đọc đúng 1 DATA frame dựa theo payload_len trong header.

        Quy trình:
          1. Đọc HEADER_LEN byte
          2. Kiểm tra CMD byte — nếu không phải CMD_DATA thì flush
             byte đó, đưa phần còn lại vào _pending, thử lại
          3. Đọc thêm payload_len byte
          4. Trả về frame hoàn chỉnh
        """
        while time.monotonic() < deadline:

            # Bước 1: đọc header
            header = self._read_exact(HEADER_LEN, deadline)
            if header is None:
                log.warning("RX  (không có dữ liệu)")
                return None

            # Bước 2: kiểm tra CMD
            if header[0] != CMD_DATA:
                log.debug(f"RX  skip byte rác 0x{header[0]:02X}, tìm lại frame")
                # Đưa 5 byte còn lại trở lại _pending để không bỏ sót
                self._pending = header[1:] + self._pending
                continue

            # Bước 3: kiểm tra payload_len hợp lệ
            payload_len = header[5]
            if payload_len == 0 or payload_len > PAYLOAD_MAX:
                log.warning(f"RX  payload_len bất thường ({payload_len}B), flush pending")
                self._pending = b""
                continue

            # Bước 4: đọc payload
            payload = self._read_exact(payload_len, deadline)
            if payload is None:
                log.warning(f"RX  thiếu payload (cần {payload_len}B), timeout")
                return None

            frame = header + payload
            log.info(f"RX  {frame.hex(' ').upper()}")
            return frame

        log.warning("RX  (không có dữ liệu)")
        return None

    def _read_exact(self, n: int, deadline: float) -> Optional[bytes]:
        """
        Đọc đúng n byte trước deadline.
        Ưu tiên lấy từ _pending trước, sau đó mới đọc serial.
        """
        # Lấy từ _pending trước
        buf = bytearray(self._pending[:n])
        self._pending = self._pending[n:]

        while len(buf) < n:
            if time.monotonic() >= deadline:
                return None
            chunk = self._ser.read(n - len(buf))
            if chunk:
                buf.extend(chunk)

        return bytes(buf)

    def _flush_pending(self):
        """Xoá byte thừa tích lũy trong _pending (gọi trước POLL mới)."""
        if self._pending:
            log.debug(f"Flush {len(self._pending)}B pending")
            self._pending = b""
    def in_waiting(self) -> int:
        try:
            return self._ser.in_waiting
        except Exception:
            return -1