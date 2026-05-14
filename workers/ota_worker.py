# workers/ota_worker.py

import hashlib
import logging
import os
import subprocess
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional
import json

import requests

from config import (
    OTA_BINARY_PATH, OTA_CHECK_INTERVAL, OTA_FIRMWARE_DIR,
    OTA_LORA_PORT, OTA_PUBLIC_KEY, OTA_SERVER_URL,OTA_LOCAL_JSON,OTA_LORA_BAUD,
)

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger("worker.ota")


@dataclass
class OtaRequest:
    node_name: str
    addh:      int
    addl:      int
    firmware:  str   
    version:   str


class OtaWorker:
    """
    Quét server OTA định kỳ.
    Khi có firmware mới cho node nào:
      1. Download firmware.bin + firmware.sig
      2. Verify signature bằng public.pem
      3. Gửi OtaRequest vào ota_queue để poll_worker xử lý
      4. Chờ event_ota_done
      5. Chạy binary OTA tool qua USB1
      6. Thêm node lại vào polling
    """

    def __init__(
        self,
        stop:          threading.Event,
        ota_queue:     "queue.Queue[OtaRequest]",
        event_ota_done: threading.Event,
    ):
        self._stop           = stop
        self._ota_queue      = ota_queue
        self._event_ota_done = event_ota_done
        self._versions: Dict[str, str] = self._load_local()
        self._thread = threading.Thread(
            target=self._run, name="OtaWorker", daemon=True)

        os.makedirs(OTA_FIRMWARE_DIR, exist_ok=True)

    def start(self): self._thread.start()
    def join(self):  self._thread.join()

    # ── Vòng lặp chính ─────────────────────────────────────────
    def _run(self):
        log.info("OtaWorker bắt đầu")
        while not self._stop.is_set():
            self._check_server()
            self._stop.wait(timeout=OTA_CHECK_INTERVAL)
        log.info("OtaWorker dừng")

    # ── Quét server ────────────────────────────────────────────
    def _check_server(self):
        log.debug(f"OTA check {OTA_SERVER_URL}")
        try:
            resp = requests.get(OTA_SERVER_URL, timeout=10, verify=False)
            resp.raise_for_status()
            log.debug(f"OTA raw response: {resp.text[:200]}")
            data = resp.json()
        except Exception as e:
            log.warning(f"OTA server error: {e}")
            return

        # Hỗ trợ cả dict (1 node) lẫn list (nhiều node)
        if isinstance(data, dict):
            entries = [data]
        elif isinstance(data, list):
            entries = data
        else:
            log.error(f"OTA response format không hợp lệ: {type(data)}")
            return

        for entry in entries:
            if not isinstance(entry, dict):
                log.warning(f"OTA entry không phải dict: {entry}")
                continue
            self._process_entry(entry)

    def _load_local(self) -> Dict[str, str]:
        """Trả về dict node_name → version từ file local."""
        try:
            with open(OTA_LOCAL_JSON, "r", encoding="utf-8") as f:
                entries = json.load(f)
            return {e["node"]: e["version"] for e in entries}
        except FileNotFoundError:
            log.warning(f"Local JSON chưa có: {OTA_LOCAL_JSON}, sẽ tạo sau khi OTA")
            return {}
        except Exception as e:
            log.warning(f"Đọc local JSON lỗi: {e}")
            return {}

    def _save_local(self, node_name: str, new_entry: Dict):
        """Cập nhật entry của node trong local JSON cho giống server."""
        try:
            with open(OTA_LOCAL_JSON, "r", encoding="utf-8") as f:
                entries = json.load(f)
        except Exception:
            entries = []

        # Tìm và replace entry của node, nếu chưa có thì append
        for i, e in enumerate(entries):
            if e.get("node") == node_name:
                entries[i] = new_entry
                break
        else:
            entries.append(new_entry)

        os.makedirs(os.path.dirname(OTA_LOCAL_JSON), exist_ok=True)
        with open(OTA_LOCAL_JSON, "w", encoding="utf-8") as f:
            json.dump(entries, f, indent=4, ensure_ascii=False)
        log.info(f"[{node_name}] Local JSON cập nhật → version={new_entry.get('version')}")

    def _process_entry(self, entry: Dict):
        node_name = entry.get("node", "")
        version   = entry.get("version", "")
        firmware  = entry.get("firmware", "")
        signature = entry.get("signature", "")
        address   = entry.get("address", "")   # "0x00:0x02"

        if not all([node_name, version, firmware, signature, address]):
            log.warning(f"OTA entry thiếu trường: {entry}")
            return

        # Kiểm tra version mới
        current = self._versions.get(node_name, "")
        if current == version:
            log.debug(f"[{node_name}] OTA up-to-date ({version})")
            return

        log.info(f"[{node_name}] OTA mới: {current or 'N/A'} → {version}")

        # Parse địa chỉ node "0x00:0x02"
        try:
            parts = address.split(":")
            addh  = int(parts[0], 16)
            addl  = int(parts[1], 16)
        except Exception:
            log.error(f"[{node_name}] địa chỉ sai format: {address}")
            return

        # Download firmware
        fw_path  = self._download(node_name, firmware)
        sig_path = self._download(node_name, signature)
        if not fw_path or not sig_path:
            return

        # Verify signature
        if not self._verify_signature(node_name, fw_path, sig_path):
            return

        # Gửi request vào queue cho poll_worker
        req = OtaRequest(
            node_name=node_name,
            addh=addh,
            addl=addl,
            firmware=fw_path,
            version=version,
        )
        self._ota_queue.put(req)
        log.info(f"[{node_name}] OTA request đã đưa vào queue")

        # Chờ poll_worker báo node đã nhận CMD_OTA
        self._event_ota_done.clear()
        log.info(f"[{node_name}] Chờ poll_worker gửi CMD_OTA...")
        self._event_ota_done.wait()
        log.info(f"[{node_name}] CMD_OTA xác nhận, bắt đầu flash firmware")

        # Chạy binary OTA
        success = self._run_ota_binary(node_name, addl, fw_path)
        if success:
            self._versions[node_name] = version
            self._save_local(node_name, entry)
            log.info(f"[{node_name}] OTA hoàn thành version={version}")
        else:
            log.error(f"[{node_name}] OTA thất bại")


    # ── Download firmware ───────────────────────────────────────
    def _download(self, node_name: str, filename: str) -> Optional[str]:
        # Nếu đã là URL đầy đủ thì dùng thẳng, không ghép base_url
        if filename.startswith("http"):
            url = filename
        else:
            base_url = OTA_SERVER_URL.rsplit("/", 1)[0]
            url = f"{base_url}/{filename}"

        # Lấy tên file từ URL để lưu local
        local_filename = filename.rsplit("/", 1)[-1]
        path = os.path.join(OTA_FIRMWARE_DIR, f"{node_name}_{local_filename}")
        
        try:
            resp = requests.get(url, timeout=30, verify=False)
            resp.raise_for_status()
            with open(path, "wb") as f:
                f.write(resp.content)
            log.info(f"[{node_name}] Download OK: {local_filename} ({len(resp.content)}B)")
            return path
        except Exception as e:
            log.error(f"[{node_name}] Download thất bại {url}: {e}")
            return None

    # ── Verify signature ────────────────────────────────────────
    def _verify_signature(
        self, node_name: str, fw_path: str, sig_path: str
    ) -> bool:
        try:
            result = subprocess.run(
                [
                    "openssl", "dgst", "-sha256",
                    "-verify", OTA_PUBLIC_KEY,
                    "-signature", sig_path,
                    fw_path,
                ],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                log.info(f"[{node_name}] Signature OK")
                return True
            else:
                log.error(f"[{node_name}] Signature FAIL: {result.stderr.strip()}")
                return False
        except Exception as e:
            log.error(f"[{node_name}] Verify error: {e}")
            return False

    # ── Chạy binary OTA ────────────────────────────────────────
    def _run_ota_binary(
        self, node_name: str, addl: int, fw_path: str
    ) -> bool:
        cmd = [
            OTA_BINARY_PATH,
            "--port",     OTA_LORA_PORT,
            "--file",     fw_path,
            "--baud",     str(OTA_LORA_BAUD),
        ]
        log.info(f"[{node_name}] OTA cmd: {' '.join(cmd)}")
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=None,
            )
            if result.returncode == 0:
                if result.stdout:
                    log.info(f"[{node_name}] OTA stdout:\n{result.stdout.strip()}")
                return True
            else:
                log.error(f"[{node_name}] OTA returncode={result.returncode}")
                if result.stderr:
                    log.error(f"[{node_name}] OTA stderr:\n{result.stderr.strip()}")
                return False
        # except subprocess.TimeoutExpired:
        #     log.error(f"[{node_name}] OTA timeout (>300s)")
        #     return False
        except FileNotFoundError:
            log.error(f"Binary không tìm thấy: {OTA_BINARY_PATH}")
            return False
        except Exception as e:
            log.error(f"[{node_name}] OTA exception: {e}")
            return False
        