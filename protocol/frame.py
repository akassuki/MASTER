# protocol/frame.py

import json
import logging
from typing import Dict, Optional

from config import CMD_ACK, CMD_DATA, CMD_POLL, GW_ADDH, GW_ADDL, LORA_CH

log = logging.getLogger("lora.frame")

FRAG_PAYLOAD_MAX = 52  # 58 - 6 byte header


def _xor(data: bytes) -> int:
    c = 0
    for b in data:
        c ^= b
    return c


def build_poll_frame(addh: int, addl: int) -> bytes:
    """POLL: GW → Node"""
    body  = bytes([CMD_POLL, GW_ADDH, GW_ADDL])
    frame = bytes([addh, addl, LORA_CH]) + body + bytes([_xor(body)])
    return frame


def build_ack_frame(addh: int, addl: int, frag_idx: int) -> bytes:
    """ACK: GW → Node, xác nhận đã nhận fragment frag_idx"""
    body  = bytes([CMD_ACK, GW_ADDH, GW_ADDL, frag_idx])
    frame = bytes([addh, addl, LORA_CH]) + body + bytes([_xor(body)])
    return frame


def parse_fragment(raw: bytes, exp_addh: int, exp_addl: int) -> Optional[Dict]:
    """
    Parse một DATA fragment từ node.

    Frame:
      [CMD_DATA][src_addh][src_addl][frag_idx][frag_total][len][payload...]

    Trả về dict:
      {
        "frag_idx":   int,
        "frag_total": int,
        "payload":    bytes,
      }
    hoặc None nếu lỗi.
    """
    # tối thiểu: 6 byte header + 1 byte payload
    if len(raw) < 7:
        log.warning(f"Fragment quá ngắn ({len(raw)}B)")
        return None

    if raw[0] != CMD_DATA:
        log.warning(f"CMD lạ: 0x{raw[0]:02X}")
        return None

    src_addh, src_addl = raw[1], raw[2]
    if src_addh != exp_addh or src_addl != exp_addl:
        log.warning(f"Nguồn lạ: 0x{src_addh:02X}:0x{src_addl:02X}")
        return None

    frag_idx   = raw[3]
    frag_total = raw[4]
    length     = raw[5]

    if len(raw) < 6 + length:
        log.warning(f"Thiếu dữ liệu: có {len(raw)}B cần {6 + length}B")
        return None

    return {
        "frag_idx":   frag_idx,
        "frag_total": frag_total,
        "payload":    raw[6 : 6 + length],
    }


def build_ota_frame(addh: int, addl: int) -> bytes:
    """CMD_OTA: GW → Node, báo node chuyển sang chế độ nhận OTA."""
    from config import CMD_OTA
    body  = bytes([CMD_OTA, GW_ADDH, GW_ADDL])
    frame = bytes([addh, addl, LORA_CH]) + body
    return frame
