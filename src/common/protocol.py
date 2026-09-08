"""
Módulo de Protocolo de Red para HYDRA-DTS
Implementa empaquetado y desempaquetado de mensajes sobre sockets TCP con
delimitación de longitud (Length-Prefix Framing: 4 bytes big-endian) para garantizar
la integridad de las tramas y evitar problemas de fragmentación en la red.
"""

import json
import struct
import socket
from typing import Optional, Dict, Any

# Tipos de Mensajes del Protocolo Distribuido
MSG_REGISTER_WORKER   = "REGISTER_WORKER"
MSG_REGISTER_ACK      = "REGISTER_ACK"
MSG_HEARTBEAT         = "HEARTBEAT"
MSG_HEARTBEAT_ACK     = "HEARTBEAT_ACK"
MSG_SUBMIT_TASK       = "SUBMIT_TASK"
MSG_TASK_ACCEPTED     = "TASK_ACCEPTED"
MSG_DISPATCH_TASK     = "DISPATCH_TASK"
MSG_TASK_RESULT       = "TASK_RESULT"
MSG_GET_TASK_STATUS   = "GET_TASK_STATUS"
MSG_TASK_STATUS_RESP  = "TASK_STATUS_RESP"
MSG_GET_METRICS       = "GET_METRICS"
MSG_METRICS_RESP      = "METRICS_RESP"
MSG_ERROR             = "ERROR"

HEADER_FORMAT = "!I"  # 4 bytes entero sin signo (Network / Big-Endian)
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


def send_message(sock: socket.socket, message: Dict[str, Any]) -> None:
    """
    Serializa un diccionario en JSON y lo transmite con cabecera de longitud de 4 bytes.
    """
    json_bytes = json.dumps(message).encode("utf-8")
    header = struct.pack(HEADER_FORMAT, len(json_bytes))
    sock.sendall(header + json_bytes)


def recv_message(sock: socket.socket, timeout: Optional[float] = None) -> Optional[Dict[str, Any]]:
    """
    Recibe un mensaje completo garantizando la lectura exacta de todos los bytes del payload.
    Retorna None si la conexión remota fue cerrada.
    """
    if timeout is not None:
        sock.settimeout(timeout)

    try:
        header_data = _recv_all(sock, HEADER_SIZE)
        if not header_data:
            return None
        
        payload_length = struct.unpack(HEADER_FORMAT, header_data)[0]
        payload_data = _recv_all(sock, payload_length)
        if not payload_data:
            return None

        return json.loads(payload_data.decode("utf-8"))
    except socket.timeout:
        raise
    except (ConnectionResetError, BrokenPipeError):
        return None
    except Exception:
        return None


def _recv_all(sock: socket.socket, n_bytes: int) -> Optional[bytes]:
    """
    Función auxiliar para leer exactamente n_bytes del flujo TCP sin truncamiento.
    """
    data = bytearray()
    while len(data) < n_bytes:
        packet = sock.recv(n_bytes - len(data))
        if not packet:
            return None
        data.extend(packet)
    return bytes(data)
