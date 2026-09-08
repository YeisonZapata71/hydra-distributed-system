"""
Patrón de Diseño: Remote Proxy (Proxy Remoto).
Oculta la complejidad de la comunicación por red (sockets TCP, serialización JSON,
reintentos y control de excepciones) exponiendo una interfaz transparente para el cliente.
Referencia: Buschmann et al. (1996), Coulouris et al. (2012).
"""

import socket
import time
import uuid
import os
import sys
from typing import Dict, Any, Optional

try:
    from ..common.protocol import (
        send_message, recv_message,
        MSG_SUBMIT_TASK, MSG_TASK_ACCEPTED,
        MSG_GET_TASK_STATUS, MSG_TASK_STATUS_RESP,
        MSG_GET_METRICS, MSG_METRICS_RESP
    )
    from ..common.models import Task, TaskPriority
except (ImportError, ValueError):
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
    from src.common.protocol import (
        send_message, recv_message,
        MSG_SUBMIT_TASK, MSG_TASK_ACCEPTED,
        MSG_GET_TASK_STATUS, MSG_TASK_STATUS_RESP,
        MSG_GET_METRICS, MSG_METRICS_RESP
    )
    from src.common.models import Task, TaskPriority


class HydraClientProxy:
    """
    Proxy Remoto que permite a los clientes consumir los servicios de procesamiento
    distribuido sin acoplarse a la topología física de la red ni a los sockets.
    """
    def __init__(self, coordinator_host: str = "127.0.0.1", coordinator_port: int = 5000, timeout: float = 5.0):
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
        self.timeout = timeout

    def _open_connection(self) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        try:
            sock.connect((self.coordinator_host, self.coordinator_port))
            return sock
        except ConnectionRefusedError:
            raise ConnectionRefusedError(
                f"No se pudo conectar con el Coordinador en {self.coordinator_host}:{self.coordinator_port}. "
                "El servidor no esta activo o no esta escuchando conexiones.\n"
                "Asegurate de iniciar primero el cluster en otra terminal con: python run_cluster.py"
            )

    def submit_task(self, task_type: str, payload: Dict[str, Any], priority: str = TaskPriority.NORMAL.value) -> str:
        """
        Envía una nueva tarea al cluster distribuido y retorna su identificador único (UUID).
        """
        task_id = f"task-{uuid.uuid4().hex[:8]}"
        task = Task(
            task_id=task_id,
            task_type=task_type,
            payload=payload,
            priority=priority,
            created_at=time.time()
        )

        with self._open_connection() as sock:
            send_message(sock, {
                "type": MSG_SUBMIT_TASK,
                "task": task.to_dict()
            })
            resp = recv_message(sock, timeout=self.timeout)
            if not resp or resp.get("type") != MSG_TASK_ACCEPTED:
                raise RuntimeError(f"Fallo al someter tarea al cluster: {resp}")
            return task_id

    def get_task_status(self, task_id: str) -> Optional[Task]:
        """
        Consulta el estado actual de una tarea en el cluster.
        """
        with self._open_connection() as sock:
            send_message(sock, {
                "type": MSG_GET_TASK_STATUS,
                "task_id": task_id
            })
            resp = recv_message(sock, timeout=self.timeout)
            if resp and resp.get("type") == MSG_TASK_STATUS_RESP:
                return Task.from_dict(resp["task"])
            return None

    def wait_for_completion(self, task_id: str, timeout_sec: float = 15.0, poll_interval: float = 0.05) -> Task:
        """
        Espera activamente a que la tarea sea completada por alguno de los nodos del cluster.
        """
        start = time.time()
        while (time.time() - start) < timeout_sec:
            task = self.get_task_status(task_id)
            if task and task.status in ("COMPLETED", "FAILED"):
                return task
            time.sleep(poll_interval)
        raise TimeoutError(f"La tarea {task_id} superó el tiempo máximo de espera ({timeout_sec}s)")

    def get_cluster_metrics(self) -> Dict[str, Any]:
        """
        Obtiene las métricas consolidadas de salud, nodos activos y tasa de despacho del cluster.
        """
        with self._open_connection() as sock:
            send_message(sock, {"type": MSG_GET_METRICS})
            resp = recv_message(sock, timeout=self.timeout)
            if resp and resp.get("type") == MSG_METRICS_RESP:
                return resp
            return {}
