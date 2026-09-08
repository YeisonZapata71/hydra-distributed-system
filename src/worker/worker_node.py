"""
Nodo de Procesamiento Distribuido (Worker Node).
Se ejecuta como un proceso independiente en un puerto de red TCP.
Registra su capacidad ante el Coordinador, emite latidos periódicos (Heartbeats)
y gestiona un pool de hilos concurrentes para el procesamiento de tareas.
"""

import socket
import threading
import time
import argparse
import os
import sys
from typing import Optional

try:
    from ..common.protocol import (
        send_message, recv_message,
        MSG_REGISTER_WORKER, MSG_REGISTER_ACK,
        MSG_HEARTBEAT, MSG_HEARTBEAT_ACK,
        MSG_DISPATCH_TASK, MSG_TASK_ACCEPTED,
        MSG_TASK_RESULT, MSG_ERROR
    )
    from ..common.models import Task
    from .thread_pool import WorkerThreadPool
except (ImportError, ValueError):
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
    from src.common.protocol import (
        send_message, recv_message,
        MSG_REGISTER_WORKER, MSG_REGISTER_ACK,
        MSG_HEARTBEAT, MSG_HEARTBEAT_ACK,
        MSG_DISPATCH_TASK, MSG_TASK_ACCEPTED,
        MSG_TASK_RESULT, MSG_ERROR
    )
    from src.common.models import Task
    from src.worker.thread_pool import WorkerThreadPool


class WorkerNode:
    def __init__(self, worker_id: str, host: str, port: int, coordinator_host: str, coordinator_port: int, pool_size: int = 4):
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
        self.pool_size = pool_size

        self.thread_pool = WorkerThreadPool(
            worker_id=self.worker_id,
            pool_size=self.pool_size,
            on_task_complete=self._on_task_finished
        )

        self.is_running = threading.Event()
        self.is_running.set()
        self.server_socket: Optional[socket.socket] = None

    def start(self) -> None:
        """Inicia el servidor de red del Worker y sus hilos auxiliares."""
        print(f"[{self.worker_id}] Iniciando Nodo Worker en {self.host}:{self.port} con {self.pool_size} hilos...")
        
        # 1. Iniciar socket servidor para recibir tareas del coordinador
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(15)

        # 2. Registrarse ante el Coordinador
        if not self._register_with_coordinator():
            print(f"[{self.worker_id}] ⚠️ Advertencia: No se pudo registrar inmediatamente. Reintentando en segundo plano...")

        # 3. Iniciar Hilo de Latidos (Heartbeat Thread)
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"{self.worker_id}-HeartbeatThread",
            daemon=True
        )
        heartbeat_thread.start()

        # 4. Iniciar Hilo Receptor de Tareas (Task Listener Thread)
        listener_thread = threading.Thread(
            target=self._listen_for_tasks_loop,
            name=f"{self.worker_id}-ListenerThread",
            daemon=True
        )
        listener_thread.start()

        print(f"[{self.worker_id}] ✅ Nodo Worker listo y operativo en la red.")

    def _register_with_coordinator(self) -> bool:
        """Envía mensaje de registro al Coordinador vía socket TCP."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(3.0)
                sock.connect((self.coordinator_host, self.coordinator_port))
                req = {
                    "type": MSG_REGISTER_WORKER,
                    "worker_id": self.worker_id,
                    "host": self.host,
                    "port": self.port,
                    "max_threads": self.pool_size
                }
                send_message(sock, req)
                resp = recv_message(sock, timeout=3.0)
                if resp and resp.get("type") == MSG_REGISTER_ACK:
                    print(f"[{self.worker_id}] Registrado exitosamente en Coordinador.")
                    return True
        except Exception as e:
            # Coordinador aún no disponible o fuera de línea
            return False
        return False

    def _heartbeat_loop(self) -> None:
        """Envía periódicamente latidos y métricas de carga al Coordinador."""
        while self.is_running.is_set():
            time.sleep(1.5)
            metrics = self.thread_pool.get_metrics()
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(2.0)
                    sock.connect((self.coordinator_host, self.coordinator_port))
                    hb_msg = {
                        "type": MSG_HEARTBEAT,
                        "worker_id": self.worker_id,
                        "host": self.host,
                        "port": self.port,
                        "max_threads": self.pool_size,
                        "active_threads": metrics["active_threads"],
                        "completed_tasks": metrics["completed_tasks"],
                        "failed_tasks": metrics["failed_tasks"]
                    }
                    send_message(sock, hb_msg)
                    recv_message(sock, timeout=2.0)
            except Exception:
                pass  # Si el coordinador reinicia, el siguiente latido reestablecerá el vínculo

    def _listen_for_tasks_loop(self) -> None:
        """Acepta conexiones TCP con tareas despachadas por el Coordinador."""
        while self.is_running.is_set():
            try:
                client_sock, addr = self.server_socket.accept()
                handler_thread = threading.Thread(
                    target=self._handle_incoming_connection,
                    args=(client_sock,),
                    daemon=True
                )
                handler_thread.start()
            except Exception:
                break

    def _handle_incoming_connection(self, client_sock: socket.socket) -> None:
        """Procesa la petición de despacho de una tarea entrante."""
        with client_sock:
            msg = recv_message(client_sock, timeout=5.0)
            if not msg:
                return

            if msg.get("type") == MSG_DISPATCH_TASK:
                task_data = msg.get("task")
                task = Task.from_dict(task_data)
                
                # Encolar en el pool de hilos de cómputo
                self.thread_pool.submit(task)

                # Confirmar recepción al Coordinador
                send_message(client_sock, {
                    "type": MSG_TASK_ACCEPTED,
                    "task_id": task.task_id,
                    "worker_id": self.worker_id
                })
            else:
                send_message(client_sock, {
                    "type": MSG_ERROR,
                    "message": "Tipo de mensaje no reconocido por Worker"
                })

    def _on_task_finished(self, task: Task) -> None:
        """Callback ejecutado por el hilo de cómputo para enviar el resultado al Coordinador."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(4.0)
                sock.connect((self.coordinator_host, self.coordinator_port))
                result_msg = {
                    "type": MSG_TASK_RESULT,
                    "task": task.to_dict()
                }
                send_message(sock, result_msg)
        except Exception as err:
            print(f"[{self.worker_id}] Error reportando resultado de tarea {task.task_id}: {err}")

    def stop(self) -> None:
        """Detiene el nodo y libera los sockets e hilos."""
        self.is_running.clear()
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass
        self.thread_pool.shutdown()
        print(f"[{self.worker_id}] Nodo detenido.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Nodo de Procesamiento HYDRA Worker")
    parser.add_argument("--id", type=str, default="Worker-1", help="Identificador único del worker")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Dirección IP de escucha")
    parser.add_argument("--port", type=int, default=5001, help="Puerto de escucha")
    parser.add_argument("--coord-host", type=str, default="127.0.0.1", help="Host del Coordinador")
    parser.add_argument("--coord-port", type=int, default=5000, help="Puerto del Coordinador")
    parser.add_argument("--threads", type=int, default=4, help="Cantidad de hilos en el Thread Pool")
    args = parser.parse_args()

    node = WorkerNode(
        worker_id=args.id,
        host=args.host,
        port=args.port,
        coordinator_host=args.coord_host,
        coordinator_port=args.coord_port,
        pool_size=args.threads
    )
    node.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.stop()
        sys.exit(0)
