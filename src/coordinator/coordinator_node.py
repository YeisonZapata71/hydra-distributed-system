"""
Nodo Coordinador Maestro (Master / Coordinator Node).
Implementa los patrones arquitectónicos Broker y Mediator:
- Centraliza la admisión de tareas de múltiples clientes concurrentes.
- Orquesta el despacho balanceado hacia los nodos Worker mediante colas con prioridad.
- Supervisa la salud de los nodos (Heartbeat Monitor) y ejecuta Failover automático
  en caso de caída de un nodo worker para garantizar Robustez y Tolerancia a Fallos.
"""

import socket
import threading
import time
import argparse
import os
import sys
from queue import PriorityQueue
from typing import Dict, Any, Optional

try:
    from ..common.protocol import (
        send_message, recv_message,
        MSG_REGISTER_WORKER, MSG_REGISTER_ACK,
        MSG_HEARTBEAT, MSG_HEARTBEAT_ACK,
        MSG_SUBMIT_TASK, MSG_TASK_ACCEPTED,
        MSG_DISPATCH_TASK, MSG_TASK_RESULT,
        MSG_GET_TASK_STATUS, MSG_TASK_STATUS_RESP,
        MSG_GET_METRICS, MSG_METRICS_RESP,
        MSG_ERROR
    )
    from ..common.models import Task, TaskStatus, TaskPriority, WorkerInfo
    from .load_balancer import LoadBalancer
except (ImportError, ValueError):
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
    from src.common.protocol import (
        send_message, recv_message,
        MSG_REGISTER_WORKER, MSG_REGISTER_ACK,
        MSG_HEARTBEAT, MSG_HEARTBEAT_ACK,
        MSG_SUBMIT_TASK, MSG_TASK_ACCEPTED,
        MSG_DISPATCH_TASK, MSG_TASK_RESULT,
        MSG_GET_TASK_STATUS, MSG_TASK_STATUS_RESP,
        MSG_GET_METRICS, MSG_METRICS_RESP,
        MSG_ERROR
    )
    from src.common.models import Task, TaskStatus, TaskPriority, WorkerInfo
    from src.coordinator.load_balancer import LoadBalancer


class CoordinatorNode:
    def __init__(self, host: str = "127.0.0.1", port: int = 5000, strategy: str = "LEAST_LOADED"):
        self.host = host
        self.port = port
        self.load_balancer = LoadBalancer(strategy=strategy)

        # Estructuras de datos sincronizadas con Locks
        self.lock = threading.Lock()
        self.workers: Dict[str, WorkerInfo] = {}
        self.tasks: Dict[str, Task] = {}
        
        # Cola de despacho con prioridades (Prioridad, timestamp, task_id)
        self.pending_queue: PriorityQueue = PriorityQueue()
        
        # Tareas en vuelo asignadas a workers: {task_id: worker_id}
        self.inflight_tasks: Dict[str, str] = {}

        # Métricas históricas del cluster
        self.total_submitted = 0
        self.total_completed = 0
        self.total_failed = 0
        self.total_retried = 0

        self.is_running = threading.Event()
        self.is_running.set()
        self.server_socket: Optional[socket.socket] = None

    def start(self) -> None:
        """Inicia el servidor y los hilos concurrentes del coordinador."""
        print(f"[COORDINATOR] Iniciando Maestro HYDRA en {self.host}:{self.port}...")

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(50)

        # 1. Hilo de Despacho de Tareas (Dispatcher Thread)
        dispatcher_thread = threading.Thread(
            target=self._task_dispatcher_loop,
            name="Coordinator-DispatcherThread",
            daemon=True
        )
        dispatcher_thread.start()

        # 2. Hilo Monitor de Latidos y Tolerancia a Fallos (Heartbeat & Failover Thread)
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_monitor_loop,
            name="Coordinator-HeartbeatMonitorThread",
            daemon=True
        )
        heartbeat_thread.start()

        # 3. Hilo Receptor de Conexiones de Red (Listener Thread)
        listener_thread = threading.Thread(
            target=self._connection_listener_loop,
            name="Coordinator-ConnectionListenerThread",
            daemon=True
        )
        listener_thread.start()

        print(f"[COORDINATOR] ✅ Maestro activo. Escuchando clientes y workers...")

    def _connection_listener_loop(self) -> None:
        """Acepta conexiones TCP entrantes y delega cada una a un hilo handler."""
        while self.is_running.is_set():
            try:
                client_sock, addr = self.server_socket.accept()
                handler_thread = threading.Thread(
                    target=self._handle_client_connection,
                    args=(client_sock, addr),
                    daemon=True
                )
                handler_thread.start()
            except Exception:
                break

    def _handle_client_connection(self, sock: socket.socket, addr: Any) -> None:
        """Maneja la solicitud de un cliente o worker según el protocolo."""
        with sock:
            msg = recv_message(sock, timeout=10.0)
            if not msg:
                return

            msg_type = msg.get("type")

            if msg_type == MSG_REGISTER_WORKER:
                self._handle_register_worker(sock, msg)
            elif msg_type == MSG_HEARTBEAT:
                self._handle_heartbeat(sock, msg)
            elif msg_type == MSG_SUBMIT_TASK:
                self._handle_submit_task(sock, msg)
            elif msg_type == MSG_TASK_RESULT:
                self._handle_task_result(sock, msg)
            elif msg_type == MSG_GET_TASK_STATUS:
                self._handle_get_task_status(sock, msg)
            elif msg_type == MSG_GET_METRICS:
                self._handle_get_metrics(sock, msg)
            else:
                send_message(sock, {"type": MSG_ERROR, "message": f"Tipo desconocido: {msg_type}"})

    def _handle_register_worker(self, sock: socket.socket, msg: Dict[str, Any]) -> None:
        worker_id = msg["worker_id"]
        worker = WorkerInfo(
            worker_id=worker_id,
            host=msg["host"],
            port=msg["port"],
            max_threads=msg["max_threads"],
            last_heartbeat=time.time(),
            state="READY"
        )
        with self.lock:
            self.workers[worker_id] = worker
        print(f"[COORDINATOR] Worker registrado: {worker_id} ({worker.host}:{worker.port}) con {worker.max_threads} hilos.")
        send_message(sock, {"type": MSG_REGISTER_ACK, "worker_id": worker_id})

    def _handle_heartbeat(self, sock: socket.socket, msg: Dict[str, Any]) -> None:
        worker_id = msg["worker_id"]
        with self.lock:
            if worker_id in self.workers:
                w = self.workers[worker_id]
                w.last_heartbeat = time.time()
                w.active_threads = msg.get("active_threads", 0)
                w.completed_tasks = msg.get("completed_tasks", w.completed_tasks)
                w.failed_tasks = msg.get("failed_tasks", w.failed_tasks)
                w.state = "ACTIVE"
            else:
                # Si el worker no estaba registrado (ej. tras reinicio del coordinador)
                self.workers[worker_id] = WorkerInfo(
                    worker_id=worker_id,
                    host=msg["host"],
                    port=msg["port"],
                    max_threads=msg.get("max_threads", 4),
                    active_threads=msg.get("active_threads", 0),
                    last_heartbeat=time.time(),
                    state="ACTIVE"
                )
        send_message(sock, {"type": MSG_HEARTBEAT_ACK})

    def _handle_submit_task(self, sock: socket.socket, msg: Dict[str, Any]) -> None:
        task_data = msg.get("task")
        task = Task.from_dict(task_data)
        weight = TaskPriority.get_weight(task.priority)

        with self.lock:
            self.tasks[task.task_id] = task
            self.total_submitted += 1
            # Se encola según tupla de prioridad: (prioridad_numerica, timestamp, task_id)
            self.pending_queue.put((weight, task.created_at, task.task_id))

        send_message(sock, {
            "type": MSG_TASK_ACCEPTED,
            "task_id": task.task_id,
            "status": TaskStatus.QUEUED.value
        })

    def _handle_task_result(self, sock: socket.socket, msg: Dict[str, Any]) -> None:
        task_data = msg.get("task")
        task_id = task_data["task_id"]

        with self.lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                task.status = task_data["status"]
                task.result = task_data.get("result")
                task.execution_time_ms = task_data.get("execution_time_ms", 0.0)
                task.completed_at = task_data.get("completed_at", time.time())
                task.assigned_worker = task_data.get("assigned_worker")
                task.assigned_thread = task_data.get("assigned_thread")
                task.error_message = task_data.get("error_message")

                if task.status == TaskStatus.COMPLETED.value:
                    self.total_completed += 1
                else:
                    self.total_failed += 1

                self.inflight_tasks.pop(task_id, None)

        send_message(sock, {"type": "RESULT_ACK", "task_id": task_id})

    def _handle_get_task_status(self, sock: socket.socket, msg: Dict[str, Any]) -> None:
        task_id = msg.get("task_id")
        with self.lock:
            task = self.tasks.get(task_id)
            if task:
                send_message(sock, {
                    "type": MSG_TASK_STATUS_RESP,
                    "task": task.to_dict()
                })
            else:
                send_message(sock, {
                    "type": MSG_ERROR,
                    "message": f"Tarea {task_id} no encontrada"
                })

    def _handle_get_metrics(self, sock: socket.socket, msg: Dict[str, Any]) -> None:
        with self.lock:
            active_workers = [w.to_dict() for w in self.workers.values() if w.is_alive()]
            metrics = {
                "type": MSG_METRICS_RESP,
                "total_submitted": self.total_submitted,
                "total_completed": self.total_completed,
                "total_failed": self.total_failed,
                "total_retried": self.total_retried,
                "queue_depth": self.pending_queue.qsize(),
                "inflight_count": len(self.inflight_tasks),
                "total_workers": len(self.workers),
                "active_workers_count": len(active_workers),
                "workers": active_workers
            }
        send_message(sock, metrics)

    def _task_dispatcher_loop(self) -> None:
        """
        Bucle de despacho: extrae tareas de la cola prioritaria y las envía
        al nodo worker seleccionado por el balanceador de carga.
        """
        while self.is_running.is_set():
            if self.pending_queue.empty():
                time.sleep(0.05)
                continue

            with self.lock:
                available_workers = [w for w in self.workers.values() if w.is_alive()]
                selected_worker = self.load_balancer.select_worker(available_workers)

            if not selected_worker:
                # No hay workers disponibles en este instante; esperar un momento
                time.sleep(0.1)
                continue

            try:
                priority, ts, task_id = self.pending_queue.get_nowait()
            except Exception:
                continue

            with self.lock:
                task = self.tasks.get(task_id)
                if not task or task.status == TaskStatus.COMPLETED.value:
                    continue

                task.status = TaskStatus.DISPATCHED.value
                task.dispatched_at = time.time()
                task.assigned_worker = selected_worker.worker_id
                self.inflight_tasks[task_id] = selected_worker.worker_id

            # Despachar tarea al worker vía socket TCP
            success = self._dispatch_to_worker(selected_worker, task)
            if not success:
                # Si falló la comunicación inmediata, reencolar para failover
                with self.lock:
                    task.retries += 1
                    task.status = TaskStatus.RETRYING.value
                    self.total_retried += 1
                    self.inflight_tasks.pop(task_id, None)
                    weight = TaskPriority.get_weight(task.priority)
                    self.pending_queue.put((weight, time.time(), task_id))

    def _dispatch_to_worker(self, worker: WorkerInfo, task: Task) -> bool:
        """Abre un socket hacia el nodo Worker y transmite la tarea para cómputo."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(3.0)
                sock.connect((worker.host, worker.port))
                send_message(sock, {
                    "type": MSG_DISPATCH_TASK,
                    "task": task.to_dict()
                })
                resp = recv_message(sock, timeout=3.0)
                return resp is not None and resp.get("type") == MSG_TASK_ACCEPTED
        except Exception:
            return False

    def _heartbeat_monitor_loop(self) -> None:
        """
        Mecanismo de Tolerancia a Fallos (Robustez):
        Supervisa la vigencia de los latidos de cada nodo. Si un worker no reporta
        actividad en > 4 segundos, se declara CAÍDO y sus tareas en vuelo son
        reasignadas automáticamente a los workers sanos restantes.
        """
        while self.is_running.is_set():
            time.sleep(1.5)
            dead_workers = []
            with self.lock:
                for w_id, w_info in self.workers.items():
                    if not w_info.is_alive(timeout_sec=4.0) and w_info.state != "DEAD":
                        w_info.state = "DEAD"
                        dead_workers.append(w_id)
                        print(f"[COORDINATOR] ⚠️ ALERTA DE TOLERANCIA A FALLOS: Worker '{w_id}' no responde. Declarado CAÍDO.")

                # Reasignar tareas huérfanas de los workers caídos
                for dead_id in dead_workers:
                    orphan_tasks = [t_id for t_id, assigned_w in list(self.inflight_tasks.items()) if assigned_w == dead_id]
                    for t_id in orphan_tasks:
                        task = self.tasks.get(t_id)
                        if task and task.status != TaskStatus.COMPLETED.value:
                            task.status = TaskStatus.RETRYING.value
                            task.retries += 1
                            self.total_retried += 1
                            self.inflight_tasks.pop(t_id, None)
                            weight = TaskPriority.get_weight(task.priority)
                            self.pending_queue.put((weight, time.time(), t_id))
                            print(f"[COORDINATOR] 🔄 Tarea huérfana '{t_id}' reencolada exitosamente para nuevo worker.")

    def stop(self) -> None:
        """Detiene de forma segura el nodo coordinador."""
        self.is_running.clear()
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass
        print("[COORDINATOR] Servidor detenido.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Nodo Coordinador Maestro HYDRA")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host de escucha")
    parser.add_argument("--port", type=int, default=5000, help="Puerto de escucha")
    parser.add_argument("--strategy", type=str, default="LEAST_LOADED", choices=["LEAST_LOADED", "ROUND_ROBIN"])
    args = parser.parse_args()

    coord = CoordinatorNode(host=args.host, port=args.port, strategy=args.strategy)
    coord.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        coord.stop()
        sys.exit(0)
