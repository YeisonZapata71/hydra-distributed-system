"""
Patrón Worker Pool (Thread Pool Concurrente) para Nodos de Procesamiento.
Gestiona un conjunto finito de hilos trabajadores (WorkerThreads) para ejecutar tareas
computacionales intensivas concurrentemente sin sobrecargar el sistema operativo.
"""

import hashlib
import threading
import queue
import time
import random
import os
import sys
from typing import Callable, Optional, Dict, Any

try:
    from ..common.models import Task, TaskStatus, TaskType
except (ImportError, ValueError):
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
    from src.common.models import Task, TaskStatus, TaskType


def execute_computational_work(task_type: str, payload: Dict[str, Any]) -> Any:
    """
    Ejecuta el cómputo real según el tipo de tarea distribuida.
    """
    if task_type == TaskType.CRYPTOGRAPHIC_HASH.value:
        # Cómputo CPU intensivo: Hashing iterativo SHA-256 (Prueba de Trabajo / Criptografía)
        iterations = payload.get("iterations", 25000)
        seed = payload.get("seed", "hydra-seed-" + str(random.random()))
        current_hash = hashlib.sha256(seed.encode("utf-8")).hexdigest()
        for _ in range(iterations):
            current_hash = hashlib.sha256(current_hash.encode("utf-8")).hexdigest()
        return {"final_hash": current_hash, "iterations": iterations}

    elif task_type == TaskType.MATRIX_MULTIPLICATION.value:
        # Cómputo matricial: Multiplicación de matrices cuadradas
        size = payload.get("size", 40)
        A = [[(i + j) % 10 for j in range(size)] for i in range(size)]
        B = [[(i * j) % 10 for j in range(size)] for i in range(size)]
        C = [[0 for _ in range(size)] for _ in range(size)]
        for i in range(size):
            for j in range(size):
                dot_sum = 0
                for k in range(size):
                    dot_sum += A[i][k] * B[k][j]
                C[i][j] = dot_sum
        return {"matrix_dimension": f"{size}x{size}", "checksum": sum(sum(row) for row in C)}

    elif task_type == TaskType.DATA_TRANSFORMATION.value:
        # Transformación, ordenamiento y agregación de colecciones de datos
        records_count = payload.get("records_count", 5000)
        data = [{"id": i, "val": (i * 37) % 1000, "cat": f"cat_{i % 5}"} for i in range(records_count)]
        sorted_data = sorted(data, key=lambda x: x["val"], reverse=True)
        summary = {}
        for item in sorted_data:
            c = item["cat"]
            summary[c] = summary.get(c, 0) + item["val"]
        return {"processed_records": records_count, "category_aggregates": summary}

    elif task_type == TaskType.RESOURCE_AGGREGATION.value:
        # Simulación de agregación estadística distribuida
        samples = payload.get("samples", 10000)
        values = [(i ** 0.5) * (i % 7) for i in range(samples)]
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return {"samples": samples, "mean": round(mean, 4), "variance": round(variance, 4)}

    else:
        # Tarea genérica con retardo controlado
        sleep_sec = payload.get("sleep_sec", 0.05)
        time.sleep(sleep_sec)
        return {"status": "ok", "echo_payload": payload}


class WorkerThreadPool:
    """
    Gestor de hilos de ejecución concurrente para un nodo worker.
    """
    def __init__(self, worker_id: str, pool_size: int = 4, on_task_complete: Optional[Callable[[Task], None]] = None):
        self.worker_id = worker_id
        self.pool_size = pool_size
        self.on_task_complete = on_task_complete
        
        self.task_queue: queue.Queue[Task] = queue.Queue()
        self.threads: list[threading.Thread] = []
        self.is_running = threading.Event()
        self.is_running.set()
        
        self.lock = threading.Lock()
        self.active_threads_count = 0
        self.completed_tasks_count = 0
        self.failed_tasks_count = 0

        # Instanciar e inicializar los hilos trabajadores
        for i in range(pool_size):
            t = threading.Thread(
                target=self._worker_loop,
                name=f"{worker_id}-Thread-{i + 1}",
                daemon=True
            )
            self.threads.append(t)
            t.start()

    def submit(self, task: Task) -> None:
        """Encola una tarea para ser atendida por el primer hilo disponible."""
        task.status = TaskStatus.QUEUED.value
        self.task_queue.put(task)

    def get_metrics(self) -> Dict[str, Any]:
        """Obtiene métricas de concurrencia y saturación del pool de hilos."""
        with self.lock:
            return {
                "total_threads": self.pool_size,
                "active_threads": self.active_threads_count,
                "idle_threads": self.pool_size - self.active_threads_count,
                "queue_depth": self.task_queue.qsize(),
                "completed_tasks": self.completed_tasks_count,
                "failed_tasks": self.failed_tasks_count
            }

    def _worker_loop(self) -> None:
        """Bucle principal de cada hilo trabajador."""
        thread_name = threading.current_thread().name
        while self.is_running.is_set():
            try:
                task = self.task_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            with self.lock:
                self.active_threads_count += 1

            start_time = time.perf_counter()
            task.assigned_worker = self.worker_id
            task.assigned_thread = thread_name
            task.status = TaskStatus.RUNNING.value

            try:
                result = execute_computational_work(task.task_type, task.payload)
                task.result = result
                task.status = TaskStatus.COMPLETED.value
                with self.lock:
                    self.completed_tasks_count += 1
            except Exception as ex:
                task.status = TaskStatus.FAILED.value
                task.error_message = str(ex)
                with self.lock:
                    self.failed_tasks_count += 1
            finally:
                elapsed_ms = (time.perf_counter() - start_time) * 1000.0
                task.execution_time_ms = round(elapsed_ms, 2)
                task.completed_at = time.time()
                self.task_queue.task_done()

                with self.lock:
                    self.active_threads_count -= 1

                # Notificar finalización al callback de red
                if self.on_task_complete:
                    try:
                        self.on_task_complete(task)
                    except Exception as err:
                        print(f"[{thread_name}] Error en callback de completado: {err}")

    def shutdown(self) -> None:
        """Detiene de forma ordenada todos los hilos trabajadores."""
        self.is_running.clear()
        for t in self.threads:
            t.join(timeout=1.0)
