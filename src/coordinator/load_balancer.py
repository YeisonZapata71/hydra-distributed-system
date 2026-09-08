"""
Módulo de Balanceo de Carga para el Nodo Coordinador.
Implementa políticas de distribución de tareas entre nodos de cómputo distribuidos:
- Least-Loaded (Menor carga de hilos activos / mayor disponibilidad)
- Round-Robin Ponderado
"""

import threading
import os
import sys
from typing import List, Optional

try:
    from ..common.models import WorkerInfo
except (ImportError, ValueError):
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
    from src.common.models import WorkerInfo


class LoadBalancer:
    def __init__(self, strategy: str = "LEAST_LOADED"):
        self.strategy = strategy
        self._rr_index = 0
        self._lock = threading.Lock()

    def select_worker(self, available_workers: List[WorkerInfo]) -> Optional[WorkerInfo]:
        """
        Selecciona el mejor nodo worker disponible según la estrategia configurada.
        """
        alive_workers = [w for w in available_workers if w.is_alive()]
        if not alive_workers:
            return None

        with self._lock:
            if self.strategy == "ROUND_ROBIN":
                worker = alive_workers[self._rr_index % len(alive_workers)]
                self._rr_index += 1
                return worker

            # Por defecto: LEAST_LOADED (Prioriza el worker con más hilos libres)
            best_worker = min(
                alive_workers,
                key=lambda w: (w.active_threads / max(1, w.max_threads), w.active_threads)
            )
            return best_worker
