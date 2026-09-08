"""
Script Orquestador para Lanzamiento Local del Cluster HYDRA-DTS.
Permite levantar en un solo comando:
- 1 Nodo Coordinador (Maestro en puerto 5000)
- 3 Nodos Workers independientes (Puertos 5001, 5002, 5003), cada uno con su propio ThreadPool
Simulando una topología de red distribuida concurrente completa.
"""

import time
import sys
import threading
from src.coordinator.coordinator_node import CoordinatorNode
from src.worker.worker_node import WorkerNode


def run_full_cluster():
    print("=" * 70)
    print("      INICIANDO CLUSTER DISTRIBUIDO HYDRA-DTS (1 MAESTRO + 3 WORKERS)      ")
    print("=" * 70)

    # 1. Iniciar Nodo Coordinador (Maestro)
    coordinator = CoordinatorNode(host="127.0.0.1", port=5000, strategy="LEAST_LOADED")
    coordinator.start()

    time.sleep(0.5)

    # 2. Iniciar 3 Nodos Workers independientes
    workers = []
    worker_configs = [
        {"id": "Worker-Node-Alpha", "port": 5001, "threads": 4},
        {"id": "Worker-Node-Beta",  "port": 5002, "threads": 4},
        {"id": "Worker-Node-Gamma", "port": 5003, "threads": 4},
    ]

    for cfg in worker_configs:
        w = WorkerNode(
            worker_id=cfg["id"],
            host="127.0.0.1",
            port=cfg["port"],
            coordinator_host="127.0.0.1",
            coordinator_port=5000,
            pool_size=cfg["threads"]
        )
        w.start()
        workers.append(w)
        time.sleep(0.3)

    print("\n" + "=" * 70)
    print("🚀 CLUSTER OPERATIVO: 1 Maestro + 3 Workers (Total: 12 Hilos de Cómputo)")
    print("   • Coordinador escuchando en: 127.0.0.1:5000")
    print("   • Worker Alpha en:           127.0.0.1:5001 (4 hilos)")
    print("   • Worker Beta en:            127.0.0.1:5002 (4 hilos)")
    print("   • Worker Gamma en:           127.0.0.1:5003 (4 hilos)")
    print("=" * 70)
    print("Presiona Ctrl+C en cualquier momento para detener todos los nodos.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDeteniendo cluster de forma ordenada...")
        for w in workers:
            w.stop()
        coordinator.stop()
        print("Cluster detenido correctamente.")
        sys.exit(0)


if __name__ == "__main__":
    run_full_cluster()
