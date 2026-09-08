"""
Prueba de Robustez y Tolerancia a Fallos (Failover) para HYDRA-DTS.
Demuestra la resiliencia del sistema distribuido ante la desconexión o muerte súbita
de un nodo de cómputo (Worker Node) durante la ejecución de tareas.
"""

import time
import threading
import os
import sys
import argparse
from typing import List

try:
    from ..client.client_proxy import HydraClientProxy
    from ..common.models import TaskType, TaskPriority
except (ImportError, ValueError):
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
    from src.client.client_proxy import HydraClientProxy
    from src.common.models import TaskType, TaskPriority


def test_fault_tolerance(coord_host: str = "127.0.0.1", coord_port: int = 5000):
    print("=" * 70)
    print("      PRUEBA DE ROBUSTEZ Y TOLERANCIA A FALLOS (FAILOVER)     ")
    print("=" * 70)

    proxy = HydraClientProxy(coord_host, coord_port)

    # 1. Verificar estado inicial del cluster
    metrics = proxy.get_cluster_metrics()
    active_count = metrics.get("active_workers_count", 0)
    print(f"1. Estado inicial: {active_count} workers activos registrados.")
    if active_count < 2:
        print("⚠️ Se recomienda contar con al menos 2 workers activos para esta prueba.")

    # 2. Despachar un lote de 20 tareas pesadas
    print("\n2. Despachando lote de 20 tareas con cómputo continuo...")
    task_ids: List[str] = []
    for i in range(20):
        tid = proxy.submit_task(
            task_type=TaskType.CRYPTOGRAPHIC_HASH.value,
            payload={"iterations": 35000, "seed": f"failover-task-{i}"},
            priority=TaskPriority.HIGH.value
        )
        task_ids.append(tid)

    print("   ✓ 20 tareas encoladas en el Coordinador.")
    time.sleep(1.0)

    # 3. Instrucción para simular la muerte de un nodo
    print("\n3. [ESCENARIO DE FALLO]: En este momento, puedes detener forzosamente uno de los")
    print("   procesos Worker (ej. presiona Ctrl+C en la terminal de Worker-2 o Worker-3).")
    print("   El Coordinador detectará la ausencia de latidos en ~4 segundos y reasignará")
    print("   automáticamente todas las tareas en curso hacia los nodos supervivientes.\n")
    print("   Esperando resolución completa de todas las tareas (tolerancia máxima: 40s)...")

    start_wait = time.time()
    completed = 0
    failed = 0
    retried_count = 0

    for tid in task_ids:
        try:
            task = proxy.wait_for_completion(tid, timeout_sec=40.0)
            if task.status == "COMPLETED":
                completed += 1
                if task.retries > 0:
                    retried_count += 1
            else:
                failed += 1
        except Exception:
            failed += 1

    total_time = time.time() - start_wait
    print("\n" + "=" * 70)
    print("                      RESULTADOS DE LA PRUEBA")
    print("=" * 70)
    print(f"• Tareas completadas con éxito: {completed} / {len(task_ids)} ({(completed/len(task_ids))*100:.1f}%)")
    print(f"• Tareas recuperadas vía failover: {retried_count}")
    print(f"• Tareas perdidas o fallidas: {failed}")
    print(f"• Tiempo total transcurrido: {total_time:.2f} s")

    metrics_final = proxy.get_cluster_metrics()
    print(f"• Workers activos finales: {metrics_final.get('active_workers_count')}")
    print(f"• Total reintentos registrados por el Coordinador: {metrics_final.get('total_retried')}")
    
    if failed == 0 and completed == len(task_ids):
        print("\n🏆 RESULTADO: PRUEBA DE TOLERANCIA A FALLOS EXITOSA (100% RESILIENTE)")
    else:
        print("\n⚠️ RESULTADO: Se presentaron pérdidas de tareas.")
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prueba de Tolerancia a Fallos HYDRA")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    args = parser.parse_args()

    test_fault_tolerance(args.host, args.port)
