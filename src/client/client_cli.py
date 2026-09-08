"""
Interfaz de Línea de Comandos (CLI) del Cliente HYDRA.
Permite interactuar interactivamente con el cluster distribuido.
"""

import os
import sys
import time
import argparse

try:
    from .client_proxy import HydraClientProxy
    from ..common.models import TaskType, TaskPriority
except (ImportError, ValueError):
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
    from src.client.client_proxy import HydraClientProxy
    from src.common.models import TaskType, TaskPriority


def print_banner():
    print("=" * 65)
    print("      HYDRA-DTS : CLIENTE DISTRIBUIDO DE PROCESAMIENTO        ")
    print("=" * 65)


def run_interactive(proxy: HydraClientProxy):
    print_banner()
    while True:
        print("\nOpciones disponibles:")
        print("1. Enviar tarea de Hashing Criptográfico (SHA-256 CPU)")
        print("2. Enviar tarea de Multiplicación Matricial")
        print("3. Enviar tarea de Procesamiento de Datos")
        print("4. Enviar lote concurrente de tareas (Prueba rápida)")
        print("5. Consultar métricas y estado del cluster")
        print("0. Salir")
        
        choice = input("\nSeleccione una opción [0-5]: ").strip()
        try:
            if choice == "0":
                break
            elif choice == "1":
                iters = int(input("Iteraciones SHA-256 [ej. 20000]: ") or "20000")
                print("Enviando tarea al cluster...")
                task_id = proxy.submit_task(
                    task_type=TaskType.CRYPTOGRAPHIC_HASH.value,
                    payload={"iterations": iters, "seed": "data-sample"},
                    priority=TaskPriority.HIGH.value
                )
                print(f"Tarea aceptada con ID: {task_id}. Esperando resultado...")
                result_task = proxy.wait_for_completion(task_id)
                print(f"✅ Tarea completada por nodo '{result_task.assigned_worker}' (Hilo: {result_task.assigned_thread})")
                print(f"   Tiempo de ejecución: {result_task.execution_time_ms} ms")
                print(f"   Resultado: {result_task.result}")

            elif choice == "2":
                dim = int(input("Dimensión de matriz [ej. 40]: ") or "40")
                task_id = proxy.submit_task(
                    task_type=TaskType.MATRIX_MULTIPLICATION.value,
                    payload={"size": dim}
                )
                print(f"Tarea {task_id} enviada. Esperando resultado...")
                res = proxy.wait_for_completion(task_id)
                print(f"✅ Multiplicación completada por {res.assigned_worker} ({res.assigned_thread}) en {res.execution_time_ms} ms")
                print(f"   Resultado: {res.result}")

            elif choice == "3":
                records = int(input("Registros a procesar [ej. 5000]: ") or "5000")
                task_id = proxy.submit_task(
                    task_type=TaskType.DATA_TRANSFORMATION.value,
                    payload={"records_count": records}
                )
                res = proxy.wait_for_completion(task_id)
                print(f"✅ Transformación finalizada en {res.execution_time_ms} ms por {res.assigned_worker}")
                print(f"   Resumen: {res.result}")

            elif choice == "4":
                count = int(input("Cantidad de tareas en el lote [ej. 20]: ") or "20")
                print(f"Disparando lote de {count} tareas...")
                task_ids = []
                start = time.perf_counter()
                for i in range(count):
                    tid = proxy.submit_task(
                        task_type=TaskType.CRYPTOGRAPHIC_HASH.value,
                        payload={"iterations": 15000, "seed": f"batch-{i}"}
                    )
                    task_ids.append(tid)
                
                print(f"{count} tareas sometidas. Esperando finalización del cluster...")
                completed = 0
                for tid in task_ids:
                    proxy.wait_for_completion(tid, timeout_sec=30.0)
                    completed += 1
                total_time = (time.perf_counter() - start)
                print(f"🚀 Lote completado: {completed} tareas en {total_time:.2f} s ({completed/total_time:.2f} tareas/seg)")

            elif choice == "5":
                metrics = proxy.get_cluster_metrics()
                print("\n--- MÉTRICAS DEL CLUSTER EN VIVO ---")
                print(f"Tareas recibidas: {metrics.get('total_submitted')}")
                print(f"Tareas completadas: {metrics.get('total_completed')}")
                print(f"Tareas fallidas: {metrics.get('total_failed')}")
                print(f"Tareas reintentadas (failover): {metrics.get('total_retried')}")
                print(f"Nodos Workers activos: {metrics.get('active_workers_count')}")
                for w in metrics.get("workers", []):
                    print(f"  • {w['worker_id']} ({w['host']}:{w['port']}) | Hilos activos: {w['active_threads']}/{w['max_threads']} | Completadas: {w['completed_tasks']} | Estado: {w['state']}")
        except ConnectionRefusedError as ce:
            print("\n❌ [ERROR DE CONEXIÓN]")
            print(f"   {ce}\n")
        except Exception as ex:
            print(f"\n⚠️ Error al comunicarse con el cluster: {ex}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cliente CLI para HYDRA")
    parser.add_argument("--coord-host", type=str, default="127.0.0.1")
    parser.add_argument("--coord-port", type=int, default=5000)
    args = parser.parse_args()

    client = HydraClientProxy(args.coord_host, args.coord_port)
    run_interactive(client)
