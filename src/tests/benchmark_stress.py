"""
Suite de Pruebas de Carga y Evaluación de Escalabilidad para HYDRA-DTS.
Evalúa el comportamiento del sistema ante diferentes niveles de concurrencia:
- 10 clientes concurrentes
- 25 clientes concurrentes
- 50 clientes concurrentes
- 100 clientes concurrentes

Mide cuantitativamente:
- Throughput (Tareas procesadas / segundo)
- Latencia Promedio, P50, P95, P99 (milisegundos)
- Tasa de Éxito (%)
- Distribución de carga entre nodos
"""

import time
import threading
import json
import statistics
import os
import sys
import argparse
from typing import List, Dict, Any

try:
    from ..client.client_proxy import HydraClientProxy
    from ..common.models import TaskType, TaskPriority
except (ImportError, ValueError):
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
    from src.client.client_proxy import HydraClientProxy
    from src.common.models import TaskType, TaskPriority


def run_concurrency_test(
    concurrency_level: int,
    tasks_per_client: int,
    coord_host: str,
    coord_port: int,
    task_type: str = TaskType.CRYPTOGRAPHIC_HASH.value
) -> Dict[str, Any]:
    """
    Ejecuta una oleada de N clientes concurrentes enviando tareas al cluster.
    """
    total_tasks = concurrency_level * tasks_per_client
    latencies: List[float] = []
    successes = 0
    failures = 0
    lock = threading.Lock()

    def client_worker(client_id: int):
        nonlocal successes, failures
        proxy = HydraClientProxy(coord_host, coord_port, timeout=15.0)
        for i in range(tasks_per_client):
            t_start = time.perf_counter()
            try:
                task_id = proxy.submit_task(
                    task_type=task_type,
                    payload={"iterations": 12000, "seed": f"bench-{client_id}-{i}"},
                    priority=TaskPriority.NORMAL.value
                )
                task = proxy.wait_for_completion(task_id, timeout_sec=20.0)
                elapsed_ms = (time.perf_counter() - t_start) * 1000.0
                with lock:
                    latencies.append(elapsed_ms)
                    if task.status == "COMPLETED":
                        successes += 1
                    else:
                        failures += 1
            except Exception as e:
                with lock:
                    failures += 1

    threads: List[threading.Thread] = []
    print(f"--> Iniciando prueba con {concurrency_level} hilos concurrentes ({total_tasks} tareas totales)...")
    wall_start = time.perf_counter()

    for c in range(concurrency_level):
        t = threading.Thread(target=client_worker, args=(c,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    total_wall_time = time.perf_counter() - wall_start
    throughput = total_tasks / total_wall_time if total_wall_time > 0 else 0

    lat_mean = statistics.mean(latencies) if latencies else 0
    lat_median = statistics.median(latencies) if latencies else 0
    lat_min = min(latencies) if latencies else 0
    lat_max = max(latencies) if latencies else 0
    lat_p95 = statistics.quantiles(latencies, n=20)[18] if len(latencies) >= 20 else lat_max
    lat_p99 = statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else lat_max

    result = {
        "concurrency": concurrency_level,
        "total_tasks": total_tasks,
        "completed": successes,
        "failed": failures,
        "success_rate_pct": round((successes / total_tasks) * 100, 2) if total_tasks else 0,
        "wall_time_sec": round(total_wall_time, 2),
        "throughput_tasks_per_sec": round(throughput, 2),
        "latency_min_ms": round(lat_min, 2),
        "latency_mean_ms": round(lat_mean, 2),
        "latency_median_ms": round(lat_median, 2),
        "latency_p95_ms": round(lat_p95, 2),
        "latency_p99_ms": round(lat_p99, 2)
    }

    print(f"    ✓ Completado en {result['wall_time_sec']}s | Throughput: {result['throughput_tasks_per_sec']} req/s | Latencia Media: {result['latency_mean_ms']}ms")
    return result


def run_full_benchmark(coord_host: str = "127.0.0.1", coord_port: int = 5000) -> List[Dict[str, Any]]:
    levels = [
        {"concurrency": 10,  "tasks_per_client": 5},
        {"concurrency": 25,  "tasks_per_client": 4},
        {"concurrency": 50,  "tasks_per_client": 3},
        {"concurrency": 100, "tasks_per_client": 2}
    ]

    results = []
    print("=" * 75)
    print("      INICIO DE EVALUACIÓN DE ESCALABILIDAD Y DESEMPEÑO HYDRA      ")
    print("=" * 75)

    for item in levels:
        res = run_concurrency_test(
            concurrency_level=item["concurrency"],
            tasks_per_client=item["tasks_per_client"],
            coord_host=coord_host,
            coord_port=coord_port
        )
        results.append(res)
        time.sleep(1.0)  # Breve pausa para estabilización de buffers de red

    print("\n" + "=" * 90)
    print("                 RESUMEN CUANTITATIVO DE RENDIMIENTO Y ESCALABILIDAD")
    print("=" * 90)
    header = f"{'Concurrencia':<13} | {'Tareas':<8} | {'Éxito %':<8} | {'Tiempo(s)':<10} | {'Throughput(t/s)':<17} | {'Lat.Media(ms)':<14} | {'Lat.P95(ms)':<12}"
    print(header)
    print("-" * 90)
    for r in results:
        row = f"{r['concurrency']:<13} | {r['total_tasks']:<8} | {r['success_rate_pct']:<8} | {r['wall_time_sec']:<10} | {r['throughput_tasks_per_sec']:<17} | {r['latency_mean_ms']:<14} | {r['latency_p95_ms']:<12}"
        print(row)
    print("=" * 90)

    # Guardar en archivo JSON para inclusión en informe técnico
    with open("benchmark_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print("\nResultados guardados exitosamente en 'benchmark_results.json'")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark de Carga para HYDRA")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    args = parser.parse_args()

    run_full_benchmark(args.host, args.port)
