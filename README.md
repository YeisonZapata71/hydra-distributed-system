# HYDRA-DTS: High-Yield Distributed Resource & Task Scheduler
### Prototipo de Arquitectura de Software Distribuida

**HYDRA-DTS** es un prototipo funcional de arquitectura de software distribuida para el procesamiento paralelo y balanceo concurrente de cargas de trabajo sobre sockets TCP con paso de mensajes, gestión de hilos (*multithreading*), monitoreo continuo (*heartbeat*) y tolerancia activa a fallos (*failover*).

---

## 🏛️ 1. Arquitectura y Patrones de Diseño Implementados

* **Broker / Mediator:** El nodo Coordinador centraliza la recepción, priorización y despacho desacoplado de tareas hacia los workers.
* **Worker Pool (Thread Pool Concurrente):** Cada nodo worker ejecuta un pool acotado de hilos de trabajo independientes (`WorkerThread-1..N`) para cómputo sin saturar el sistema operativo.
* **Remote Proxy:** `HydraClientProxy` encapsula los sockets de red, serialización JSON y timeouts ofreciendo una API limpia al desarrollador.
* **Observer Distribuido con Heartbeats:** Supervisión activa de nodos cada 1.5s; detección de nodos caídos en < 4s.
* **Failover Automático:** Reasignación inmediata de tareas huérfanas ante la desconexión intempestiva de un nodo.

---

## 📁 2. Estructura del Prototipo

```
HYDRA_DISTRIBUTED_SYSTEM/
├── src/
│   ├── common/
│   │   ├── protocol.py            # Protocolo TCP con Length-Prefix Framing (evita fragmentación)
│   │   └── models.py              # Entidades: Task, Priority, WorkerInfo, Metrics
│   ├── coordinator/
│   │   ├── coordinator_node.py    # Servidor Maestro (Broker, Dispatcher, Heartbeat Monitor)
│   │   └── load_balancer.py       # Algoritmos de balanceo (Least-Loaded y Round-Robin)
│   ├── worker/
│   │   ├── worker_node.py         # Nodo de Cómputo autónomo en red
│   │   └── thread_pool.py         # Pool de hilos de ejecución paralela concurrentes
│   ├── client/
│   │   ├── client_proxy.py        # Remote Proxy para llamadas transparentes
│   │   └── client_cli.py          # Interfaz de línea de comandos para usuarios
│   └── tests/
│       ├── benchmark_stress.py    # Suite de pruebas de carga concurrente y escalabilidad
│       └── fault_tolerance_test.py# Prueba de resiliencia y recuperación ante caída de nodos
├── run_cluster.py                 # Orquestador del cluster completo (1 Maestro + 3 Workers)
└── README.md
```

---

## 🚀 3. Guía Rápida de Ejecución

### Requisitos
* Python 3.10 o superior (utiliza la biblioteca estándar, sin dependencias externas).

### Paso 1: Iniciar el Cluster Distribuido
Abre una terminal en la carpeta del proyecto y ejecuta:
```bash
python run_cluster.py
```
Esto iniciará:
* **1 Nodo Coordinador** en `127.0.0.1:5000`
* **Worker Alpha** en `127.0.0.1:5001` (4 hilos de cómputo)
* **Worker Beta** en `127.0.0.1:5002` (4 hilos de cómputo)
* **Worker Gamma** en `127.0.0.1:5003` (4 hilos de cómputo)
* **Total:** 12 hilos concurrentes de procesamiento.

---

### Paso 2: Interactuar con el Cliente (CLI)
En una segunda terminal, ejecuta:
```bash
python -m src.client.client_cli
```
Permite seleccionar tareas de cómputo intensivo (hashing criptográfico SHA-256, multiplicación de matrices cuadradas o procesamiento de datos) y consultar métricas del cluster en tiempo real.

---

### Paso 3: Ejecutar la Suite de Pruebas de Carga y Escalabilidad
Para evaluar el comportamiento del sistema ante diferentes niveles de concurrencia:
```bash
python -m src.tests.benchmark_stress
```
El script somete al cluster a ráfagas de **10, 25, 50 y 100 clientes concurrentes**, midiendo:
* Throughput (tareas por segundo)
* Latencias (milisegundos): Mínima, Media, Mediana (P50), P95 y P99
* Tasa de éxito porcentual (100.0%)

---

### Paso 4: Ejecutar la Prueba de Tolerancia a Fallos (Robustez)
```bash
python -m src.tests.fault_tolerance_test
```
El script envía un lote de tareas pesadas mientras se simula la desconexión de un nodo. El monitor de latidos detecta la caída, declara el nodo caído y reasigna automáticamente las tareas en vuelo a los nodos activos sin pérdida de información.

---

## 👤 Autor
* **Yeison Zapata**
* Arquitectura de Software – Unidad 2
