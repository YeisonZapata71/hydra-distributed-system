"""
Modelos de Datos y Entidades para el Sistema Distribuido HYDRA-DTS.
"""

from dataclasses import dataclass, field, asdict
from enum import Enum
import time
from typing import Any, Dict, Optional


class TaskPriority(str, Enum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

    @classmethod
    def get_weight(cls, priority: str) -> int:
        weights = {
            cls.CRITICAL.value: 1,
            cls.HIGH.value: 2,
            cls.NORMAL.value: 3,
            cls.LOW.value: 4
        }
        return weights.get(priority, 3)


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    DISPATCHED = "DISPATCHED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"


class TaskType(str, Enum):
    CRYPTOGRAPHIC_HASH = "CRYPTOGRAPHIC_HASH"      # Carga intensiva de CPU (hashing criptográfico)
    MATRIX_MULTIPLICATION = "MATRIX_MULTIPLICATION"# Carga matemática matricial
    DATA_TRANSFORMATION = "DATA_TRANSFORMATION"    # Procesamiento y filtrado de datos distribuidos
    RESOURCE_AGGREGATION = "RESOURCE_AGGREGATION"  # Consolidación distribuida de métricas


@dataclass
class Task:
    task_id: str
    task_type: str
    payload: Dict[str, Any]
    priority: str = TaskPriority.NORMAL.value
    created_at: float = field(default_factory=time.time)
    dispatched_at: Optional[float] = None
    completed_at: Optional[float] = None
    status: str = TaskStatus.PENDING.value
    assigned_worker: Optional[str] = None
    assigned_thread: Optional[str] = None
    result: Optional[Any] = None
    execution_time_ms: float = 0.0
    error_message: Optional[str] = None
    retries: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Task":
        return cls(**data)


@dataclass
class WorkerInfo:
    worker_id: str
    host: str
    port: int
    max_threads: int
    active_threads: int = 0
    state: str = "IDLE"
    cpu_percent: float = 0.0
    completed_tasks: int = 0
    failed_tasks: int = 0
    last_heartbeat: float = field(default_factory=time.time)

    def is_alive(self, timeout_sec: float = 4.0) -> bool:
        return (time.time() - self.last_heartbeat) < timeout_sec

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
