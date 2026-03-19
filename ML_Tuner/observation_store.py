#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ObservationStore

兼容：
- ObservationStore()
- ObservationStore(csv_path="xx.csv")
- ObservationStore(config_path="ml_tuner_config.yaml")
- ObservationStore(config=config_dict)
- ObservationStore(config_dict=config_dict)
- valid / is_valid
- parts / j_parts
- get_top_k() 返回 theta dict 列表
- mark_last_invalid()
- get_training_data(space)
- save_csv / load_csv
"""

from __future__ import annotations

import ast
import csv
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import yaml

logger = logging.getLogger("ml_tuner.store")


# ============================================================
#  工具函数
# ============================================================

def _safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return float(default)


def _safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return int(default)


def _safe_bool(x, default=False):
    if isinstance(x, bool):
        return x
    if x is None:
        return default
    if isinstance(x, (int, np.integer)):
        return bool(x)
    s = str(x).strip().lower()
    if s in {"1", "true", "yes", "y", "t"}:
        return True
    if s in {"0", "false", "no", "n", "f"}:
        return False
    return default


def _json_default(obj):
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    return str(obj)


def _safe_json_dumps(obj) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=_json_default)
    except Exception:
        return json.dumps(str(obj), ensure_ascii=False)


def _safe_json_loads(s, default=None):
    if default is None:
        default = {}
    if s is None:
        return default
    if isinstance(s, (dict, list)):
        return s
    if not isinstance(s, str):
        return default
    text = s.strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except Exception:
        pass
    try:
        return ast.literal_eval(text)
    except Exception:
        return default


def _load_yaml_config(path: Optional[str]) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data or {}
    except Exception as e:
        logger.warning("加载配置文件失败: %s", e)
        return {}


def _nested_get(d: Dict[str, Any], path, default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


# ============================================================
#  ObservationRecord
# ============================================================

@dataclass
class ObservationRecord:
    theta: Dict[str, Any]
    features: Dict[str, Any]
    j_total: float
    j_parts: Dict[str, float] = field(default_factory=dict)
    is_valid: bool = True
    reason: str = ""
    phase: str = ""
    round_id: int = -1
    timestamp: float = field(default_factory=time.time)

    # ---------- 兼容旧字段 ----------

    @property
    def valid(self) -> bool:
        return self.is_valid

    @valid.setter
    def valid(self, value: bool):
        self.is_valid = bool(value)

    @property
    def parts(self) -> Dict[str, float]:
        return self.j_parts

    @parts.setter
    def parts(self, value: Dict[str, float]):
        self.j_parts = dict(value or {})

    # ---------- 序列化 ----------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "round_id": self.round_id,
            "phase": self.phase,
            "theta": self.theta,
            "features": self.features,
            "j_total": self.j_total,
            "j_parts": self.j_parts,
            "is_valid": self.is_valid,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ObservationRecord":
        theta = _safe_json_loads(data.get("theta"), default={})
        features = _safe_json_loads(data.get("features"), default={})
        j_parts = _safe_json_loads(
            data.get("j_parts", data.get("parts", "{}")),
            default={},
        )
        return cls(
            theta=theta,
            features=features,
            j_total=_safe_float(data.get("j_total", float("inf")), default=float("inf")),
            j_parts=j_parts,
            is_valid=_safe_bool(data.get("is_valid", data.get("valid", True)), default=True),
            reason=str(data.get("reason", "")),
            phase=str(data.get("phase", "")),
            round_id=_safe_int(data.get("round_id", -1), default=-1),
            timestamp=_safe_float(data.get("timestamp", time.time()), default=time.time()),
        )


# ============================================================
#  ObservationStore
# ============================================================

class ObservationStore:
    """观测存储"""

    def __init__(
        self,
        csv_path: Optional[str] = None,
        autosave: Optional[bool] = None,
        config_path: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        config_dict: Optional[Dict[str, Any]] = None,
    ):
        cfg = config or config_dict or _load_yaml_config(config_path)

        cfg_csv_path = None
        for path in [
            ("storage", "csv_path"),
            ("observation_store", "csv_path"),
            ("output", "csv_path"),
            ("store", "csv_path"),
        ]:
            cfg_csv_path = _nested_get(cfg, path, default=None)
            if cfg_csv_path is not None:
                break

        cfg_autosave = None
        for path in [
            ("storage", "autosave"),
            ("observation_store", "autosave"),
            ("store", "autosave"),
        ]:
            cfg_autosave = _nested_get(cfg, path, default=None)
            if cfg_autosave is not None:
                break

        self.csv_path = csv_path if csv_path is not None else cfg_csv_path
        self.autosave = bool(autosave) if autosave is not None else bool(cfg_autosave or False)

        self.records: List[ObservationRecord] = []
        self.j_best: Optional[float] = None
        self.theta_best: Optional[Dict[str, Any]] = None
        self.best_record: Optional[ObservationRecord] = None

        logger.info(
            "ObservationStore 初始化: csv_path=%s, autosave=%s",
            self.csv_path,
            self.autosave,
        )

    # ---------- 基础 ----------

    def __repr__(self):
        return (
            f"ObservationStore(records={len(self.records)}, "
            f"valid={self.count_valid()}, invalid={self.count_invalid()}, "
            f"best={self.j_best})"
        )

    def __len__(self):
        return len(self.records)

    def clear(self):
        self.records.clear()
        self.j_best = None
        self.theta_best = None
        self.best_record = None

    # ---------- best 维护 ----------

    def _update_best(self, record: ObservationRecord):
        if not record.is_valid:
            return
        try:
            j = float(record.j_total)
        except Exception:
            return
        if np.isnan(j):
            return
        if self.j_best is None or j < self.j_best:
            self.j_best = j
            self.theta_best = dict(record.theta)
            self.best_record = record

    def _rebuild_best(self):
        self.j_best = None
        self.theta_best = None
        self.best_record = None
        for r in self.records:
            self._update_best(r)

    # ---------- 写入 ----------

    def add(
        self,
        theta: Dict[str, Any],
        features: Optional[Dict[str, Any]],
        j_total: float,
        j_parts: Optional[Dict[str, float]] = None,
        parts: Optional[Dict[str, float]] = None,
        valid: Optional[bool] = None,
        is_valid: Optional[bool] = None,
        reason: str = "",
        phase: str = "",
        round_id: int = -1,
        timestamp: Optional[float] = None,
    ) -> ObservationRecord:
        if j_parts is None and parts is not None:
            j_parts = parts
        if is_valid is None:
            is_valid = True if valid is None else bool(valid)

        record = ObservationRecord(
            theta=dict(theta or {}),
            features=dict(features or {}),
            j_total=float(j_total),
            j_parts=dict(j_parts or {}),
            is_valid=bool(is_valid),
            reason=str(reason or ""),
            phase=str(phase or ""),
            round_id=int(round_id),
            timestamp=float(timestamp if timestamp is not None else time.time()),
        )

        self.records.append(record)
        self._update_best(record)

        if self.autosave:
            try:
                self.save_csv()
            except Exception as e:
                logger.warning("autosave 失败: %s", e)

        return record

    def mark_invalid(
        self,
        theta: Dict[str, Any],
        features: Optional[Dict[str, Any]] = None,
        j_total: float = float("inf"),
        j_parts: Optional[Dict[str, float]] = None,
        parts: Optional[Dict[str, float]] = None,
        reason: str = "",
        phase: str = "",
        round_id: int = -1,
    ) -> ObservationRecord:
        if j_parts is None and parts is not None:
            j_parts = parts
        return self.add(
            theta=theta,
            features=features or {},
            j_total=j_total,
            j_parts=j_parts or {},
            is_valid=False,
            reason=reason,
            phase=phase,
            round_id=round_id,
        )

    def mark_last_invalid(self, reason: str = "") -> Optional[ObservationRecord]:
        """
        将最后一条记录标记为无效。
        test_step2 会调用此方法。
        """
        if not self.records:
            return None

        last = self.records[-1]
        last.is_valid = False
        last.reason = str(reason or "")

        # 如果被标记的恰好是当前最优，需要重建
        if self.best_record is last:
            self._rebuild_best()

        return last

    # ---------- 计数 ----------

    def count_total(self) -> int:
        return len(self.records)

    def count_valid(self) -> int:
        return sum(1 for r in self.records if r.is_valid)

    def count_invalid(self) -> int:
        return sum(1 for r in self.records if not r.is_valid)

    # ---------- 查询 ----------

    def get_records(self, valid_only: Optional[bool] = None) -> List[ObservationRecord]:
        if valid_only is None:
            return list(self.records)
        if valid_only:
            return [r for r in self.records if r.is_valid]
        return [r for r in self.records if not r.is_valid]

    def get_latest(self, valid_only: Optional[bool] = None) -> Optional[ObservationRecord]:
        records = self.get_records(valid_only=valid_only)
        return records[-1] if records else None

    def get_best(
        self,
        valid_only: bool = True,
    ) -> Tuple[Optional[float], Optional[Dict[str, Any]], Optional[ObservationRecord]]:
        if valid_only:
            if self.best_record is None:
                return None, None, None
            return self.j_best, dict(self.theta_best), self.best_record

        if not self.records:
            return None, None, None

        best = min(self.records, key=lambda r: float(r.j_total))
        return float(best.j_total), dict(best.theta), best

    def get_top_k(
        self,
        k: int = 5,
        valid_only: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        返回 J 最小的前 k 条的 theta（dict 列表）。

        bayesian_optimizer.py 第 128 行：
            top_k_thetas = store.get_top_k(5)
            top_k_norms = [self.space.normalize(t) for t in top_k_thetas]

        所以必须返回 dict 列表，不能返回 ObservationRecord 列表。
        """
        records = self.get_records(valid_only=valid_only)
        sorted_records = sorted(records, key=lambda r: float(r.j_total))
        return [dict(r.theta) for r in sorted_records[:k]]

    def get_tried_thetas(self, valid_only: bool = False) -> List[Dict[str, Any]]:
        return [dict(r.theta) for r in self.get_records(valid_only=valid_only)]

    def has_theta(self, theta: Dict[str, Any], valid_only: bool = False) -> bool:
        target = dict(theta or {})
        for r in self.get_records(valid_only=valid_only):
            if dict(r.theta) == target:
                return True
        return False

    # ---------- 训练数据导出 ----------

    def _infer_param_names(self, space=None) -> List[str]:
        if space is not None:
            for attr in ("param_names", "parameter_names", "names"):
                if hasattr(space, attr):
                    names = getattr(space, attr)
                    if isinstance(names, (list, tuple)) and len(names) > 0:
                        return list(names)

            for attr in ("params", "param_space", "space"):
                if hasattr(space, attr):
                    obj = getattr(space, attr)
                    if isinstance(obj, dict) and obj:
                        return list(obj.keys())

            if hasattr(space, "get_defaults"):
                try:
                    d = space.get_defaults()
                    if isinstance(d, dict) and d:
                        return list(d.keys())
                except Exception:
                    pass

        valid_records = self.get_records(valid_only=True)
        if valid_records:
            return list(valid_records[0].theta.keys())

        if self.records:
            return list(self.records[0].theta.keys())

        return []

    def get_training_data(self, space=None, valid_only: bool = True):
        """
        返回:
            X: np.ndarray [n, d]
            y: np.ndarray [n]
        """
        records = self.get_records(valid_only=valid_only)
        param_names = self._infer_param_names(space)

        if not records:
            dim = max(len(param_names), 1)
            return np.zeros((0, dim), dtype=float), np.zeros((0,), dtype=float)

        X = []
        y = []

        for r in records:
            vec = None

            if space is not None and hasattr(space, "normalize"):
                try:
                    norm = space.normalize(r.theta)
                    if isinstance(norm, dict):
                        if not param_names:
                            param_names = list(norm.keys())
                        vec = [float(norm.get(k, 0.0)) for k in param_names]
                    elif isinstance(norm, (list, tuple, np.ndarray)):
                        vec = list(np.asarray(norm, dtype=float).reshape(-1))
                except Exception:
                    vec = None

            if vec is None:
                if not param_names:
                    param_names = list(r.theta.keys())
                vec = [float(r.theta.get(k, 0.0)) for k in param_names]

            X.append(vec)
            y.append(float(r.j_total))

        return np.asarray(X, dtype=float), np.asarray(y, dtype=float)

    # ---------- baseline ----------

    def estimate_throughput_baseline(
        self,
        metric_key_candidates=(
            "avg_input_rate",
            "throughput",
            "input_rate",
            "source_rate",
        ),
        reducer: str = "max",
    ) -> Optional[float]:
        vals = []
        for r in self.get_records(valid_only=True):
            feats = r.features or {}
            for k in metric_key_candidates:
                if k in feats:
                    try:
                        vals.append(float(feats[k]))
                    except Exception:
                        pass
                    break

        if not vals:
            return None

        if reducer == "median":
            return float(np.median(vals))
        return float(np.max(vals))

    # ---------- CSV 持久化 ----------

    _CSV_FIELDS = [
        "timestamp",
        "round_id",
        "phase",
        "theta",
        "features",
        "j_total",
        "j_parts",
        "is_valid",
        "reason",
    ]

    def save_csv(self, path: Optional[str] = None):
        path = path or self.csv_path
        if not path:
            return

        folder = os.path.dirname(path)
        if folder:
            os.makedirs(folder, exist_ok=True)

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self._CSV_FIELDS)
            writer.writeheader()

            for r in self.records:
                writer.writerow({
                    "timestamp": r.timestamp,
                    "round_id": r.round_id,
                    "phase": r.phase,
                    "theta": _safe_json_dumps(r.theta),
                    "features": _safe_json_dumps(r.features),
                    "j_total": r.j_total,
                    "j_parts": _safe_json_dumps(r.j_parts),
                    "is_valid": int(r.is_valid),
                    "reason": r.reason,
                })

    def load_csv(self, path: Optional[str] = None, append: bool = False) -> int:
        path = path or self.csv_path
        if not path or not os.path.exists(path):
            return 0

        loaded: List[ObservationRecord] = []

        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    loaded.append(ObservationRecord.from_dict(row))
                except Exception as e:
                    logger.warning("跳过坏记录: %s | row=%s", e, row)

        if append:
            self.records.extend(loaded)
        else:
            self.records = loaded

        self._rebuild_best()
        return len(loaded)

    # 在类内部，紧跟在 save_csv / load_csv 后面

    _DEFAULT_CSV = "ml_tuner_observations.csv"

    def save(self, path: Optional[str] = None):
        """save_csv 的别名，csv_path 为 None 时自动使用默认路径"""
        if path is None and self.csv_path is None:
            self.csv_path = self._DEFAULT_CSV
        return self.save_csv(path)

    def load(self, path: Optional[str] = None, append: bool = False) -> int:
        """load_csv 的别名，csv_path 为 None 时自动使用默认路径"""
        if path is None and self.csv_path is None:
            self.csv_path = self._DEFAULT_CSV
        return self.load_csv(path, append)

    def get_throughput_baseline(self, **kwargs):
        """estimate_throughput_baseline 的别名"""
        return self.estimate_throughput_baseline(**kwargs)

    def get_last(self, valid_only: Optional[bool] = None) -> Optional[ObservationRecord]:
        """get_latest 的别名"""
        return self.get_latest(valid_only=valid_only)




