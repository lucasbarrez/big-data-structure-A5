
import json, math
from pathlib import Path


_DEFAULTS = {
    "page_size": 4096,
    "page_cost": 0.01,
    "cpu_per_tuple": 0.001,
    "cpu_per_comp": 0.001,
    "net_cost_per_byte": 0.00001,
}

class CostModel:
    def __init__(self, cfg: dict):
        self.page_size = cfg.get("page_size", _DEFAULTS["page_size"])
        self.page_cost = cfg.get("page_cost", _DEFAULTS["page_cost"])
        self.cpu_per_tuple = cfg.get("cpu_per_tuple", _DEFAULTS["cpu_per_tuple"])
        self.cpu_per_comp = cfg.get("cpu_per_comp", _DEFAULTS["cpu_per_comp"])
        self.net_cost_per_byte = cfg.get("net_cost_per_byte", _DEFAULTS["net_cost_per_byte"])

    @classmethod
    def from_file(cls, path: str | None):
         # Load configuration from file or use defaults
        if path and Path(path).exists():
            with open(path, "r") as f:
                return cls(json.load(f))
        return cls(_DEFAULTS)

    def pages_read(self, n_in: int, avg_doc_size_bytes: float) -> int:
        # Estimate the number of pages read
        if n_in <= 0 or avg_doc_size_bytes <= 0:
            return 0
        return max(1, math.ceil((n_in * avg_doc_size_bytes) / self.page_size))

    def io_cost(self, pages_read: int) -> float:
        # Estimate disk I/O cost: number of pages * cost per page
        # Represents the effort to read data from storage
        return max(0.0, pages_read * self.page_cost)

    def cpu_cost_per_tuple(self, n_in: int) -> float:
        # Estimate CPU cost for scanning/filtering tuples (documents)
        # Each processed document consumes a small CPU unit
        return max(0.0, n_in * self.cpu_per_tuple)

    def cpu_cost_comparisons(self, n_comp: int) -> float:
        # Estimate CPU cost for performing comparisons (e.g. joins)
        # Used when matching multiple tuples between collections
        return max(0.0, n_comp * self.cpu_per_comp)

    def network_cost(self, bytes_transferred: float) -> float:
        # Estimate cost for transferring data between nodes/shards
        # Depends on the total number of bytes sent over the networ
        return max(0.0, bytes_transferred * self.net_cost_per_byte)

    def total_cost(self, io_cost: float, cpu_cost: float, network_cost: float) -> float:
        # Total estimated cost
        return max(0.0, io_cost + cpu_cost + network_cost)
