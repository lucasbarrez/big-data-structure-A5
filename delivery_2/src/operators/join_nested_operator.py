from operators.base_operator import BaseOperator
from operators.cost_model import CostModel
from utils.size_computer import SizeComputer
from utils.schema_builder import SchemaBuilder
import json
from pathlib import Path

class NestedLoopJoinOperator(BaseOperator):
    def __init__(self, left_collection, right_collection, join_key, output_keys, selectivity=None):
        super().__init__(left_collection, output_keys, join_key, selectivity)
        self.right_collection = right_collection

    def run(self):
        # Load all required input files
        schema = self._load_json(Path("../basic_schema.json"))
        stats = self._load_json(Path("../basic_statistic.json"))
        key_sizes_path = Path("../key_sizes.json")
        cost_model_path = Path("../cost_model.json")

        # Read statistics for both collections involved in the join
        left_stats = stats["collections"][self.collection]
        right_stats = stats["collections"][self.right_collection]
        n_left = int(left_stats["document_count"])
        n_right = int(right_stats["document_count"])
        # Estimate output size using join selectivity (percentage of matching pairs)
        join_selectivity = float(self.selectivity or 0.001)
        n_out = int(n_left * n_right * join_selectivity)

        
        # Compute the average joined document size,left + right document sizes
        schema_builder = SchemaBuilder(schema)
        all_classes = schema_builder.create_all_dataclasses()
        left_class = all_classes[self.collection]
        right_class = all_classes[self.right_collection]

        key_sizes = SizeComputer.load_key_sizes(str(key_sizes_path))
        left_avg = SizeComputer.compute_dataclass_size(left_class, key_sizes)
        right_avg = SizeComputer.compute_dataclass_size(right_class, key_sizes)
        avg_join_size = left_avg + right_avg
        total_size = n_out * avg_join_size

        # Compute costs using the CostModel
        cm = CostModel.from_file(str(cost_model_path))
        io_cost = cm.io_cost(cm.pages_read(n_left, left_avg) + cm.pages_read(n_right, right_avg))
        cpu_cost = cpu_cost = cm.cpu_cost_comparisons(n_left * n_right)

        network_cost = 0
        total_cost = cm.total_cost(io_cost, cpu_cost, network_cost)

        # Return standardized output result
        return {
            "operator_name": "Nested Loop Join (no sharding)",
            "left_collection": self.collection,
            "right_collection": self.right_collection,
            "join_key": self.filter_key,
            "output_doc_count": n_out,
            "output_size_bytes": int(total_size),
            "costs": {
                "cpu_cost": cpu_cost,
                "io_cost": io_cost,
                "network_cost": network_cost,
                "total_cost": total_cost
            }
        }

    def _load_json(self, path: Path):
        if not path.exists():
            raise FileNotFoundError(f"Missing file: {path}")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
