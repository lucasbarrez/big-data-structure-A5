from operators.base_operator import BaseOperator
from operators.cost_model import CostModel
from utils.size_computer import SizeComputer
from utils.schema_builder import SchemaBuilder
import json
from pathlib import Path


class FilterOperator(BaseOperator):
    def run(self):
        # Load all required input files
        schema = self._load_json(Path("../basic_schema.json"))
        stats = self._load_json(Path("../basic_statistic.json"))
        key_sizes_path = Path("../key_sizes.json")
        cost_model_path = Path("../cost_model.json")

        # Read statistics for the target collection
        col_stats = stats["collections"][self.collection]
        n_in = int(col_stats["document_count"])
        selectivity = float(self.selectivity or 0.1)
        n_out = int(n_in * selectivity)

        # Compute the average projected document size
        schema_builder = SchemaBuilder(schema)
        dclass = schema_builder.create_all_dataclasses()[self.collection]
        key_sizes = SizeComputer.load_key_sizes(str(key_sizes_path))
        field_specs = col_stats.get("field_specifics", {})

        avg_projected = 0
        for key in self.output_keys:
            if key in dclass.__dataclass_fields__:
                avg_projected += SizeComputer.compute_field_size(
                    dclass.__dataclass_fields__[key].type,
                    key,
                    key_sizes,
                    field_specs.get(key, {})
                )

        total_size = n_out * avg_projected

        # Estimate execution costs using CostModel
        cm = CostModel.from_file(str(cost_model_path))
        pages = cm.pages_read(n_in, avg_projected)
        io_cost = cm.io_cost(pages)
        cpu_cost = cm.cpu_cost_per_tuple(n_in)
        network_cost = 0.0
        total_cost = cm.total_cost(io_cost, cpu_cost, network_cost)

        
        return {
            "operator_name": "Filter (no sharding)",
            "collection": self.collection,
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
    
