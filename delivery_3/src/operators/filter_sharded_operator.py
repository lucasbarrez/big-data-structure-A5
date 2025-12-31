from operators.base_operator import BaseOperator
from operators.cost_model import CostModel
from utils.size_computer import SizeComputer
from utils.schema_builder import SchemaBuilder
import json
from pathlib import Path

class FilterShardedOperator(BaseOperator):
    def __init__(self, collection, output_keys, filter_key, selectivity=None, sharding_info=None):
        super().__init__(collection, output_keys, filter_key, selectivity)
        self.sharding_info = sharding_info or {"nb_shards": 2, "shard_key": "id", "distribution": "uniform"}

    def run(self):
        # Load all required input files
        schema = self._load_json(Path("../basic_schema.json"))
        stats = self._load_json(Path("../basic_statistic.json"))
        key_sizes_path = Path("../key_sizes.json")
        cost_model_path = Path("../cost_model.json")

        # Read collection statistics
        col_stats = stats["collections"][self.collection]
        n_in = int(col_stats["document_count"])# total input documents
        selectivity = float(self.selectivity or 0.1)# fraction passing the filter

        n_out = int(n_in * selectivity) # estimated output coun

        # Split input/output per shard
        nb_shards = self.sharding_info.get("nb_shards", 2)
        n_in_shard = n_in / nb_shards
        n_out_shard = n_out / nb_shards

         # Compute the average projected document size
        schema_builder = SchemaBuilder(schema)
        dclass = schema_builder.create_all_dataclasses()[self.collection]
        key_sizes = SizeComputer.load_key_sizes(str(key_sizes_path))
        field_specs = col_stats.get("field_specifics", {})
        avg_projected = sum(
            SizeComputer.compute_field_size(dclass.__dataclass_fields__[k].type, k, key_sizes, field_specs.get(k, {}))
            for k in self.output_keys if k in dclass.__dataclass_fields__
        )
        total_size = n_out * avg_projected

        # Compute costs using CostModel
        cm = CostModel.from_file(str(cost_model_path))
        pages = cm.pages_read(n_in, avg_projected)
        io_cost = cm.io_cost(pages)
        cpu_cost = cm.cpu_cost_per_tuple(n_in)
        network_cost = cm.network_cost(total_size / nb_shards)
        total_cost = cm.total_cost(io_cost, cpu_cost, network_cost)

        # Return standardized output result
        return {
            "operator_name": "Filter (with sharding)",
            "collection": self.collection,
            "nb_shards": nb_shards,
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
