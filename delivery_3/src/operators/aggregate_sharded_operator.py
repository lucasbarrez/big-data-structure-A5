# src/operators/aggregate_sharded_operator.py
from operators.base_operator import BaseOperator
from operators.cost_model import CostModel
from utils.size_computer import SizeComputer
from utils.schema_builder import SchemaBuilder
import json
from pathlib import Path


class AggregateShardedOperator(BaseOperator):
    """
    Simule un Aggregate avec sharding, basé sur les statistiques uniquement.
    """
    def __init__(
        self,
        collection,
        group_keys,
        agg_key,
        output_keys,
        filter_key=None,
        selectivity=None,
        sharding_info=None,
        statistics=None,
        schema_builder=None,
    ):
        super().__init__(collection, output_keys, filter_key, selectivity)
        self.group_keys = group_keys
        self.agg_key = agg_key
        self.sharding_info = sharding_info or {"nb_shards": 2, "shard_key": "id", "distribution": "uniform"}
        self.statistics = statistics
        self.schema_builder = schema_builder

    def run(self):
        # Charger stats et schema
        stats = self.statistics or self._load_json(Path("../basic_statistic.json"))
        schema = self.schema_builder or SchemaBuilder(self._load_json(Path("../basic_schema.json")))

        # Stats de la collection
        col_stats = stats["collections"][self.collection]
        n_in = int(col_stats["document_count"])

        # ------------------------
        # Étape filtre simulé
        # ------------------------
        if self.filter_key:
            sel = float(self.selectivity or 0.1)
            n_in = int(n_in * sel)

        # ------------------------
        # Sharding
        # ------------------------
        nb_shards = self.sharding_info.get("nb_shards", 2)
        n_in_shard = n_in / nb_shards

        # ------------------------
        # Nombre de groupes distincts simulé
        # ------------------------
        distinct_count = n_in
        for key in self.group_keys:
            field_stats = col_stats.get("field_specifics", {}).get(key, {})
            if "distinct_values" in field_stats:
                distinct_count = min(distinct_count, field_stats["distinct_values"])
        distinct_count_shard = distinct_count / nb_shards

        # ------------------------
        # Taille moyenne document output
        # ------------------------
        dataclasses = schema.create_all_dataclasses()
        dclass = dataclasses[self.collection]
        key_sizes = SizeComputer.load_key_sizes("../key_sizes.json")
        avg_doc_size = sum(
            SizeComputer.compute_field_size(dclass.__dataclass_fields__[k].type, k, key_sizes, 
                                            col_stats.get("field_specifics", {}).get(k, {}))
            for k in self.output_keys if k in dclass.__dataclass_fields__
        )
        total_size_bytes = distinct_count * avg_doc_size

        # ------------------------
        # Coût avec CostModel
        # ------------------------
        cm = CostModel.from_file("../cost_model.json")
        pages = cm.pages_read(n_in, avg_doc_size)
        io_cost = cm.io_cost(pages)
        cpu_cost = cm.cpu_cost_per_tuple(n_in)
        # Réseau: si co-localisé sur shards, seulement la somme des résultats
        network_cost = cm.network_cost(total_size_bytes / nb_shards)
        total_cost = cm.total_cost(io_cost, cpu_cost, network_cost)

        return {
            "operator_name": "Aggregate (with sharding)",
            "collection": self.collection,
            "nb_shards": nb_shards,
            "input_doc_count": n_in,
            "distinct_count": distinct_count,
            "avg_document_size_bytes": avg_doc_size,
            "total_size_bytes": int(total_size_bytes),
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
