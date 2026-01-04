from typing import Optional
from utils.size_computer import SizeComputer
from utils.schema_builder import SchemaBuilder


class AggregateOperator:
    """
    Aggregate operator that works based on statistics only, 
    without generating all documents.
    """
    def __init__(
        self,
        collection: str,
        group_keys: list,
        agg_key: str,
        output_keys: list,
        statistics: dict,
        schema_builder: Optional[SchemaBuilder] = None,
        filter_key: Optional[str] = None
    ):
        self.collection = collection
        self.group_keys = group_keys
        self.agg_key = agg_key
        self.output_keys = output_keys
        self.statistics = statistics
        self.schema_builder = schema_builder
        self.filter_key = filter_key

    def run(self):
        col_stats = self.statistics["collections"].get(self.collection, {})
        doc_count = col_stats.get("document_count", 0)
        n_in = int(col_stats["document_count"])

        if self.filter_key:
            # est-ce qu'on doit adapter la selectivity en fonction du filter_key??
            selectivity = 0.1
            n_in = int(n_in * selectivity)
        
        # Estimation du nombre de groupes distincts
        # Pour simplifier, on peut utiliser le doc_count / occurrence moyenne
        distinct_count = n_in
        for key in self.group_keys:
            field_stats = col_stats.get("field_specifics", {}).get(key, {})
            if "distinct_values" in field_stats:
                distinct_count = min(distinct_count, field_stats["distinct_values"])

        avg_doc_size = 0
        for key in self.output_keys:
            field_stats = col_stats.get("field_specifics", {}).get(key, {})
            # Taille simulée du champ
            avg_doc_size += field_stats.get("avg_length", 10)

        total_size_bytes = distinct_count * avg_doc_size


        return {
            "operator_name": "Aggregate (stats-only)",
            "collection": self.collection,
            "input_doc_count": n_in,
            "distinct_count": distinct_count,
            "avg_document_size_bytes": avg_doc_size,
            "total_size_bytes": total_size_bytes

        }