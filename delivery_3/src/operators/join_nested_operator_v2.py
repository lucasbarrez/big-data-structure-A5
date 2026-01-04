from utils.size_computer import SizeComputer

class NestedLoopJoinOperator:
    """
    Simule un join nested loop entre deux collections.
    Peut fonctionner soit avec des documents réels, soit en mode 'stats only'.
    """

    def __init__(self, left_collection: str, right_collection: str,
                 left_key: str, right_key: str,
                 left_docs=None, right_docs=None,
                 statistics=None, schema_builder=None):
        self.left_collection = left_collection
        self.right_collection = right_collection
        self.left_key = left_key
        self.right_key = right_key

        self.left_docs = left_docs
        self.right_docs = right_docs

        self.statistics = statistics
        self.schema_builder = schema_builder

    def run(self):
        """Run join with real documents"""
        if self.left_docs is None or self.right_docs is None:
            raise ValueError("Documents must be provided for real join")

        joined = []
        for ldoc in self.left_docs:
            for rdoc in self.right_docs:
                if ldoc[self.left_key] == rdoc[self.right_key]:
                    merged = {**ldoc, **rdoc}
                    joined.append(merged)
        return joined

    def run_simulated(self):
        """
        Simule le join en utilisant uniquement les statistiques.
        Retourne le nombre de résultats et la taille estimée sans créer de documents.
        """
        if self.statistics is None or self.schema_builder is None:
            raise ValueError("Statistics and schema_builder must be provided for simulated join")

        left_stats = self.statistics["collections"].get(self.left_collection, {})
        right_stats = self.statistics["collections"].get(self.right_collection, {})

        left_count = left_stats.get("document_count", 0)
        right_count = right_stats.get("document_count", 0)

        # Approximation : nombre total de résultats = min(left_count, right_count) * facteur de correspondance
        # Si right est agrégé, utiliser distinct_count à la place
        right_distinct = right_stats.get("distinct_count", right_count)

        total_count = min(left_count, right_distinct)

        # Taille moyenne d’un document du join = somme tailles des deux collections
        left_size = SizeComputer.compute_dataclass_size(
            self.schema_builder.create_dataclass_from_collection(self.left_collection),
            key_sizes=SizeComputer.load_key_sizes(),
            field_specifics=left_stats.get("field_specifics", {})
        )
        right_size = SizeComputer.compute_dataclass_size(
            self.schema_builder.create_dataclass_from_collection(self.right_collection),
            key_sizes=SizeComputer.load_key_sizes(),
            field_specifics=right_stats.get("field_specifics", {})
        )

        avg_doc_size = left_size + right_size
        total_size_bytes = total_count * avg_doc_size

        return {
            "total_count": total_count,
            "avg_doc_size": avg_doc_size,
            "total_size_bytes": total_size_bytes
        }
