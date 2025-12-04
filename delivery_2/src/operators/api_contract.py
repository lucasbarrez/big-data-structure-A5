
class OperatorInput:
    """Common input structure for all operators"""
    def __init__(self, collection_name, output_keys, filter_key, selectivity=None):
        self.collection_name = collection_name
        self.output_keys = output_keys
        self.filter_key = filter_key
        self.selectivity = selectivity or 0.1


class OperatorOutput:
    """Common output structure for all operators"""
    def __init__(self, output_doc_count, output_size_bytes, costs):
        self.output_doc_count = output_doc_count
        self.output_size_bytes = output_size_bytes
        self.costs = costs
