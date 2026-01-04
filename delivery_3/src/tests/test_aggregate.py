
from operators.aggregate_operator_v2 import AggregateOperator
from operators.filter_operator import FilterOperator
from operators.join_nested_operator_v2 import NestedLoopJoinOperator
from operators.aggregate_sharded_operator import AggregateShardedOperator
from operators.join_sharded_operator import NestedLoopJoinShardedOperator
from utils.schema_builder import SchemaBuilder
from utils.size_computer import SizeComputer
import json

def Q6_aggregate_only( stats, builder: SchemaBuilder):
    # ------------------------
    # Aggregate sur OrderLine
    # ------------------------
    agg_orderline_op = AggregateOperator(
        collection="OrderLine",
        group_keys=["product_id"],  # correspond à IDP
        agg_key="quantity",
        output_keys=["product_id", "NB"],  # NB = somme
        statistics=stats,
        schema_builder=builder
    )

    agg_result = agg_orderline_op.run()
    #agg_docs = agg_result["output_documents"]  # documents simulés, valeurs None

    print("=== Aggregate OrderLine ===")
    print("Nombre de groupes :", agg_result["distinct_count"])
    print("Taille moyenne document (bytes) :", agg_result["avg_document_size_bytes"])
    print("Taille totale (bytes) :", agg_result["total_size_bytes"])

    # ------------------------
    # Join simulé avec Product
    # ------------------------
    # Estimation basée sur stats, pas de documents
    product_stats = stats["collections"]["Product"]
    product_doc_count = product_stats["document_count"]
    orderline_groups = agg_result["distinct_count"]

    join_op = NestedLoopJoinOperator(
        left_collection="Product",
        right_collection="OrderLine",
        left_key="id",       # IDP dans Product
        right_key="product_id",
        schema_builder=builder,
        statistics=stats
    )

    joined_docs = join_op.run_simulated()

    print("=== Join simulé Product x OrderLine ===")
    print(f"Nombre de résultats join : {joined_docs['total_count']}")
    print(f"Taille moyenne doc join (bytes) : {joined_docs['avg_doc_size']}")
    print(f"Taille totale join (bytes) : {joined_docs['total_size_bytes']}")

def Q7_aggregate_only(stats, builder: SchemaBuilder):

    # ------------------------
    # Filtrer OrderLine sur client_id=125
    # ------------------------
    filter_op = FilterOperator(
        collection="OrderLine",
        output_keys=["product_id", "quantity"],
        selectivity=0.05  # fraction approx des docs qui passent le filtre
    )
    filter_result = filter_op.run()
    print("=== Filter OrderLine ===")
    print(filter_result)

    # ------------------------
    # Aggregate SUM(quantity) par product_id
    # ------------------------
    agg_op = AggregateOperator(
        collection="OrderLine",
        group_keys=["product_id"],
        agg_key="quantity",
        output_keys=["product_id", "NB"],
        statistics=stats,
        schema_builder=builder,
        filter_key={"client_id": 125}  # simule le WHERE !! pas sûre de la bonne implémentation !!
    )
    agg_result = agg_op.run()
    print("=== Aggregate OrderLine (client_id=125) ===")
    print(agg_result)

    # ------------------------
    # Join avec Product
    # ------------------------
    join_op = NestedLoopJoinOperator(
        left_collection="Product",
        right_collection="OrderLine",
        left_key="id",
        right_key="product_id",
        statistics=stats,
        schema_builder=builder
    )
    join_result = join_op.run_simulated()

    # ------------------------
    # Tri DESC et LIMIT 1
    # ------------------------
    total_count = min(join_result["total_count"], 1)
    avg_doc_size = join_result["avg_doc_size"]
    total_size_bytes = total_count * avg_doc_size

    print("=== Join + tri DESC NB + LIMIT 1 ===")
    print("Nombre de résultats :", total_count)
    print("Taille moyenne doc (bytes) :", avg_doc_size)
    print("Taille totale join (bytes) :", total_size_bytes)


def Q6_sharded_aggregate(stats, builder):
    agg_orderline_op = AggregateShardedOperator(
        collection="OrderLine",
        group_keys=["product_id"],  # IDP
        agg_key="quantity",
        output_keys=["product_id", "NB"],  # NB = somme
        statistics=stats,
        schema_builder=builder,
        sharding_info={"nb_shards": 2, "shard_key": "product_id", "distribution": "uniform"},
        
    )

    agg_result = agg_orderline_op.run()
    print("=== Aggregate OrderLine (sharded) ===")
    print("Nombre de groupes :", agg_result["distinct_count"])
    print("Taille moyenne doc :", agg_result["avg_document_size_bytes"])
    print("Taille totale :", agg_result["total_size_bytes"])
    print("Coût total :", agg_result["costs"]["total_cost"])

    # ------------------------
    # 2) Join Sharded avec Product
    # ------------------------
    join_op = NestedLoopJoinShardedOperator(
        left_collection="Product",
        right_collection="OrderLine",
        join_key="id",        # IDP dans Product
        output_keys=["name", "price", "NB"],
        selectivity=1.0,      # join complet
        sharding_info={"nb_shards": 2, "shard_key": "id", "distribution": "uniform"},
        
    )

    joined_result = join_op.run()  # passer l'agg_result comme entrée

    print("=== Join Product x OrderLine (sharded) ===")
    print(joined_result)
    


if __name__ == "__main__":
    # Charger le schema et les statistiques
    with open("../basic_schema.json") as f:
        schema_json = json.load(f)
    with open("../basic_statistic.json") as f:
        stats = json.load(f)

    builder = SchemaBuilder(schema_json)
    # print("Schema loaded and dataclasses created.")
    # print("-----------------------------------")
    # print("\nRunning Q6: Aggregate only")
    # print("-----------------------------------")
    # Q6_aggregate_only(stats, builder)
    # print("-----------------------------------")
    # print("\nRunning Q7: Aggregate only")
    # print("-----------------------------------")
    # Q7_aggregate_only(stats, builder)

    print("-----------------------------------")
    Q6_sharded_aggregate(stats, builder)
    print("-----------------------------------")
    