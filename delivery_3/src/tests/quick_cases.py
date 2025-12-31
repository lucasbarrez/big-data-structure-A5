from pathlib import Path
import json
from operators.filter_operator import FilterOperator
from operators.filter_sharded_operator import FilterShardedOperator
from operators.join_nested_operator import NestedLoopJoinOperator
from operators.join_sharded_operator import NestedLoopJoinShardedOperator
from utils.size_computer import SizeComputer

_BASE = Path(__file__).resolve().parent
_STATS = (_BASE / "../basic_statistic.json").resolve()

def _load_json(p: Path):
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def _get_n_in(collection: str) -> int:
    stats = _load_json(_STATs)
    return int(stats["collections"][collection]["document_count"])

def _avg_projected(result: dict) -> float:
    nout = max(1, int(result.get("output_doc_count", 0)))
    return result.get("output_size_bytes", 0) / nout

def trace_run(title: str, params: dict, result: dict):
    print("\n" + "="*70)
    print(title)
    print("="*70)
    print("Params:", {k: v for k, v in params.items()})
    if "collection" in params:
        try:
            print(f"N_in: {_get_n_in(params['collection'])}")
        except: pass
    if all(k in params for k in ("left_collection", "right_collection")):
        try:
            n_left = _get_n_in(params["left_collection"])
            n_right = _get_n_in(params["right_collection"])
            print(f"N_left: {n_left} | N_right: {n_right}")
        except: pass
    n_out = result.get("output_doc_count", 0)
    avg_sz = _avg_projected(result)
    print(f"N_out: {n_out}")
    print(f"avg_doc_size: {avg_sz:.2f} B")
    total_sz = result.get("output_size_bytes", 0)
    print(f"output_size: {total_sz} bytes")
    c = result.get("costs", {})
    print("Costs:", f"io={c.get('io_cost',0)}",
          f"cpu={c.get('cpu_cost',0)}",
          f"net={c.get('network_cost',0)}",
          f"total={c.get('total_cost',0)}")

def req_filter_eq_no_sharding(selectivity: float = 0.05):
    params = {"operator": "Filter (no sharding)", "collection": "Product",
              "output_keys": ["name", "price"], "filter_key": "brand",
              "selectivity": selectivity}
    res = FilterOperator("Product", ["name", "price"], "brand", selectivity).run()
    trace_run("REQ — Filter '=' (no sharding)", params, res)
    return res

def req_filter_between_sharded_aligned(selectivity: float = 0.15):
    params = {"operator": "Filter (with sharding)", "collection": "Stock",
              "output_keys": ["quantity", "location"], "filter_key": "IDW",
              "selectivity": selectivity,
              "sharding_info": {"nb_shards": 4, "shard_key": "IDW", "distribution": "uniform"}}
    res = FilterShardedOperator("Stock", ["quantity", "location"], "IDW",
                                selectivity, params["sharding_info"]).run()
    trace_run("REQ — Filter 'range/BETWEEN' (with sharding, aligned)", params, res)
    return res

def req_nlj_no_sharding_small(join_selectivity: float = 0.02):
    params = {"operator": "Nested Loop Join (no sharding)", "left_collection": "Stock",
              "right_collection": "Product", "join_key": "IDP",
              "output_keys": ["name", "quantity"], "selectivity": join_selectivity}
    res = NestedLoopJoinOperator("Stock", "Product", "IDP", ["name", "quantity"],
                                 join_selectivity).run()
    trace_run("REQ — NLJ (no sharding)", params, res)
    return res

def req_nlj_sharded_compare(join_selectivity: float = 0.001):
    params_local = {"operator": "NLJ (with sharding, co-located)",
                    "left_collection": "Product", "right_collection": "Stock",
                    "join_key": "IDP",
                    "output_keys": ["name", "price", "IDW", "quantity"],
                    "selectivity": join_selectivity,
                    "sharding_info": {"nb_shards": 4, "shard_key": "IDP", "distribution": "uniform"}}
    res_local = NestedLoopJoinShardedOperator("Product", "Stock", "IDP",
                    ["name", "price", "IDW", "quantity"],
                    join_selectivity, params_local["sharding_info"]).run()
    trace_run("REQ — NLJ (with sharding, co-located)", params_local, res_local)

    params_non = {**params_local, "operator": "NLJ (with sharding, non co-located)",
                  "sharding_info": {"nb_shards": 4, "shard_key": "IDW", "distribution": "uniform"}}
    res_non = NestedLoopJoinShardedOperator("Product", "Stock", "IDP",
                    ["name", "price", "IDW", "quantity"],
                    join_selectivity, params_non["sharding_info"]).run()
    trace_run("REQ — NLJ (with sharding, non co-located)", params_non, res_non)

    print("\n>> network_cost comparison:",
          "co-located =", res_local["costs"]["network_cost"],
          "| non co-located =", res_non["costs"]["network_cost"])
    return {"co_located": res_local, "non_co_located": res_non}

