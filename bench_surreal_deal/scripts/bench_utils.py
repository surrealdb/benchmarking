from random import Random
from uuid import UUID
import time
import math

import numpy as np
from tabulate import tabulate
from surrealdb import SurrealDB

table_definition = {
    "person_amount": 1000,
    "product_amount": 1000,
    "order_amount":10000,
    "artist_amount": 500,
    "review_amount":2000
}

def insert_relate_statement(table_data:list[dict], db_connection=SurrealDB("ws://localhost:8000/test/test")) -> str:
    """
    Inserting data through relate statement
    """
    db = db_connection

    table_record_id = -1
    for record in table_data:
        table_record_id += 1
        db.query(
    f"RELATE {table_data[table_record_id]['in']} -> {table_data[table_record_id]['id']} -> {table_data[table_record_id]['out']} CONTENT {record};"
            )

def generate_uuid4(amount, seed=42):
    """Yields a generator object of pseudorandom uuids"""
    rnd = Random()
    rnd.seed(seed)
    return (UUID(int=rnd.getrandbits(128), version=4) for _ in range(amount))

def generate_random_number_list(total_num, list_num, seed=42):
    """Returns a list of unique pseudorandomumbers"""
    rnd = Random()
    rnd.seed(seed)
    rnd_num_list = []
    while len(rnd_num_list) <= list_num:
        number = rnd.randint(0, total_num) 
        if number not in rnd_num_list:
            rnd_num_list.append(number)
    sorted_rnd_num_list = sorted(rnd_num_list)
    return sorted_rnd_num_list

def get_gen_uuid4_unique_list(total_num, list_num, seed=42):
    """Returns 10 randomly chosen uuids from a pseudorandom generator"""
    # Generating the uuids
    uuid_gen = generate_uuid4(total_num, seed)

    # Generating a list of unique random numbers
    num_list = generate_random_number_list(total_num, list_num, seed)
    
    # gathering uudids into a list
    num = 0
    uuid_list = []
    for _ in range(total_num):
        if num in num_list:
            uuid_list.append(next(uuid_gen))
            num +=1
        else:
            next(uuid_gen)
            num +=1
    return uuid_list


def format_time(raw_time, unit="ms", precision=0, unit_label=False):
    """
    Takes in time in nanoseconds
    Returns formatted time in selected unit
    """

    units = {"s": 1e+9, "ms": 1e+6, "us": 1e+3, "ns": 1}

    selected_unit = units[unit]

    converted_time = raw_time / selected_unit

    if unit_label == False:
        if int(converted_time) == converted_time:
            formatted_time = converted_time
        elif precision==0:
            formatted_time = int(round(converted_time, precision))
        else:
            formatted_time = round(converted_time, precision)

    if unit_label == True:
        if int(converted_time) == converted_time:
            formatted_time = f"{converted_time} {unit}"
        elif precision==0:
            formatted_time = f"{int(round(converted_time, precision))} {unit}"
        else:
            formatted_time = f"{round(converted_time, precision)} {unit}"

    return formatted_time

def format_time_list(raw_time_list, unit="ms", precision=0, unit_label=False):
    """
    Takes in a list of time in nanoseconds
    Returns a list of formatted time in selected unit
    """
    return [format_time(raw_time, unit=unit, precision=precision, unit_label=unit_label) for raw_time in raw_time_list]


def calculate_latency_metrics(result_list, unit="ms"):
    """
    Takes in a list of integers and calculates latency metrics
    """
    sorted_list = sorted(result_list)
    number_of_results = len(result_list)-1 # accounting for zero-based indexing
    
    # // same as using math floor
    latency_metrics = {
        f"min({unit})": format_time(min(result_list), unit=unit),
        f"p1({unit})": format_time(sorted_list[(number_of_results * 1) // 100], unit=unit),
        f"p5({unit})": format_time(sorted_list[(number_of_results * 5) // 100], unit=unit),
        f"p25({unit})": format_time(sorted_list[(number_of_results * 25) // 100], unit=unit),
        f"p50({unit})": format_time(sorted_list[(number_of_results * 50) // 100], unit=unit),
        f"p75({unit})": format_time(sorted_list[(number_of_results * 75) // 100], unit=unit),
        f"p90({unit})": format_time(sorted_list[(number_of_results * 90) // 100], unit=unit),
        f"p99({unit})": format_time(sorted_list[(number_of_results * 99) // 100], unit=unit),
        f"max({unit})": format_time(max(result_list), unit=unit)
    }
    return latency_metrics

def throughput_calc(query_count, duration, unit="s", precision=2):
    throughput = query_count / format_time(duration, unit=unit, precision=precision)
    return throughput

# foundation for plot_vals and plot_box adapted from here:
# https://gitlab.com/Soha/termbox.py
def plot_vals(vals, step, ticks, out):
    i = -1
    v = ticks[0]
    box_plot = ""
    while v < ticks[-1]:
        if i == -1:
            if v < out[0] and v + step > out[0]:
                box_plot += "+"
            elif v > vals[0]:
                box_plot += "|"
            else:
                box_plot += " "
        elif i == 0:
            box_plot += "-"
        elif i < 3:
            if v < vals[2] and v + step > vals[2]:  # Median is only one point
                box_plot += ":"
            else:
                box_plot += "="
        elif i == 3:
            if v >= vals[4] or v + step > ticks[-1]:
                box_plot += "|"
            else:
                box_plot += "-"
        if i == 4:
            if v < out[1] and v + step > out[1]:
                box_plot += "+"
            else:
                box_plot += " "
        elif v > vals[i + 1]:
            i += 1

        v += step
    return box_plot

def plot_box(data, nticks=5):
    box = [5, 25, 50, 75, 95]
    df = np.array(data, dtype=np.float64)
    vals = np.quantile(df, [x / 100 for x in box], method="lower")
    out = np.quantile(df, [0.01, 0.99], method="lower")

    lo, hi = (df.min(), df.max())

    ticks = np.unique(np.linspace(lo, math.ceil(hi), nticks, dtype=int))
    # The step is the *width* of a character
    step = (ticks.max() - ticks.min()) / 50

    return plot_vals(vals, step, ticks, out)

def create_markdown_metrics_table(SDB_result, MDB_result, unit="ms"):
    SDB_metrics = calculate_latency_metrics(SDB_result, unit=unit)
    MDB_metrics = calculate_latency_metrics(MDB_result, unit=unit)

    SDB_metrics_list = list(SDB_metrics.values())
    MDB_metrics_list = list(MDB_metrics.values())
    reshaped_dict = {
        "Metric": list(SDB_metrics.keys()),
        "SurrealDB": SDB_metrics_list,
        "MongoDB": MDB_metrics_list,
        "Difference": [round((((mdb - sdb) / sdb) * 100), 1) for sdb, mdb in zip(SDB_metrics_list, MDB_metrics_list)]
    }
    table = tabulate(reshaped_dict, headers="keys", tablefmt="github")
    return table

def run_surrealdb_query(operation, query, iterations=1):
    """
    Run templated surrealdb queries
    """
    result_list = []
    for _ in range(iterations):
        start_time = time.perf_counter_ns()

		# query to be run
        operation(query)

        end_time = time.perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    return result_list


def run_mongo_query(operation, pipeline, iterations=1):
    """
    Run templated pymongo queries
    only works for certain queries, queries done individually in benchmark
    just thought I'd keep it for if need it later.
    """
    result_list = []
    for _ in range(iterations):
        start_time = time.perf_counter_ns()

		# query to be run
        list(operation(pipeline))

        end_time = time.perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    return result_list