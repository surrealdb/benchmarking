from random import Random
from uuid import UUID
import time
import sys
import math

import numpy as np
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


def format_time(raw_time, unit="ms", precision=2):
    """
    Takes in time in nanoseconds
    Returns formatted time in selected unit
    """

    units = {"s": 1e+9, "ms": 1e+6, "us": 1e+3, "ns": 1}

    selected_unit = units[unit]

    converted_time = raw_time / selected_unit

    if int(converted_time) == converted_time:
        formatted_time = f"{int(converted_time)} {unit}"
    else:
        formatted_time = f"{round(converted_time, precision)} {unit}"

    return formatted_time

def calculate_latency_metrics(result_list):
    """
    Takes in a list of integers and calculates latency metrics
    """
    sorted_list = sorted(result_list)
    number_of_results = len(result_list)-1 # accounting for zero-based indexing
    
    latency_metrics = {
        "min": min(result_list),
        "max": max(result_list),
        "p50": sorted_list[(number_of_results * 50) // 100], # // same as using math floor
        "p75": sorted_list[(number_of_results * 75) // 100],
        "p90": sorted_list[(number_of_results * 90) // 100],
        "p99": sorted_list[(number_of_results * 99) // 100]
    }
    return latency_metrics

# plot_vals and plot_box adapted from here:
# https://gitlab.com/Soha/termbox.py
def plot_vals(vals, step, ticks, out, prefix):
    i = -1
    v = ticks[0]
    print(prefix, end="")
    while v < ticks[-1]:
        if i == -1:
            if out is not None and v < out[0] and v + step > out[0]:
                print("+", end="")
            elif v > vals[0]:
                print("|", end="")
            else:
                print(" ", end="")
        elif i == 0:
            print("-", end="")
        elif i < 3:
            if v < vals[2] and v + step > vals[2]:  # Median is only one point
                print(":", end="")
            else:
                print("=", end="")
        elif i == 3:
            if v >= vals[4] or v + step > ticks[-1]:
                print("|", end="")
            else:
                print("-", end="")
        if i == 4:
            if out is not None and v < out[1] and v + step > out[1]:
                print("+", end="")
            else:
                print(" ", end="")
        elif v > vals[i + 1]:
            i += 1

        v += step
    print("")

def plot_box(data, nticks=5, maxima=True, outliers=True, debug=False):
    box = [5, 25, 50, 75, 95]
    df = np.array(data, dtype=np.float64)
    vals = np.quantile(df, [x / 100 for x in box])
    out = np.quantile(df, [0.01, 0.99])

    if maxima:
        lo, hi = (df.min(), df.max())
    elif outliers:
        lo, hi = (out[0], out[1])
    else:
        lo, hi = (vals.min(), vals.max())

    ticks = np.unique(np.linspace(lo, math.ceil(hi), nticks, dtype=int))
    # The step is the *width* of a character
    step = (ticks.max() - ticks.min()) / 50

    if debug:
        print("step:", step)
        print("vals:", vals)
        print("ticks:", ticks)
        print("out:", out)

    tick_width = len(str(ticks.max()))

    plot_vals(vals, step, ticks, out if outliers else None, " " * (tick_width // 2))

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