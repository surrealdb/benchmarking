from random import Random
from uuid import UUID
import time

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


def format_time(raw_time, unit="ms", precision=1, unit_label=False):
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


def calculate_latency_percentile(result_list, unit="ms", interpolation=True):
    """
    Takes in a list of integers and calculates percentile metrics
    """

    if interpolation == False:
        sorted_list = sorted(result_list)
        number_of_results = len(result_list)-1 # accounting for zero-based indexing

        # // same as using math floor
        percentile_metrics = {
            f"min({unit})": format_time(min(sorted_list), unit=unit),
            f"p1({unit})": format_time(sorted_list[(number_of_results * 1) // 100], unit=unit),
            f"p5({unit})": format_time(sorted_list[(number_of_results * 5) // 100], unit=unit),
            f"p25({unit})": format_time(sorted_list[(number_of_results * 25) // 100], unit=unit),
            f"p50({unit})": format_time(sorted_list[(number_of_results * 50) // 100], unit=unit),
            f"p75({unit})": format_time(sorted_list[(number_of_results * 75) // 100], unit=unit),
            f"p90({unit})": format_time(sorted_list[(number_of_results * 90) // 100], unit=unit),
            f"p99({unit})": format_time(sorted_list[(number_of_results * 99) // 100], unit=unit),
            f"max({unit})": format_time(max(sorted_list), unit=unit)
        }

    if interpolation == True:
        percentile_metrics = {
            f"min({unit})": format_time(min(result_list), unit=unit),
            f"p1({unit})": format_time(int(np.percentile(result_list, 1)), unit=unit),
            f"p5({unit})": format_time(int(np.percentile(result_list, 5)), unit=unit),
            f"p25({unit})": format_time(int(np.percentile(result_list, 25)), unit=unit),
            f"p50({unit})": format_time(int(np.percentile(result_list, 50)), unit=unit),
            f"p75({unit})": format_time(int(np.percentile(result_list, 75)), unit=unit),
            f"p90({unit})": format_time(int(np.percentile(result_list, 90)), unit=unit),
            f"p99({unit})": format_time(int(np.percentile(result_list, 99)), unit=unit),
            f"max({unit})": format_time(max(result_list), unit=unit)
        }
    return percentile_metrics

def throughput_calc(query_count, duration, unit="s", precision=1):
    throughput = query_count / format_time(duration, unit=unit, precision=precision)
    return round(throughput, precision)

def calculate_throughput_percentile(result_list, unit="s", interpolation=True):
    """
    Takes in a list of numbers and calculates percentile metrics
    """
    if interpolation == False:
        sorted_list = sorted(result_list)
        number_of_results = len(result_list)-1 # accounting for zero-based indexing

        # // same as using math floor
        percentile_metrics = {
            f"min({unit})": min(sorted_list),
            f"p1({unit})": sorted_list[(number_of_results * 1) // 100],
            f"p5({unit})": sorted_list[(number_of_results * 5) // 100],
            f"p25({unit})": sorted_list[(number_of_results * 25) // 100],
            f"p50({unit})": sorted_list[(number_of_results * 50) // 100],
            f"p75({unit})": sorted_list[(number_of_results * 75) // 100],
            f"p90({unit})": sorted_list[(number_of_results * 90) // 100],
            f"p99({unit})": sorted_list[(number_of_results * 99) // 100],
            f"max({unit})": max(sorted_list)
        }

    if interpolation == True:
        percentile_metrics = {
            f"min({unit})": min(result_list),
            f"p1({unit})": np.percentile(result_list, 1),
            f"p5({unit})": np.percentile(result_list, 5),
            f"p25({unit})": np.percentile(result_list, 25),
            f"p50({unit})": np.percentile(result_list, 50),
            f"p75({unit})": np.percentile(result_list, 75),
            f"p90({unit})": np.percentile(result_list, 90),
            f"p99({unit})": np.percentile(result_list, 99),
            f"max({unit})": max(result_list)
        }

    return percentile_metrics

def create_markdown_metrics_table(SDB_result, ADB_result, unit="ms", metric="latency"):
    if metric == "latency":
        SDB_metrics = calculate_latency_percentile(SDB_result, unit=unit)
        ADB_metrics = calculate_latency_percentile(ADB_result, unit=unit)
    elif metric == "throughput":
        SDB_metrics = calculate_throughput_percentile(SDB_result, unit=unit)
        ADB_metrics = calculate_throughput_percentile(ADB_result, unit=unit)
    else:
        raise Exception(f"metric can either be latency or throughput, not {metric}")

    SDB_metrics_list = list(SDB_metrics.values())
    ADB_metrics_list = list(ADB_metrics.values())
    reshaped_dict = {
        "Metric": list(SDB_metrics.keys()),
        "SurrealDB": SDB_metrics_list,
        "MongoDB": ADB_metrics_list,
        "Difference (%)": [0 if sdb == 0 or adb == 0 else round((((adb - sdb) / sdb) * 100), 2) for sdb, adb in zip(SDB_metrics_list, ADB_metrics_list)]
    }
    table = tabulate(reshaped_dict, headers="keys", tablefmt="github")
    return table

def create_markdown_summary_table(SDB_result, ADB_result, unit="ms"):

    SDB_metrics = {
        "P99 throughput (QPS)": calculate_throughput_percentile(SDB_result["total_throughput_qps"], unit=unit)[f"p99({unit})"],
        "P99 latency (ms)": calculate_latency_percentile(SDB_result["total_time_duration"], unit=unit)[f"p99({unit})"],
        "P99 read latency (ms)": calculate_latency_percentile(SDB_result["total_read_duration"], unit=unit)[f"p99({unit})"],
        "P99 write latency (ms)": calculate_latency_percentile(SDB_result["total_write_duration"], unit=unit)[f"p99({unit})"]
    }

    ADB_metrics = {
        "P99 throughput (QPS)": calculate_throughput_percentile(ADB_result["total_throughput_qps"], unit=unit)[f"p99({unit})"],
        "P99 latency (ms)": calculate_latency_percentile(ADB_result["total_time_duration"], unit=unit)[f"p99({unit})"],
        "P99 read latency (ms)": calculate_latency_percentile(ADB_result["total_read_duration"], unit=unit)[f"p99({unit})"],
        "P99 write latency (ms)": calculate_latency_percentile(ADB_result["total_write_duration"], unit=unit)[f"p99({unit})"]
    }

    SDB_metrics_list = list(SDB_metrics.values())
    ADB_metrics_list = list(ADB_metrics.values())
    reshaped_dict = {
        "Metric": list(SDB_metrics.keys()),
        "SurrealDB": SDB_metrics_list,
        "MongoDB": ADB_metrics_list,
        "Difference (%)": [0 if sdb == 0 or adb == 0 else round((((adb - sdb) / sdb) * 100), 2) for sdb, adb in zip(SDB_metrics_list, ADB_metrics_list)]
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


def run_arangodb_query(operation, query, iterations=1):
    """
    Run templated arangodb queries
    """
    result_list = []
    for _ in range(iterations):
        start_time = time.perf_counter_ns()

		# query to be run
        list(operation(query))

        end_time = time.perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    return result_list