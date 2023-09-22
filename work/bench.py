import json
import pathlib

from surrealdb import SurrealHTTP

# change this per run
version = "1.0.0+20230913.54aedcd for macos on aarch64"
engine = "file"


async def main():
    # Read bench queries
    queries_path = pathlib.Path(__file__).parents[0] / "bench_queries.json"

    with open(queries_path, "r") as read_file:
        query_list = json.load(read_file)

    statements_list = [
        "create_record_id",
        "create_record_link",
        "create_data",
        "create_datetimes",
        "create_geometries",
        "insert",
        "update",
        "relate",
        "select_graph",
        "select_datetime",
        "select_casting"
    ]

    # Open connection to SurrealDB
    db = SurrealHTTP(
        "http://localhost:8000",
        namespace="test",
        database="test",
        username="root",
        password="root",
    )

    # Run warmup
    warmup_result = []
    for query in query_list["warmup"]:
        query_result = await db.query(
            f"""
            {query};
            """
        )
        warmup_result.append(query_result)

    print("warmup completed")

    # Run benchmark
    query_result = []

    for statement in statements_list:
        print(statement + " started")
        for query in query_list[statement]:
            for _ in range(100):
                request_result = await db.query(
                    f"""
                    {query};
                    """
                )
                # TODO make function to standardize the time
                time_str = request_result[0]["time"]
                if time_str.endswith(("ms", "µs")):
                    time = float(time_str[:-2])
                    unit = time_str[-2:]
                else:
                    time = float(time_str[:-1])
                    unit = time_str[-1:]
                status = request_result[0]["status"]

                # TODO add more metadata
                cleaned_result = {
                    "query": query,
                    "time": time,
                    "unit": unit,
                    "status": status,
                    "statement": statement,
                    "engine": engine,
                    "version": version,
                }
                query_result.append(cleaned_result)
        print(statement + " completed")
    print("Benchmark completed")

    # Output the results
    export_path = pathlib.Path(__file__).parents[1] / "export"

    with open(export_path / pathlib.Path("warmup.json"), "w") as write_file:
        json.dump(warmup_result, write_file, ensure_ascii=False)

    with open(export_path / pathlib.Path("bench_result.json"), "a") as write_file:
        json.dump(query_result, write_file, ensure_ascii=False)

    print("Results outputted")

if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
