# supabench

Platform to run and keep the history of benchmark runs.

![Supabench Banner](https://user-images.githubusercontent.com/58992960/186262109-e6c9ab69-e5f7-4fd0-bd62-5ea08ab3fe60.png)

## References

- [Supabase](https://supabase.com) - Built by Supabase team.
- [Pocketbase](https://pocketbase.io) - This project uses Pocketbase as a backend, and the frontend is also based on the Pocketbase admin UI.
- [k6](https://k6.io) - The load generator used.
- [Terraform](https://www.terraform.io) - SUT and loader infrastructure delivery.
- [Grafana](https://grafana.com) - Dashboard for benchmark results.
- [Prometheus](https://prometheus.io) - Store metrics for benchmark results.
- [Telegraf](https://www.influxdata.com/time-series-platform/telegraf/) - Send benchmark metrics to Prometheus.

## More Info

This directory is a clone of https://github.com/supabase/supabench with customisations specific to SurrealDB

The components are:
* Supabench instance: it persists the benchmark definitions and executes the benchmarks
* Benchmark definition: it has a ZIP file and a JSON with vars. The ZIP contains Terraform and K6 code. The vars are passed to the Terraform command.
* Infrastructure: the Terraform code creates the EC2 instances, uploads the K6 code and runs it.
* Grafana Cloud: the K6 metrics are pushed to Grafana Cloud (https://surrealdb.grafana.net)

How to run it?
* Start the Supabench instance somewhere accessible by the EC2 instances:
```
$ docker-compose up -d
```
* Access the Admin panel and create the necessary resources. Check https://github.com/supabase/benchmarks/wiki for a step-by-step guide. Most of it still works the same way
```
$ open http://<supabench-ip>:8090/_/
```
* Once the benchmarks are defined, access the Supabase UI and launch a new run:
```
$ open http://<supabench-ip>:8090/#/
```
* Check the Grafana K6 Dashboard to monitor the test:
```
$ open https://surrealdb.grafana.net/d/01npcT44k/official-k6-test-result
```
