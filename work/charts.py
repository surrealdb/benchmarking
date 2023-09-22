import pathlib

import pandas as pd
import altair as alt 


def main():

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

    export_path = pathlib.Path(__file__).parents[1] / "export"
    # Create metrics

    bench_df = pd.read_json(export_path / pathlib.Path("bench_result.json"))

    metrics = (
        bench_df.groupby(["query", "unit", "engine", "version", "statement"])["time"]
        .agg(["std", "mean", "median", "min", "max"])
        .reset_index()
        .round(1)
    )

    def make_median_stdev_text(metrics):
        return f"{metrics['median']} ± {metrics['std']}"

    def make_mean_stdev_text(metrics):
        return f"{metrics['mean']} ± {metrics['std']}"

    metrics["median_stdev"] = metrics.apply(make_median_stdev_text, axis=1)
    metrics["mean_stdev"] = metrics.apply(make_mean_stdev_text, axis=1)

    print("Metrics created")

    # Create charts

    ## Boxplot

    boxplot = (
        alt.Chart(bench_df)
        .mark_boxplot(extent="min-max")
        .encode(
            alt.X("time").scale(zero=False).title(f"time [µs]"),
            alt.Y("query").title("query"),
        )
    ).facet(column='engine')

    boxplot.save(
        export_path / pathlib.Path("boxplot.html"), embed_options={"renderer": "svg"}
    )

    print("Boxplot outputted")

    ## heatmap
    ## Chart filtering

    from IPython.display import display, HTML

    display(HTML("""
    <style>
    form.vega-bindings {
        position: absolute;
        left: 0px;
        top: 0px;
    }
    </style>
    """))

    labels = [statement + ' ' for statement in statements_list]


    input_dropdown = alt.binding_select(options=statements_list + [None], labels=labels + ['All'], name='Choose statement: ')
    selection = alt.selection_point(value='create_record_id', fields=['statement'], bind=input_dropdown)


    heatmap_layer = alt.Chart(bench_df).mark_rect().encode(
        alt.Y("query", axis=alt.Axis(labelLimit=300)).sort(None).title(None),
        alt.X("engine").title("engine"),
        alt.Color("time").title('time'),

    ).add_params(
        selection
    ).transform_filter(
        selection
    )

    heatmap_layer

    text_metrics = alt.Chart(metrics).mark_text(baseline='bottom', color='black').encode(
        alt.Y("query").title("query"),
        alt.X("engine").title("engine"),
        alt.Text("mean_stdev"),
        tooltip=[
            alt.Tooltip("engine"),
            alt.Tooltip("min"),
            alt.Tooltip("max"),
            alt.Tooltip("mean"),
            alt.Tooltip("median"),
            alt.Tooltip("median"),
            alt.Tooltip("query"),
        ],
    ).add_params(
        selection
    ).transform_filter(
        selection
    )

    text_unit = alt.Chart(metrics).mark_text(baseline='top', color='black').encode(
        alt.Y("query").title("query"),
        alt.X("engine").title("engine"),
        alt.Text("unit"),
        tooltip=[
            alt.Tooltip("engine"),
            alt.Tooltip("min"),
            alt.Tooltip("max"),
            alt.Tooltip("mean"),
            alt.Tooltip("median"),
            alt.Tooltip("median"),
            alt.Tooltip("query"),
        ],
    ).add_params(
        selection
    ).transform_filter(
        selection
    )

    full_heatmap = alt.layer(heatmap_layer, text_metrics, text_unit).facet(column='version', data=metrics).configure_view(
        step=70,
    )
    full_heatmap



    full_heatmap.save(
        export_path / pathlib.Path("heatmap.html"), embed_options={"renderer": "svg"}
    )

    print("heatmap outputted")

if __name__ == "__main__":
    main()