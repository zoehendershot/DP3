from shiny import App, render, ui, reactive
import duckdb
import pandas as pd
import plotly.express as px
from shinywidgets import output_widget, render_widget

# Connect to the DuckDB database
con = duckdb.connect("data/github.duckdb")
DB_PATH = "data/github.duckdb"

app_ui = ui.page_fluid(
    ui.h1("GitHub Events Dashboard"),
    ui.navset_tab(
        ui.nav_panel("Event Types Distribution", output_widget("event_types_plot")),
        ui.nav_panel("Top Organizations", output_widget("top_orgs_plot")),
        ui.nav_panel("Ref Type Distribution", output_widget("ref_type_plot")),
        ui.nav_panel("Top Repositories", output_widget("top_repos_plot"))
    ),
)

# Define the server logic
def server(input, output, session):

    # Poll the database every 30 seconds for changes
    @reactive.poll(lambda:30)
    def get_connection():
        return duckdb.connect(DB_PATH, read_only=True)

    # Event Types Distribution
    @output
    @render_widget
    def event_types_plot():
        df_event_types = con.execute("""
            SELECT type, COUNT(*) AS count
            FROM events
            WHERE type IS NOT NULL
            GROUP BY type
            ORDER BY count DESC
        """).fetchdf()
        fig = px.bar(df_event_types, x="type", y="count", title="Event Types Distribution")
        return fig


    # Top Organizations by Event Count
    @output
    @render_widget
    def top_orgs_plot():
        df_top_orgs = con.execute("""
            SELECT org, COUNT(*) AS count
            FROM events
            WHERE org IS NOT NULL
            GROUP BY org
            ORDER BY count DESC
            LIMIT 10
        """).fetchdf()
        fig = px.bar(df_top_orgs, x="org", y="count", title="Top Organizations by Event Count")
        return fig

    # Ref Type Distribution
    @output
    @render_widget
    def ref_type_plot():
        df_ref_type = con.execute("""
            SELECT ref_type, COUNT(*) AS count
            FROM events
            WHERE ref_type IS NOT NULL
            GROUP BY ref_type
            ORDER BY count DESC
        """).fetchdf()
        fig = px.pie(df_ref_type, names="ref_type", values="count", title="Ref Type Distribution")
        return fig

# Create the app
app = App(app_ui, server)