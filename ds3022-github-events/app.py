from shiny import App, render, ui, reactive
import duckdb
import plotly.express as px
from shinywidgets import output_widget, render_widget

DB_PATH = "data/github.duckdb"

app_ui = ui.page_fluid(
    ui.h1("GitHub Events Dashboard"),
    ui.navset_tab(
        ui.nav_panel("Event Types Distribution", output_widget("event_types_plot")),
        ui.nav_panel("Top Organizations", output_widget("top_orgs_plot")),
        ui.nav_panel("Ref Type Distribution", output_widget("ref_type_plot")),
        ui.nav_panel("Top Repositories", output_widget("top_repos_plot")),
    ),
)

def server(input, output, session):

    # fires every 30 s and opens a fresh connection
    @reactive.poll(lambda: 30)
    def poll_db():
        with duckdb.connect(DB_PATH, read_only=True) as conn:
            return conn.execute(
                "SELECT MAX(created_at) FROM events"
            ).fetchone()[0]

    def load_query(query: str):
        with duckdb.connect(DB_PATH, read_only=True) as conn:
            return conn.execute(query).fetchdf()

    @output
    @render_widget
    def event_types_plot():
        _ = poll_db()  # depend on polling trigger
        df = load_query("""
            SELECT type, COUNT(*) AS count
            FROM events
            WHERE type IS NOT NULL
            GROUP BY type
            ORDER BY count DESC
        """)
        return px.bar(df, x="type", y="count", title="Event Types Distribution")

    @output
    @render_widget
    def top_orgs_plot():
        _ = poll_db()
        df = load_query("""
            SELECT org, COUNT(*) AS count
            FROM events
            WHERE org IS NOT NULL
            GROUP BY org
            ORDER BY count DESC
            LIMIT 10
        """)
        return px.bar(df, x="org", y="count", title="Top Organizations by Event Count")

    @output
    @render_widget
    def ref_type_plot():
        _ = poll_db()
        df = load_query("""
            SELECT ref_type, COUNT(*) AS count
            FROM events
            WHERE ref_type IS NOT NULL
            GROUP BY ref_type
            ORDER BY count DESC
        """)
        return px.pie(df, names="ref_type", values="count", title="Ref Type Distribution")

app = App(app_ui, server)
