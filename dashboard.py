import os
import re
from pathlib import Path
from datetime import date

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from user_agents import parse


ENV_FILE_PATH = Path(__file__).resolve().parent / ".env"


def load_dotenv_file(env_file_path: Path) -> dict[str, str]:
    if not env_file_path.exists():
        return {}

    env_values: dict[str, str] = {}
    for raw_line in env_file_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        env_values[key.strip()] = value.strip().strip("'").strip('"')
    return env_values


DOTENV_VALUES = load_dotenv_file(ENV_FILE_PATH)


def get_config_value(key: str, fallback: str = "") -> str:
    return os.environ.get(key) or DOTENV_VALUES.get(key) or fallback


DEFAULT_DB_CONFIG = {
    "host": get_config_value("POSTGRES_HOST", "localhost"),
    "port": get_config_value("POSTGRES_PORT", "5432"),
    "database": get_config_value("POSTGRES_DB", "glamira_analytics"),
    "user": get_config_value("POSTGRES_USER", "postgres"),
    "password": get_config_value("POSTGRES_PASSWORD", "postgres"),
    "schema": get_config_value("POSTGRES_SCHEMA", "public"),
}
VIEW_COLLECTION = "view_product_detail"


def safe_identifier(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier or ""):
        raise ValueError(f"Invalid SQL identifier: {identifier!r}")
    return identifier


def build_database_url(config: dict[str, str]) -> URL:
    return URL.create(
        "postgresql+psycopg2",
        username=config["user"],
        password=config["password"],
        host=config["host"],
        port=int(config["port"]),
        database=config["database"],
    )


@st.cache_resource(show_spinner=False)
def get_engine(database_url: str):
    return create_engine(database_url, pool_pre_ping=True)


@st.cache_data(ttl=60, show_spinner=False)
def run_query(database_url: str, sql: str, params: dict | None = None) -> pd.DataFrame:
    engine = get_engine(database_url)
    with engine.connect() as connection:
        return pd.read_sql(text(sql), connection, params=params or {})


def build_query_parts(schema: str) -> dict[str, str]:
    safe_schema = safe_identifier(schema)

    return {
        "from_clause": f"""
            FROM {safe_schema}.fact_log_event fact
            JOIN {safe_schema}.dim_date dd
              ON fact.date_key = dd.date_key
            LEFT JOIN {safe_schema}.dim_product dp
              ON fact.product_key = dp.product_key
            LEFT JOIN {safe_schema}.dim_location dl
              ON fact.location_key = dl.location_key
        """,
        "base_where": """
            WHERE fact.collection = :collection
              AND dd.date = :report_date
        """,
    }


def get_available_dates(database_url: str, schema: str) -> list[date]:
    query_parts = build_query_parts(schema)
    sql = f"""
        SELECT dd.date
        {query_parts["from_clause"]}
        WHERE fact.collection = :collection
        GROUP BY dd.date
        ORDER BY dd.date DESC
    """
    df = run_query(database_url, sql, {"collection": VIEW_COLLECTION})
    if df.empty:
        return []
    return pd.to_datetime(df["date"]).dt.date.tolist()


def get_summary_metrics(database_url: str, schema: str, report_date: date) -> dict[str, int]:
    query_parts = build_query_parts(schema)
    sql = f"""
        SELECT
            COALESCE(SUM(fact.event_count), 0) AS total_views,
            COUNT(DISTINCT dp.product_id) AS unique_products,
            COUNT(DISTINCT dl.country) AS unique_countries,
            COUNT(DISTINCT NULLIF(fact.referrer_url, '')) AS unique_referrers
        {query_parts["from_clause"]}
        {query_parts["base_where"]}
    """
    df = run_query(
        database_url,
        sql,
        {"collection": VIEW_COLLECTION, "report_date": report_date},
    )
    if df.empty:
        return {
            "total_views": 0,
            "unique_products": 0,
            "unique_countries": 0,
            "unique_referrers": 0,
        }
    record = df.iloc[0].fillna(0).to_dict()
    return {key: int(value) for key, value in record.items()}


def get_top_products(database_url: str, schema: str, report_date: date) -> pd.DataFrame:
    query_parts = build_query_parts(schema)
    sql = f"""
        SELECT
            dp.product_id,
            SUM(fact.event_count) AS views
        {query_parts["from_clause"]}
        {query_parts["base_where"]}
          AND dp.product_id IS NOT NULL
        GROUP BY dp.product_id
        ORDER BY views DESC, dp.product_id
        LIMIT 10
    """
    return run_query(
        database_url,
        sql,
        {"collection": VIEW_COLLECTION, "report_date": report_date},
    )


def get_top_countries(database_url: str, schema: str, report_date: date) -> pd.DataFrame:
    query_parts = build_query_parts(schema)
    sql = f"""
        SELECT
            COALESCE(NULLIF(dl.country, ''), 'Unknown') AS country,
            SUM(fact.event_count) AS views
        {query_parts["from_clause"]}
        {query_parts["base_where"]}
        GROUP BY country
        ORDER BY views DESC, country
        LIMIT 10
    """
    return run_query(
        database_url,
        sql,
        {"collection": VIEW_COLLECTION, "report_date": report_date},
    )


def get_top_referrers(database_url: str, schema: str, report_date: date) -> pd.DataFrame:
    query_parts = build_query_parts(schema)
    sql = f"""
        SELECT
            fact.referrer_url,
            SUM(fact.event_count) AS views
        {query_parts["from_clause"]}
        {query_parts["base_where"]}
          AND NULLIF(fact.referrer_url, '') IS NOT NULL
        GROUP BY fact.referrer_url
        ORDER BY views DESC, fact.referrer_url
        LIMIT 5
    """
    return run_query(
        database_url,
        sql,
        {"collection": VIEW_COLLECTION, "report_date": report_date},
    )


def get_country_options(database_url: str, schema: str, report_date: date) -> pd.DataFrame:
    query_parts = build_query_parts(schema)
    sql = f"""
        SELECT
            COALESCE(NULLIF(dl.country, ''), 'Unknown') AS country,
            SUM(fact.event_count) AS views
        {query_parts["from_clause"]}
        {query_parts["base_where"]}
        GROUP BY country
        ORDER BY views DESC, country
    """
    return run_query(
        database_url,
        sql,
        {"collection": VIEW_COLLECTION, "report_date": report_date},
    )


def get_store_views_by_country(
    database_url: str,
    schema: str,
    report_date: date,
    country: str,
) -> pd.DataFrame:
    query_parts = build_query_parts(schema)
    sql = f"""
        SELECT
            COALESCE(NULLIF(fact.store_id, ''), 'Unknown') AS store_id,
            SUM(fact.event_count) AS views
        {query_parts["from_clause"]}
        {query_parts["base_where"]}
          AND COALESCE(NULLIF(dl.country, ''), 'Unknown') = :country
        GROUP BY store_id
        ORDER BY views DESC, store_id
    """
    return run_query(
        database_url,
        sql,
        {
            "collection": VIEW_COLLECTION,
            "report_date": report_date,
            "country": country,
        },
    )


def get_product_options(database_url: str, schema: str, report_date: date) -> pd.DataFrame:
    query_parts = build_query_parts(schema)
    sql = f"""
        SELECT
            dp.product_id,
            SUM(fact.event_count) AS views
        {query_parts["from_clause"]}
        {query_parts["base_where"]}
          AND dp.product_id IS NOT NULL
        GROUP BY dp.product_id
        ORDER BY views DESC, dp.product_id
    """
    return run_query(
        database_url,
        sql,
        {"collection": VIEW_COLLECTION, "report_date": report_date},
    )


def get_product_hourly_views(
    database_url: str,
    schema: str,
    report_date: date,
    product_id: str,
) -> pd.DataFrame:
    query_parts = build_query_parts(schema)
    sql = f"""
        SELECT
            EXTRACT(HOUR FROM fact.event_timestamp)::INT AS event_hour,
            SUM(fact.event_count) AS views
        {query_parts["from_clause"]}
        {query_parts["base_where"]}
          AND dp.product_id = :product_id
        GROUP BY event_hour
        ORDER BY event_hour
    """
    hourly_df = run_query(
        database_url,
        sql,
        {
            "collection": VIEW_COLLECTION,
            "report_date": report_date,
            "product_id": product_id,
        },
    )
    return complete_hour_range(hourly_df, "views")


def get_browser_os_hourly(database_url: str, schema: str, report_date: date) -> pd.DataFrame:
    query_parts = build_query_parts(schema)
    sql = f"""
        SELECT
            EXTRACT(HOUR FROM fact.event_timestamp)::INT AS event_hour,
            COALESCE(fact.user_agent, '') AS user_agent,
            SUM(fact.event_count) AS views
        {query_parts["from_clause"]}
        {query_parts["base_where"]}
        GROUP BY event_hour, user_agent
        ORDER BY event_hour, user_agent
    """
    raw_df = run_query(
        database_url,
        sql,
        {"collection": VIEW_COLLECTION, "report_date": report_date},
    )
    if raw_df.empty:
        return raw_df

    unique_agents = raw_df["user_agent"].drop_duplicates()
    browser_lookup = {user_agent: parse_browser_family(user_agent) for user_agent in unique_agents}
    os_lookup = {user_agent: parse_os_family(user_agent) for user_agent in unique_agents}
    raw_df["browser"] = raw_df["user_agent"].map(browser_lookup).fillna("Unknown")
    raw_df["os"] = raw_df["user_agent"].map(os_lookup).fillna("Unknown")

    return (
        raw_df.groupby(["event_hour", "browser", "os"], as_index=False)["views"]
        .sum()
        .sort_values(["event_hour", "views"], ascending=[True, False])
    )


def parse_browser_family(user_agent: str) -> str:
    if not user_agent:
        return "Unknown"

    try:
        browser_family = parse(user_agent).browser.family
    except Exception:
        return "Unknown"

    return browser_family or "Unknown"


def parse_os_family(user_agent: str) -> str:
    if not user_agent:
        return "Unknown"

    try:
        os_family = parse(user_agent).os.family
    except Exception:
        return "Unknown"

    return os_family or "Unknown"


def complete_hour_range(hourly_df: pd.DataFrame, value_column: str) -> pd.DataFrame:
    base_hours = pd.DataFrame({"event_hour": list(range(24))})
    if hourly_df.empty:
        base_hours[value_column] = 0
        return base_hours

    completed_df = base_hours.merge(hourly_df, on="event_hour", how="left").fillna(0)
    completed_df[value_column] = completed_df[value_column].astype(int)
    return completed_df


def complete_hour_breakdown(df: pd.DataFrame, category_column: str) -> pd.DataFrame:
    if df.empty:
        return df

    hours = pd.DataFrame({"event_hour": list(range(24))})
    categories = pd.DataFrame({category_column: sorted(df[category_column].dropna().unique())})
    complete_index = hours.assign(_key=1).merge(categories.assign(_key=1), on="_key").drop(columns="_key")
    completed_df = complete_index.merge(df, on=["event_hour", category_column], how="left").fillna(0)
    completed_df["views"] = completed_df["views"].astype(int)
    return completed_df


def render_ranked_bar_chart(df: pd.DataFrame, category_column: str, title: str) -> None:
    if df.empty:
        st.info("No data available for this chart.")
        return

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("views:Q", title="Views"),
            y=alt.Y(f"{category_column}:N", sort="-x", title=""),
            tooltip=[category_column, "views"],
        )
        .properties(title=title)
    )
    st.altair_chart(chart, use_container_width=True)


def ellipsize_text(value: str, max_length: int = 40) -> str:
    if value is None:
        return ""
    if len(value) <= max_length:
        return value
    return f"{value[:max_length - 3]}..."


def render_hourly_line_chart(
    df: pd.DataFrame,
    series_column: str | None,
    title: str,
) -> None:
    if df.empty:
        st.info("No data available for this chart.")
        return

    encodings = {
        "x": alt.X("event_hour:O", title="Hour of day"),
        "y": alt.Y("views:Q", title="Views"),
        "tooltip": ["event_hour", "views"],
    }
    if series_column:
        encodings["color"] = alt.Color(f"{series_column}:N", title=series_column.replace("_", " ").title())
        encodings["tooltip"] = ["event_hour", series_column, "views"]

    chart = alt.Chart(df).mark_line(point=True).encode(**encodings).properties(title=title)
    st.altair_chart(chart, use_container_width=True)


def main() -> None:
    st.set_page_config(page_title="Glamira Analytics Dashboard", layout="wide")
    st.title("Glamira Product View Dashboard")
    st.caption(
        "The dashboard reads from the Postgres warehouse created by the Spark pipeline "
        "and focuses on `view_product_detail` events from the README requirements."
    )

    with st.sidebar:
        st.header("Database Connection")
        st.caption("Default Postgres values are loaded from `.env` when available.")
        host = st.text_input("Host", value=DEFAULT_DB_CONFIG["host"])
        port = st.text_input("Port", value=DEFAULT_DB_CONFIG["port"])
        database = st.text_input("Database", value=DEFAULT_DB_CONFIG["database"])
        user = st.text_input("User", value=DEFAULT_DB_CONFIG["user"])
        password = st.text_input("Password", value=DEFAULT_DB_CONFIG["password"], type="password")
        schema = st.text_input("Schema", value=DEFAULT_DB_CONFIG["schema"])

        if st.button("Refresh Cached Data", use_container_width=True):
            st.cache_data.clear()
            st.cache_resource.clear()

    config = {
        "host": host.strip(),
        "port": port.strip(),
        "database": database.strip(),
        "user": user.strip(),
        "password": password,
        "schema": schema.strip(),
    }

    try:
        database_url = build_database_url(config)
        available_dates = get_available_dates(database_url, config["schema"])
    except Exception as exc:
        st.error(f"Unable to connect to Postgres or load metadata: {exc}")
        st.stop()

    if not available_dates:
        st.warning("No `view_product_detail` data is available in the warehouse yet.")
        st.stop()

    selected_date = st.sidebar.selectbox(
        "Report Date",
        options=available_dates,
        index=0,
        format_func=lambda value: value.strftime("%Y-%m-%d"),
    )
    st.caption(
        f"Showing data for `{selected_date:%Y-%m-%d}`. "
        "The default is the latest available date in the warehouse."
    )

    summary = get_summary_metrics(database_url, config["schema"], selected_date)
    metric_columns = st.columns(4)
    metric_columns[0].metric("Total Views", f"{summary['total_views']:,}")
    metric_columns[1].metric("Unique Products", f"{summary['unique_products']:,}")
    metric_columns[2].metric("Countries", f"{summary['unique_countries']:,}")
    metric_columns[3].metric("Referrers", f"{summary['unique_referrers']:,}")

    overview_tab, country_tab, product_tab, browser_os_tab = st.tabs(
        ["Top Reports", "Country Drilldown", "Product Drilldown", "Browser and OS"]
    )

    with overview_tab:
        top_product_col, top_country_col = st.columns(2)

        top_products_df = get_top_products(database_url, config["schema"], selected_date)
        with top_product_col:
            st.subheader("Top 10 Products")
            render_ranked_bar_chart(top_products_df, "product_id", "Top viewed product IDs")
            st.dataframe(top_products_df, use_container_width=True, hide_index=True)

        top_countries_df = get_top_countries(database_url, config["schema"], selected_date)
        with top_country_col:
            st.subheader("Top 10 Countries")
            render_ranked_bar_chart(top_countries_df, "country", "Top countries by views")
            st.dataframe(top_countries_df, use_container_width=True, hide_index=True)

        top_referrers_df = get_top_referrers(database_url, config["schema"], selected_date)
        st.subheader("Top 5 Referrers")
        if top_referrers_df.empty:
            st.info("No data available for this chart.")
        else:
            referrer_chart_df = top_referrers_df.copy()
            referrer_chart_df["referrer_label"] = referrer_chart_df["referrer_url"].apply(
                lambda value: ellipsize_text(value, 55)
            )

            referrer_chart = (
                alt.Chart(referrer_chart_df)
                .mark_bar()
                .encode(
                    x=alt.X("views:Q", title="Views"),
                    y=alt.Y("referrer_label:N", sort="-x", title=""),
                    tooltip=[
                        alt.Tooltip("referrer_url:N", title="Referrer URL"),
                        alt.Tooltip("views:Q", title="Views"),
                    ],
                )
                .properties(title="Top referrer URLs")
            )
            st.altair_chart(referrer_chart, use_container_width=True)
            st.dataframe(
                top_referrers_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "referrer_url": st.column_config.TextColumn(
                        "referrer_url",
                        width="large",
                    ),
                    "views": st.column_config.NumberColumn("views", format="%d"),
                },
            )

    with country_tab:
        country_options_df = get_country_options(database_url, config["schema"], selected_date)
        if country_options_df.empty:
            st.info("No country data available for the selected date.")
        else:
            country = st.selectbox("Country", options=country_options_df["country"].tolist())
            store_views_df = get_store_views_by_country(
                database_url,
                config["schema"],
                selected_date,
                country,
            )
            st.subheader(f"Store views in {country}")
            render_ranked_bar_chart(store_views_df, "store_id", f"Store ranking for {country}")
            st.dataframe(store_views_df, use_container_width=True, hide_index=True)

    with product_tab:
        product_options_df = get_product_options(database_url, config["schema"], selected_date)
        if product_options_df.empty:
            st.info("No product data available for the selected date.")
        else:
            product_options_df["label"] = product_options_df.apply(
                lambda row: f"{row['product_id']} ({int(row['views'])} views)",
                axis=1,
            )
            product_label = st.selectbox("Product", options=product_options_df["label"].tolist())
            selected_product_id = product_options_df.loc[
                product_options_df["label"] == product_label, "product_id"
            ].iloc[0]

            product_hourly_df = get_product_hourly_views(
                database_url,
                config["schema"],
                selected_date,
                selected_product_id,
            )
            st.subheader(f"Hourly views for product {selected_product_id}")
            render_hourly_line_chart(product_hourly_df, None, "Hourly product views")
            st.dataframe(product_hourly_df, use_container_width=True, hide_index=True)

    with browser_os_tab:
        browser_os_df = get_browser_os_hourly(database_url, config["schema"], selected_date)
        if browser_os_df.empty:
            st.info("No browser or OS data available for the selected date.")
        else:
            browser_hourly_df = (
                browser_os_df.groupby(["event_hour", "browser"], as_index=False)["views"]
                .sum()
                .pipe(complete_hour_breakdown, "browser")
            )
            os_hourly_df = (
                browser_os_df.groupby(["event_hour", "os"], as_index=False)["views"]
                .sum()
                .pipe(complete_hour_breakdown, "os")
            )

            browser_chart_col, os_chart_col = st.columns(2)
            with browser_chart_col:
                st.subheader("Hourly views by browser")
                render_hourly_line_chart(browser_hourly_df, "browser", "Browser traffic by hour")

            with os_chart_col:
                st.subheader("Hourly views by OS")
                render_hourly_line_chart(os_hourly_df, "os", "OS traffic by hour")

            st.subheader("Detailed browser and OS breakdown")
            st.dataframe(browser_os_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
