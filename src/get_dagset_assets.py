import datetime

import pandas as pd
from dagster.core.definitions.event import FloatMetadataEntryData
from dagster.serdes import deserialize_json_to_dagster_namedtuple
from sqlalchemy import MetaData, create_engine, select


# helper function to get all asset_keys in database
def get_all_asset_keys(table, filter_asset_keys: str = ""):
    _sd = select([table.c.asset_key]).distinct(table.c.asset_key)
    return [
        asset_key[0]
        for asset_key in conn.execute(_sd)
        if asset_key[0] and filter_asset_keys in asset_key[0]
    ]


def get_asset_keys_values(results) -> dict:
    assets = {}
    for result in results:
        dagster_namedtuple = deserialize_json_to_dagster_namedtuple(result[0])
        time_stamp = datetime.fromtimestamp(dagster_namedtuple.timestamp).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        assets[time_stamp] = {}
        assets[time_stamp][
            "asset_key"
        ] = dagster_namedtuple.dagster_event.asset_key.to_string()
        for (
            entry
        ) in (
            dagster_namedtuple.dagster_event.event_specific_data.materialization.metadata_entries
        ):
            if isinstance(
                entry.entry_data, FloatMetadataEntryData
            ):  # Only assets that are numerical
                assets[time_stamp][entry.label] = entry.entry_data.value
    return assets


def convert_dict_2_df(dict_assets) -> pd.DataFrame:
    df_assets = pd.DataFrame.from_dict(dict_assets, orient="index")
    df_assets.index = pd.to_datetime(df_assets.index)
    df_assets = df_assets[
        ["asset_key", "count"]
    ]  # use only relevant columns for monitor
    df_assets_pivot = pd.pivot(df_assets, columns="asset_key")
    df_assets_pivot.columns = df_assets_pivot.columns.droplevel()
    df_assets_pivot = df_assets_pivot.dropna()
    df_assets_pct_change = df_assets_pivot.pct_change()
    return df_assets_pct_change


def main():
    engine = create_engine(
        "postgersql://database_user:user_password@server:port/database_name"
    )
    conn = engine.connect()
    meta = MetaData()
    meta.reflect(engine)
    t_event_logs = meta.tables["event_logs"]  # table which dagster saves all the events

    assets = get_all_asset_keys(t_event_logs, "")

    _s = select([t_event_logs.c.event]).where(t_event_logs.c.asset_key.in_(assets))
    results = conn.execute(_s)

    assets = get_asset_keys_values(results)

    df_assets_pct_change = convert_dict_2_df(assets)
    print(f"{df_assets_pct_change.head()}")


if __name__ == "__main__":
    main()
