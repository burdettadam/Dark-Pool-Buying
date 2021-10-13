# -*- coding: utf-8 -*-
"""
Jens Olson
jens.olson@gmail.com
"""
import pandas as pd
import numpy as np
import logging
from tqdm import tqdm

LOGGER = logging.getLogger("uvicorn.error." + __name__)
FINRA = "http://regsho.finra.org"
NASDAQ = FINRA + "/FNSQshvol"
NYSE = FINRA + "/FNYXshvol"
OTC = FINRA + "/FORFshvol"


def _panda_read_csv(api, year, month, day):

    return pd.read_csv(
        api + year + month + day + ".txt",
        sep="|",
        dtype={
            "ShortVolume": np.float64,
            "ShortExemptVolume": np.float64,
            "TotalVolume": np.float64,
        },
    )


def calculate_volume(frame, api, year, month, day):

    try:
        df = _panda_read_csv(api, year, month, day)
        date = year + "/" + month + "/" + day
        frame.loc[date, "ShortVolume"] = df["ShortVolume"].sum(
            axis=0
        )
        frame.loc[date, "TotalVolume"] = df["TotalVolume"].sum(
            axis=0
        )
    except Exception as err:
        LOGGER.error(err)


def darkPoolBuying():
    df1 = pd.read_pickle("total_shorts.pkl")

    last_idx = df1.index[-1]

    df_NDAQ = pd.DataFrame(columns=["ShortVolume", "TotalVolume"])
    df_NYSE = pd.DataFrame(columns=["ShortVolume", "TotalVolume"])
    df_ORF = pd.DataFrame(columns=["ShortVolume", "TotalVolume"])
    time_period = pd.date_range(
        start=last_idx, end=pd.Timestamp.today(), freq="D")
    for x in tqdm(time_period):
        year = str(x.date().year)
        month = str(x.date().month).zfill(2)
        day = str(x.date().day).zfill(2)
        # Nasdaq
        calculate_volume(df_NDAQ, NASDAQ, year, month, day)
        # Nyse
        calculate_volume(df_NYSE, NYSE, year, month, day)
        # Otc
        calculate_volume(df_NYSE, OTC, year, month, day)
    for frame in [df_NDAQ, df_NYSE, df_ORF]:
        frame.index = pd.to_datetime(frame.index, infer_datetime_format=True)
    df2 = df_NDAQ + df_NYSE + df_ORF
    df2["ShortPct"] = df2["ShortVolume"] / df2["TotalVolume"]
    df3 = df1.iloc[:-1, :].append(df2, sort=True)
    df3.to_pickle("total_shorts2.pkl")
    return df3
