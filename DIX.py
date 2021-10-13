# -*- coding: utf-8 -*-
"""
Jens Olson
jens.olson@gmail.com
"""
import logging
import sys
from threading import main_thread

import numpy as np
import pandas as pd
from tqdm import tqdm

LOGGER = logging.getLogger("uvicorn.error." + __name__)


FINRA = "https://cdn.finra.org/equity/regsho/daily/"
NMS = FINRA + "CNMSshvol"
TRF = FINRA + "FNQCshvol"
ADF = FINRA + "FNRAshvol"
NASDAQ = FINRA + "FNSQshvol"
NYSE = FINRA + "FNYXshvol"
OTC = FINRA + "FORFshvol"


def _pandaReadCsv(api, year, month, day):

    LOGGER.info("Retrieving CSV from: {}".format(api))
    return pd.read_csv(
        api + year + month + day + ".txt",
        sep="|",
        dtype={
            "Symbol": np.float64,
            "ShortVolume": np.float64,
            "ShortExemptVolume": np.float64,
            "TotalVolume": np.float64,
        },
    )


def calculateVolume(frame, api, year, month, day):

    LOGGER.info("Calculating volumes for")
    try:
        df = _pandaReadCsv(api, year, month, day)
        date = year + "/" + month + "/" + day
        frame.loc[date, "ShortVolume"] = df["ShortVolume"].sum(
            axis=0
        )
        frame.loc[date, "TotalVolume"] = df["TotalVolume"].sum(
            axis=0
        )
    except Exception as err:
        LOGGER.error("During {} call, {}".format(api, err))


def darkPoolBuying():

    # dataFrame for each market volume
    df_NMS = pd.DataFrame(columns=["ShortVolume", "TotalVolume"])
    df_TRF = pd.DataFrame(columns=["ShortVolume", "TotalVolume"])
    df_ADF = pd.DataFrame(columns=["ShortVolume", "TotalVolume"])
    df_NDAQ = pd.DataFrame(columns=["ShortVolume", "TotalVolume"])
    df_NYSE = pd.DataFrame(columns=["ShortVolume", "TotalVolume"])
    df_ORF = pd.DataFrame(columns=["ShortVolume", "TotalVolume"])
    # load old values from pickle file
    df1 = pd.read_pickle("total_shorts.pkl")
    # start from the last saved place in pickle
    last_idx = df1.index[-1]
    time_period = pd.date_range(
        start=last_idx, end=pd.Timestamp.today(), freq="D")
    for x in tqdm(time_period):
        year = str(x.date().year)
        month = str(x.date().month).zfill(2)
        day = str(x.date().day).zfill(2)
        calculateVolume(df_NMS, NMS, year, month, day)
        calculateVolume(df_TRF, TRF, year, month, day)
        calculateVolume(df_ADF, ADF, year, month, day)
        calculateVolume(df_NDAQ, NASDAQ, year, month, day)
        calculateVolume(df_NYSE, NYSE, year, month, day)
        calculateVolume(df_NYSE, OTC, year, month, day)
    for frame in [df_NMS, df_TRF, df_ADF, df_NDAQ, df_NYSE, df_ORF]:
        frame.index = pd.to_datetime(frame.index, infer_datetime_format=True)
    # combine all frames
    df2 = df_NMS + df_TRF + df_ADF + df_NDAQ + df_NYSE + df_ORF
    # calculate shortPct from short volume over total volume
    df2["ShortPct"] = df2["ShortVolume"] / df2["TotalVolume"]
    # combine all data with past calculated values
    df3 = df1.iloc[:-1, :].append(df2, sort=True)
    # save new values
    df3.to_pickle("total_shorts2.pkl")
    return df3


if __name__ == '__main__':
    darkPoolBuying().plot()
    sys.exit(main_thread())
