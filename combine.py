import os
import operator
from datetime import datetime
import pandas as pd


def pd_series(d_f):
    d_f.columns = ["Date", "Time", "Open", "High", "Low", "Close", "TotalVolume"]
    dates = d_f.Date.values + " " + d_f.Time.values
    date = [datetime.strptime(x, "%Y/%m/%d %H:%M:%S") for x in dates]
    d_f.index = date
    return d_f


def pd_resample(d_f, scale):
    output = pd.DataFrame(
        {
            "Open": d_f.Open.resample(scale, label="left").first(),
            "High": d_f.High.resample(scale, label="left").max(),
            "Low": d_f.Low.resample(scale, label="left").min(),
            "Close": d_f.Close.resample(scale, label="left").last(),
            "TotalVolume": d_f.TotalVolume.resample(scale, label="left").sum(),
        }
    )
    output.dropna(inplace=True)
    return output


def combine_ohlc(files, ratios, intercept, operators):
    ops = {
        "+": operator.add,
        "-": operator.sub,
        "*": operator.mul,
        "/": operator.truediv,
        "%": operator.mod,
        "^": operator.xor,
    }
    product_a = pd.read_csv(files[0])
    product_a = pd_series(product_a)
    product_b = pd.read_csv(files[1])
    product_b = pd_series(product_b)
    time_scale_a = product_a.index[1] - product_a.index[0]
    time_scale_b = product_b.index[1] - product_b.index[0]
    scale = max(time_scale_a, time_scale_b)
    product_a = pd_resample(product_a, scale)
    product_b = pd_resample(product_b, scale)
    product_c = ops[operators[4]](
        ops[operators[3]](
            ops[operators[1]](
                ops[operators[0]](product_a, ratios[0]),
                ops[operators[2]](product_b, ratios[1]),
            ),
            ratios[2],
        ),
        intercept,
    )
    first = product_c[~product_c.Open.isna()].index[0]
    last = product_c[~product_c.Open.isna()].index[-1]
    product_c = product_c.loc[first:last]

    product_c["Date"] = [datetime.strftime(x, "%Y/%m/%d") for x in product_c.index]
    product_c["Time"] = [datetime.strftime(x, "%H:%M:%S") for x in product_c.index]
    product_c = product_c[
        ["Date", "Time", "Open", "High", "Low", "Close", "TotalVolume"]
    ]
    product_c.loc[product_c.TotalVolume < 0, "TotalVolume"] = 1
    product_c.loc[product_c.TotalVolume.isna(), "TotalVolume"] = -1
    product_c.loc[product_c.TotalVolume != -1, "TotalVolume"] = (
        product_c.loc[product_c.TotalVolume != -1, "TotalVolume"]
        .astype("int")
        .astype("str")
    )
    product_c.loc[product_c.TotalVolume == -1, "TotalVolume"] = None
    correlation = ohlc_corr(product_a, product_b, product_c)
    for i in files:
        os.remove(i)
    return product_c, correlation


def ohlc_corr(product_a, product_b, product_c):
    col = ["Open", "High", "Low", "Close"]
    new = product_c.copy()
    output = {"Comparison": ["New_A", "New_B", "A_B"]}
    for i in col:
        new[i + "_A"] = product_a[i]
        new[i + "_B"] = product_b[i]
        corr = new[[i, i + "_A", i + "_B"]].corr()
        output[i] = list(corr[i].unique()[1:3]) + [corr[i + "_A"].unique()[-1]]
    output = pd.DataFrame(output)
    return output
