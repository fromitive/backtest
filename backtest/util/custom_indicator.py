from backtest.module_compet.pandas import pd
import numpy as np


def twin_range_filter(data: pd.DataFrame) -> pd.DataFrame:
    # Inputs
    per1 = 27
    mult1 = 1.6
    per2 = 55
    mult2 = 2

    # Smooth Average Range
    def smoothrng(x, t, m):
        wper = t * 2 - 1
        avrng = x.diff().abs().ewm(span=t, adjust=False).mean()
        smoothrng_result = avrng.ewm(span=wper, adjust=False).mean() * m
        return smoothrng_result

    data["smrng1"] = smoothrng(data["close"], per1, mult1)
    data["smrng2"] = smoothrng(data["close"], per2, mult2)
    data["smrng"] = (data["smrng1"] + data["smrng2"]) / 2

    # Range Filter
    def rngfilt(x, r):
        rngfilt = [x.iloc[0]]
        for i in range(1, len(x)):
            x_val = x.iloc[i]
            r_val = r.iloc[i]
            prev_filt = rngfilt[-1]
            if x_val > prev_filt:
                if x_val - r_val < prev_filt:
                    rngfilt.append(prev_filt)
                else:
                    rngfilt.append(x_val - r_val)
            elif x_val + r_val > prev_filt:
                rngfilt.append(prev_filt)
            else:
                rngfilt.append(x_val + r_val)
        return pd.Series(rngfilt, index=x.index)

    data["filt"] = rngfilt(data["close"], data["smrng"])

    data["upward"] = 0.0
    data["downward"] = 0.0

    for i in range(1, len(data)):
        if data["filt"].iat[i] > data["filt"].iat[i - 1]:
            data["upward"].iat[i] = data["upward"].iat[i - 1] + 1
        elif data["filt"].iat[i] < data["filt"].iat[i - 1]:
            data["upward"].iat[i] = 0

    for i in range(1, len(data)):
        if data["filt"].iat[i] < data["filt"].iat[i - 1]:
            data["downward"].iat[i] = data["downward"].iat[i - 1] + 1
        elif data["filt"].iat[i] > data["filt"].iat[i - 1]:
            data["downward"].iat[i] = 0

    # source > filt and source > source[1] and upward > 0
    long_cond = (data["close"] > data["filt"]) & (data["close"] > data["close"].shift(1)) & (data["upward"] > 0) | (
        data["close"] > data["filt"]
    ) & (data["close"] < data["close"].shift(1)) & (data["upward"] > 0)
    short_cond = (data["close"] < data["filt"]) & (data["close"] < data["close"].shift(1)) & (data["downward"] > 0) | (
        data["close"] < data["filt"]
    ) & (data["close"] > data["close"].shift(1)) & (data["downward"] > 0)

    data["CondIni"] = np.where(long_cond, 1, np.where(short_cond, -1, np.NAN))
    data["CondIni"].fillna(method="ffill", inplace=True)

    data["long"] = long_cond & (data["CondIni"].shift(1) == -1)
    data["short"] = short_cond & (data["CondIni"].shift(1) == 1)

    return data[["long", "short"]]


def trendilo(data: pd.DataFrame) -> pd.DataFrame:
    # Inputs from the user
    smooth = 1
    length = 25
    offset = 0.85
    sigma = 6
    bmult = 1.0
    cblen = False
    blen = 20

    def alma(series, window, offset, sigma):
        m = round(offset * (window - 1))
        s = series.rolling(window).apply(
            lambda x: sum(np.exp(-((m - i) ** 2) / (2 * sigma * sigma)) * x[i] for i in range(window))
            / sum(np.exp(-((m - i) ** 2) / (2 * sigma * sigma)) for i in range(window)),
            raw=True,
        )
        return s

    # Logic for calculations
    data["pch"] = data["close"].diff(smooth) / data["close"] * 100
    data["avpch"] = alma(data["pch"], length, offset, sigma)

    blength = blen if cblen else length
    data["rms"] = bmult * np.sqrt((data["avpch"] ** 2).rolling(blength).mean())

    return data[["avpch", "rms"]]
