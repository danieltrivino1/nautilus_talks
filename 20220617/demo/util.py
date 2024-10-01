from typing import List

import pandas as pd
from nautilus_trader.core.nautilus_pyo3 import nanos_to_secs
from nautilus_trader.model.data import Bar, BarSpecification, DataType, BarType
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.identifiers import InstrumentId


def make_bar_type(instrument_id: InstrumentId, bar_spec) -> BarType:
    return BarType(instrument_id=instrument_id, bar_spec=bar_spec, aggregation_source=AggregationSource.EXTERNAL)


def one(iterable):
    if len(iterable) == 0:
        return None
    elif len(iterable) > 1:
        raise AssertionError("Too many values")
    else:
        return iterable[0]


def bars_to_dataframe(source_id: str, source_bars: List[Bar], target_id: str, target_bars: List[Bar]) -> pd.DataFrame:
    def _bars_to_frame(bars, instrument_id):
        df = pd.DataFrame([t.to_dict(t) for t in bars]).astype({"close": float})
        return df.assign(instrument_id=instrument_id).set_index(["instrument_id", "ts_init"])

    ldf = _bars_to_frame(bars=source_bars, instrument_id=source_id)
    rdf = _bars_to_frame(bars=target_bars, instrument_id=target_id)
    data = pd.concat([ldf, rdf])["close"].unstack(0).sort_index().fillna(method="ffill")
    return data.dropna()


def human_readable_duration(ns: float):
    from dateutil.relativedelta import relativedelta  # type: ignore

    seconds = nanos_to_secs(ns)
    delta = relativedelta(seconds=seconds)
    attrs = ["months", "days", "hours", "minutes", "seconds"]
    return ", ".join(
        [
            f"{getattr(delta, attr)} {attr if getattr(delta, attr) > 1 else attr[:-1]}"
            for attr in attrs
            if getattr(delta, attr)
        ]
    )
