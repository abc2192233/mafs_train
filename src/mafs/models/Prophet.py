from kats.models.prophet import ProphetModel, ProphetParams
from kats.consts import TimeSeriesData


class MAFSProphetModel:
    def __init__(self, data):
        # self._data = data.columns = ["time", "value"]
        self._data = TimeSeriesData(value=data.value, time=data.index)

    def predict(self, step=300):
        params = ProphetParams()
        m = ProphetModel(self._data, params)
        m.fit()

        steps = int(10 * 24 * 60 * 60 / step)

        forecast = m.predict(steps=steps, freq=f"{step}S", include_history=True)
        m.plot()
        print(forecast)
