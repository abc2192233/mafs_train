
class ReSize:
    def __init__(self, data, step=60, method='PAA'):
        self._data = data
        self._step = step
        self._metric = None
        self._method = method

        if self._method == 'PAA':
            self.re_index()

    def re_index(self):
        # self._data.set_index('time_stamp', inplace=True)
        self._data = self._data.resample(f'{self._step}S', on='time_stamp').mean()

    def get_data(self):
        return self._data

