import pandas as pd
import numpy as np


def create_df():
    data = pd.DataFrame(np.random.randn(100, 4), columns=list('ABCD'))
    return data


if __name__ == '__main__':
    df = create_df()



