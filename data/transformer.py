import pandas


def transform(df: pandas.DataFrame) -> pandas.DataFrame:
    df_res = df.copy()
    df_res["test"] = "testing"
    return df_res
