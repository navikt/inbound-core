import pandas


def transform(df: pandas.DataFrame) -> pandas.DataFrame:
    df_res = df.copy()
    df_res["test"] = "I'm transformed"
    return df_res
