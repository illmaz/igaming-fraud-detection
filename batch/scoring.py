from pyspark.sql import functions as F


def score(df, flag_col, target):
    tp = df.filter((F.col(flag_col) == True) & (F.col("fraud_type") == target)).count()
    fp = df.filter((F.col(flag_col) == True) & (F.col("fraud_type") != target)).count()
    fn = df.filter((F.col(flag_col) == False) & (F.col("fraud_type") == target)).count()
    return tp, fp, fn
