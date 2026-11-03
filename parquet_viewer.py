import polars as pl

symbol = "AAPL"

lazy_df = pl.scan_parquet("database/raw/AA.parquet")

df = (
    lazy_df
    .select([
        "Date", "Open", "High", "Low", "Close", "AdjClose", "Volume"
    ]).collect()              # Do not collect !! if you want other methods 
                              # to play with data use numpy arrays (<1ms latency)
)


#everything: 1.7052028179168701                     (1.7s)
#without print: 0.0                                 (0s)
#with changed parquet: 0.08300018310546875          (83ms)
#with 2 itmes to print & same symbol+df conversion  (36ms)
