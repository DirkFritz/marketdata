from db import Db
import pandas as pd


db = Db()

data_db = db.select_historic()
stocks_db_df = pd.DataFrame(
        columns=["Symbol", "DateTime", "Open", "Close", "High", "Low", "Volume"]
    )
stocks_db_df = pd.concat(
        [
            stocks_db_df,
            pd.DataFrame(
                data_db,
                columns=[
                    "Symbol",
                    "DateTime",
                    "Open",
                    "Close",
                    "High",
                    "Low",
                    "Volume",
                ],
            ),
        ]
    )
db.close()

print(stocks_db_df)

stocks_db_df.to_csv("ndx_backup.csv")

