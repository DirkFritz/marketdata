import pandas as pd


pd.options.mode.chained_assignment = None  # default='warn'


class NdxData:
    def __init__(self, stock_data_file, stock_prices_file):
        self.nasdaq_df = pd.read_csv(stock_data_file)
        self.nasdaq_df = self.nasdaq_df.set_index("Symbol")

        self.ndx_prices_df = pd.read_csv(stock_prices_file)
        self.ndx_prices_df = self.ndx_prices_df.drop(["Unnamed: 0"], axis=1)
        self.nasdaq_df = self.nasdaq_df.reset_index()

        self.ndx_prices_df["DateTime"] = pd.to_datetime(
            self.ndx_prices_df["DateTime"], format="%Y-%m-%d %H:%M:%S"
        )

        self.nasdaq100_df = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")[3]

        self.market_cap_period_df = pd.DataFrame()
        self.ticker_symbols = []

        self.market_cap_period_groups = pd.DataFrame(
            columns=["Group", "DateTime", "Market Cap", "Percent", "Average Percent"]
        )

    def get_last_day(self):
        return self.ndx_prices_df["DateTime"].max()

    def set_comparison_group(self, ticker_symbols=[]):

        self.ticker_symbols = ticker_symbols
        ndx100_symbols = self.nasdaq100_df["Ticker"].tolist()
        self.ndx100 = self.nasdaq_df.loc[self.nasdaq_df["Symbol"].isin(ndx100_symbols)]
        self.ndx100_faang = self.ndx100.loc[self.ndx100["Symbol"].isin(ticker_symbols)]
        self.ndx100_no_faang = self.ndx100.loc[
            ~self.ndx100["Symbol"].isin(ticker_symbols)
        ]

        self.ndx100_no_faang_tech = self.ndx100_no_faang.loc[
            self.ndx100_no_faang["Sector"] == "Technology"
        ]
        self.ndx100_no_faang_no_tech = self.ndx100_no_faang.loc[
            self.ndx100_no_faang["Sector"] != "Technology"
        ]

    def set_compare_dates(self, compare_date1, compare_date2):
        self.create_market_cap_period(
            "MANTA", self.ticker_symbols, compare_date1, compare_date2
        )
        self.create_market_cap_period(
            "TECH",
            self.ndx100_no_faang_tech["Symbol"].tolist(),
            compare_date1,
            compare_date2,
        )
        self.create_market_cap_period(
            "OTHERS",
            self.ndx100_no_faang_no_tech["Symbol"].tolist(),
            compare_date1,
            compare_date2,
        )
        self.market_cap_period_df.loc[
            self.market_cap_period_df["Symbol"].isin(["GOOG", "GOOGL"]), ["Market Cap"]
        ] *= 0.5

        self.market_cap_period_df.reset_index(drop=True, inplace=True)
        self.create_market_cap_period_groups("MANTA")
        self.create_market_cap_period_groups("TECH")
        self.create_market_cap_period_groups("OTHERS")
        self.market_cap_period_groups["DateTime"] = pd.to_datetime(
            self.market_cap_period_groups["DateTime"], format="%Y%m%d"
        )

        return self.market_cap_period_groups
   

    def create_market_cap_period(
        self, group_name, symbols_group, compare_date1, compare_date2
    ):

        df = self.nasdaq_df.set_index("Symbol")
        df[df.columns[1]] = (
            df[df.columns[1]].replace("[\$,]", "", regex=True).astype(float)
        )
        df["No Shares"] = df["Market Cap"] / df["Last Sale"]

        for symbol in symbols_group:
            number_shares = df.loc[symbol]["No Shares"]
            stock_prices_start = self.ndx_prices_df[
                (self.ndx_prices_df["DateTime"] == compare_date1)
                & (self.ndx_prices_df["Symbol"] == symbol)
            ]["Price"]
            price_per_stock = self.ndx_prices_df.loc[
                (self.ndx_prices_df["DateTime"] >= compare_date1)
                & (self.ndx_prices_df["DateTime"] <= compare_date2)
                & (self.ndx_prices_df["Symbol"] == symbol)
            ]
            price_per_stock.loc[:, "Market Cap"] = (
                price_per_stock["Price"] * number_shares
            )

            price_per_stock.loc[:, "Percent"] = (
                price_per_stock["Price"] / stock_prices_start.values[0]
            ) * 100 - 100
            price_per_stock["Group"] = group_name

            self.market_cap_period_df = pd.concat(
                [self.market_cap_period_df, price_per_stock]
            )

    def create_market_cap_period_groups(self, group_name):
        dates = self.market_cap_period_df["DateTime"].unique()
        for date in dates:
            group_per_date = self.market_cap_period_df[
                (self.market_cap_period_df["Group"] == group_name)
                & (self.market_cap_period_df["DateTime"] == date)
            ]

            market_cap = group_per_date["Market Cap"].sum()
            percent_average = group_per_date["Percent"].sum() / len(
                group_per_date["Symbol"].unique()
            )

            data_record = [[group_name, date, market_cap, percent_average]]
            self.market_cap_period_groups = pd.concat(
                [
                    self.market_cap_period_groups,
                    pd.DataFrame(
                        data_record,
                        columns=["Group", "DateTime", "Market Cap", "Average Percent"],
                    ),
                ]
            )

        market_cap_start_date = self.market_cap_period_groups[
            (self.market_cap_period_groups["Group"] == group_name)
            & (self.market_cap_period_groups["DateTime"] == dates[0])
        ]["Market Cap"]

        self.market_cap_period_groups.loc[
            self.market_cap_period_groups["Group"] == group_name, "Percent"
        ] = (
            self.market_cap_period_groups[
                (self.market_cap_period_groups["Group"] == group_name)
            ]["Market Cap"]
            / market_cap_start_date
        ) * 100 - 100
