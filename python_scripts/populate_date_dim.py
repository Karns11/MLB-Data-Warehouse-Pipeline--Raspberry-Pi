# CREATE TABLE warehouse.dim_date (
# 	as_of_date          DATE,
#     date_id             INTEGER,
#     day                 INTEGER,
#     day_suffix          TEXT,
#     day_name            TEXT,
#     weekday_ind         BOOLEAN,
#     weekday_num         INTEGER,
#     week_of_month       INTEGER,
#     week_of_year        INTEGER,
#     iso_week            INTEGER,
#     month               INTEGER,
#     month_name          TEXT,
#     month_abbrev        TEXT,
#     quarter             INTEGER,
#     quarter_name        TEXT,
#     year                INTEGER,
#     iso_year            INTEGER,
#     year_month          TEXT,
#     year_quarter        TEXT,
#     first_day_of_month  DATE,
#     last_day_of_month   DATE,
#     first_day_of_qtr    DATE,
#     last_day_of_qtr     DATE,
#     first_day_of_year   DATE,
#     last_day_of_year    DATE,
#     day_of_year         INTEGER,
#     is_holiday          BOOLEAN,
#     holiday_name        TEXT,
#     fiscal_month        INTEGER,
#     fiscal_quarter      INTEGER,
#     fiscal_year         INTEGER,
#     is_weekend          BOOLEAN,
#     is_last_day_of_month BOOLEAN,
#     is_first_day_of_month BOOLEAN,
#     prev_day            DATE,
#     next_day            DATE,
#     prev_month          TEXT,
#     next_month          TEXT,
#     prev_year           INTEGER,
#     next_year           INTEGER,
#     days_in_month       INTEGER,
#     leap_year           BOOLEAN,
#     iso_day_of_week     INTEGER,
#     unix_timestamp      BIGINT,
#     julian_date         DOUBLE PRECISION,
#     created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# );



import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Generate date range
dates = pd.date_range(start="1900-01-01", end="2100-12-31", freq="D")
df = pd.DataFrame({'as_of_date': dates})

# Add date_id in YYYYMMDD format
df["date_id"] = df["as_of_date"].dt.strftime('%Y%m%d').astype(int)

# Derive fields
df["day"] = df["as_of_date"].dt.day
df["day_suffix"] = df["day"].apply(lambda x: 'th' if 11<=x<=13 else {1:'st',2:'nd',3:'rd'}.get(x%10, 'th'))
df["day_name"] = df["as_of_date"].dt.day_name()
df["weekday_ind"] = df["as_of_date"].dt.weekday < 5
df["weekday_num"] = df["as_of_date"].dt.weekday + 1
df["week_of_month"] = df["as_of_date"].apply(lambda d: (d.day - 1) // 7 + 1)
df["week_of_year"] = df["as_of_date"].dt.isocalendar().week
df["iso_week"] = df["as_of_date"].dt.isocalendar().week
df["month"] = df["as_of_date"].dt.month
df["month_name"] = df["as_of_date"].dt.month_name()
df["month_abbrev"] = df["as_of_date"].dt.strftime('%b')
df["quarter"] = df["as_of_date"].dt.quarter
df["quarter_name"] = df["quarter"].apply(lambda q: f"Q{q}")
df["year"] = df["as_of_date"].dt.year
df["iso_year"] = df["as_of_date"].dt.isocalendar().year
df["year_month"] = df["as_of_date"].dt.strftime("%Y-%m")
df["year_quarter"] = df["as_of_date"].dt.to_period("Q").astype(str)
df["first_day_of_month"] = df["as_of_date"].values.astype('datetime64[M]')
df["last_day_of_month"] = df["first_day_of_month"] + pd.offsets.MonthEnd(0)
df["first_day_of_qtr"] = df["as_of_date"].dt.to_period('Q').apply(lambda p: p.start_time)
df["last_day_of_qtr"] = df["as_of_date"].dt.to_period('Q').apply(lambda p: p.end_time)
df["first_day_of_year"] = df["as_of_date"].dt.to_period('Y').apply(lambda p: p.start_time)
df["last_day_of_year"] = df["as_of_date"].dt.to_period('Y').apply(lambda p: p.end_time)
df["day_of_year"] = df["as_of_date"].dt.dayofyear
df["is_holiday"] = False
df["holiday_name"] = None
df["fiscal_month"] = df["month"]
df["fiscal_quarter"] = df["quarter"]
df["fiscal_year"] = df["year"]
df["is_weekend"] = df["weekday_num"] >= 6
df["is_last_day_of_month"] = df["as_of_date"] == df["last_day_of_month"]
df["is_first_day_of_month"] = df["as_of_date"] == df["first_day_of_month"]
df["prev_day"] = df["as_of_date"] - pd.Timedelta(days=1)
df["next_day"] = df["as_of_date"] + pd.Timedelta(days=1)
df["prev_month"] = (df["as_of_date"] - pd.DateOffset(months=1)).dt.strftime("%Y-%m")
df["next_month"] = (df["as_of_date"] + pd.DateOffset(months=1)).dt.strftime("%Y-%m")
df["prev_year"] = df["year"] - 1
df["next_year"] = df["year"] + 1
df["days_in_month"] = df["as_of_date"].dt.days_in_month
df["leap_year"] = df["as_of_date"].dt.is_leap_year
df["iso_day_of_week"] = df["as_of_date"].dt.isocalendar().day
df["unix_timestamp"] = df["as_of_date"].astype(np.int64) // 10**9
df["julian_date"] = df["as_of_date"].apply(lambda x: x.to_julian_date())
df["created_at"] = pd.Timestamp.now()


engine = create_engine("postgresql+psycopg2://root:root@localhost:5432/baseballr_db") #my_postgres as host if running in container
df.to_sql("dim_date", engine, index=False, if_exists="append", method='multi', schema='warehouse')