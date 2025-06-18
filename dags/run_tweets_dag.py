from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from sqlalchemy import create_engine
import tweepy
from datetime import datetime, timedelta
import pandas as pd

TRIPLE_CROWN_TWEET_GO = 1

def tweet_triple_crown_watch():
    pg_user = os.getenv("MY_PG_USER")
    pg_password = os.getenv("MY_PG_PASSWORD")
    pg_db = os.getenv("MY_PG_DB")
    pg_host = os.getenv("MY_PG_HOST")
    pg_port = os.getenv("MY_PG_PORT")


    api_key = os.getenv("X_API_KEY")
    api_secret = os.getenv("X_API_KEY_SECRET")
    access_token = os.getenv("X_ACCESS_TOKEN")
    access_token_secret = os.getenv("X_ACCESS_TOKEN_SECRET")

    client = tweepy.Client(consumer_key=api_key, consumer_secret=api_secret,
                        access_token=access_token, access_token_secret=access_token_secret)
    
    response = client.get_me()
    print(response.data)


    engine = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}") #my_postgres as host if running in container

    daily_triple_crown_query = """
    SELECT 
        dim_player.full_name,
        dim_team.team_full_name,
        fact_team_gp.gp as team_gps,
        CASE 
            WHEN fact.pa >= 3.1 * fact_team_gp.gp THEN 1
            ELSE 0
        END AS qualified_hitter,
        dim_date.year_quarter,
        fact.*,
        dim_team.league_name
    FROM warehouse.fact_player_stats_history fact
    JOIN warehouse.dim_player dim_player
        ON dim_player.player_sk = fact.player_sk
    JOIN warehouse.dim_team dim_team
        on dim_team.team_sk = fact.team_sk
    JOIN warehouse.fact_team_games fact_team_gp
        on fact_team_gp.team_sk = fact.team_sk
        and fact_team_gp.as_of_date = fact.as_of_date
    JOIN warehouse.dim_date dim_date
        ON dim_date.as_of_date = fact.as_of_date
    WHERE 1=1
        AND fact.as_of_date = (SELECT MAX(as_of_date) FROM warehouse.fact_player_stats_history)
        AND (CASE 
            WHEN fact.pa >= 3.1 * fact_team_gp.gp THEN 1
            ELSE 0
        END = 1)
    ORDER BY fact.batting_avg DESC
    """


    df = pd.read_sql(daily_triple_crown_query, engine)

    #Batting avg tweet
    df["batting_avg"] = df["batting_avg"].astype(float)


    as_of_date = pd.to_datetime(df["as_of_date"].max()).strftime("%B %d, %Y")  


    al_top3 = df[df["league_name"] == "American League"].nlargest(3, "batting_avg")
    nl_top3 = df[df["league_name"] == "National League"].nlargest(3, "batting_avg")

    def format_top3(league_df):
        lines = []
        for i, row in enumerate(league_df.itertuples(index=False), start=1):
            avg_str = f"{row.batting_avg:.3f}".lstrip("0")  
            lines.append(f"{i}. {row.full_name} ({avg_str})")
        return lines


    tweet_lines = [
        f"🔥 Triple Crown Watch As Of {as_of_date} 🔥",
        "",
        "Batting Avg Leaders",
        "",
        "American League:",
        *format_top3(al_top3),
        "",
        "National League:",
        *format_top3(nl_top3)
    ]

    tweet_text = "\n".join(tweet_lines)

    if TRIPLE_CROWN_TWEET_GO == 1:
        try:
            response = client.create_tweet(text=tweet_text)
            tweet_id = response.data["id"]
            print(f"Tweet posted successfully! Tweet ID: {tweet_id}")
        except Exception as e:
            print("Error posting tweet:", e)
    else:
        print("Triple crown tweets turned off")


    #HR tweet
    df["num_hr"] = df["num_hr"].astype(int)


    as_of_date = pd.to_datetime(df["as_of_date"].max()).strftime("%B %d, %Y")  


    al_top3 = df[df["league_name"] == "American League"].nlargest(3, "num_hr")
    nl_top3 = df[df["league_name"] == "National League"].nlargest(3, "num_hr")

    def format_top3(league_df):
        lines = []
        for i, row in enumerate(league_df.itertuples(index=False), start=1):
            avg_str = f"{row.num_hr}"  
            lines.append(f"{i}. {row.full_name} ({avg_str})")
        return lines


    tweet_lines = [
        f"🔥 Triple Crown Watch As Of {as_of_date} 🔥",
        "",
        "HR Leaders",
        "",
        "American League:",
        *format_top3(al_top3),
        "",
        "National League:",
        *format_top3(nl_top3)
    ]

    tweet_text = "\n".join(tweet_lines)


    if TRIPLE_CROWN_TWEET_GO == 1:
        try:
            response = client.create_tweet(text=tweet_text)
            tweet_id = response.data["id"]
            print(f"Tweet posted successfully! Tweet ID: {tweet_id}")
        except Exception as e:
            print("Error posting tweet:", e)
    else:
        print("Triple crown tweets turned off")



    #RBI tweet
    df["num_rbi"] = df["num_rbi"].astype(int)


    as_of_date = pd.to_datetime(df["as_of_date"].max()).strftime("%B %d, %Y")  


    al_top3 = df[df["league_name"] == "American League"].nlargest(3, "num_rbi")
    nl_top3 = df[df["league_name"] == "National League"].nlargest(3, "num_rbi")

    def format_top3(league_df):
        lines = []
        for i, row in enumerate(league_df.itertuples(index=False), start=1):
            avg_str = f"{row.num_rbi}"  
            lines.append(f"{i}. {row.full_name} ({avg_str})")
        return lines


    tweet_lines = [
        f"🔥 Triple Crown Watch As Of {as_of_date} 🔥",
        "",
        "RBI Leaders",
        "",
        "American League:",
        *format_top3(al_top3),
        "",
        "National League:",
        *format_top3(nl_top3)
    ]

    tweet_text = "\n".join(tweet_lines)


    if TRIPLE_CROWN_TWEET_GO == 1:
        try:
            response = client.create_tweet(text=tweet_text)
            tweet_id = response.data["id"]
            print(f"Tweet posted successfully! Tweet ID: {tweet_id}")
        except Exception as e:
            print("Error posting tweet:", e)
    else:
        print("Triple crown tweets turned off")
    

#Dag definition
default_args = {
    'owner': 'nathan',
}

dag = DAG(
    'run_tweetpy_script',
    default_args=default_args,
    description='Run python script to tweet triple crown watch every day at 11 am',
    schedule_interval='0 15 * * *', #None
    start_date=datetime(2025, 6, 17),
    catchup=False
)

run_tweetpy_script= PythonOperator(
        task_id=f"run_tweetpy_script",
        python_callable=tweet_triple_crown_watch,
        dag=dag
    )

run_tweetpy_script