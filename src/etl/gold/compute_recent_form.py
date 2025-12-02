import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import os



PATH = "/app/output"


def main():

    
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        port="5432"
    )
    conn.autocommit = True
    cur = conn.cursor()

    

    # --------------------------------------------------------------
    # Load standings_historical.csv into PostgreSQL
    # --------------------------------------------------------------

    engine = create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:5432/{os.getenv('DB_NAME')}"
    )

    df = pd.read_csv(f"{PATH}/standings_historical.csv")
    df.to_sql("standings_historical", engine, index=False, if_exists="replace")

    # --------------------------------------------------------------
    # STEP 1 — Compute weekly deltas (cumulative → increments)
    # --------------------------------------------------------------
    cur.execute("DROP TABLE IF EXISTS weekly_deltas;")
    cur.execute("""
        CREATE TABLE weekly_deltas AS
        SELECT
            team,
            updated_at,
            (wins         - LAG(wins)         OVER (PARTITION BY team ORDER BY updated_at)) AS wins_inc,
            (draws        - LAG(draws)        OVER (PARTITION BY team ORDER BY updated_at)) AS draws_inc,
            (losses       - LAG(losses)       OVER (PARTITION BY team ORDER BY updated_at)) AS losses_inc,
            (goals_for    - LAG(goals_for)    OVER (PARTITION BY team ORDER BY updated_at)) AS gf_inc,
            (goals_against- LAG(goals_against)OVER (PARTITION BY team ORDER BY updated_at)) AS ga_inc,
            (points       - LAG(points)       OVER (PARTITION BY team ORDER BY updated_at)) AS pts_inc
        FROM standings_historical;
    """)
    print("######################### weekly_deltas")
    print(pd.read_sql_query("""
        SELECT *
        FROM weekly_deltas
    """, conn).head())
    # --------------------------------------------------------------
    # STEP 2 — Expand weekly deltas to *one row per match*
    #          using PostgreSQL generate_series()  ← super simple
    # --------------------------------------------------------------
    cur.execute("DROP TABLE IF EXISTS matches;")
    query = """
    CREATE TABLE matches AS
WITH base AS (
    SELECT
        team,
        updated_at,
        wins_inc,
        draws_inc,
        losses_inc,
        gf_inc,
        ga_inc,
        NULLIF(wins_inc + draws_inc + losses_inc,0) AS match_count
    FROM weekly_deltas
),
combined AS (
    SELECT
        team,
        updated_at,
        'W' AS result,
        3 AS pts,
        gf_inc / match_count AS gf,
        ga_inc / match_count AS ga
    FROM base
    CROSS JOIN generate_series(1, COALESCE(wins_inc, 0)) AS g(n)

    UNION ALL

    SELECT
        team,
        updated_at,
        'D' AS result,
        1 AS pts,
        gf_inc / match_count AS gf,
        ga_inc / match_count AS ga
    FROM base
    CROSS JOIN generate_series(1, COALESCE(draws_inc, 0)) AS g(n)

    UNION ALL

    SELECT
        team,
        updated_at,
        'L' AS result,
        0 AS pts,
        gf_inc / match_count AS gf,
        ga_inc / match_count AS ga
    FROM base
    CROSS JOIN generate_series(1, COALESCE(losses_inc, 0)) AS g(n)
)
SELECT *
FROM combined;
    """
    query = query.replace("\r", "").replace("\t", " ")
    cur.execute(query)

    print("######################### matches")
    print(pd.read_sql_query("""
        SELECT *
        FROM matches
    """, conn).head())

    # put a incrimental rn column 12..38 the matches that are played by each team
    cur.execute("DROP TABLE IF EXISTS ordered;")
    cur.execute("""
    CREATE TABLE ordered AS
    SELECT
        m.team,
        s.updated_at,                        -- weekly snapshot
        m.result,
        m.pts,
        m.gf AS gf_per_match,
        m.ga AS ga_per_match,
        ROW_NUMBER() OVER (
            PARTITION BY m.team, s.updated_at
            ORDER BY m.updated_at DESC
        ) AS rn
    FROM matches m
    JOIN standings_historical s
        ON s.team = m.team
    AND m.updated_at <= s.updated_at;     -- match happened before that weekly snapshot

    """)
    print("######################### ordered")
    print(pd.read_sql_query("""
        SELECT *
        FROM ordered
    """, conn).head())

    # get last 5 matches of each team
    cur.execute("DROP TABLE IF EXISTS last5;")
    cur.execute("""
        CREATE TABLE last5 AS
        SELECT *
        FROM ordered
        WHERE rn <= 5;
    """)

    print("######################### last5")
    print(pd.read_sql_query("""
        SELECT *
        FROM last5
    """, conn).head())

    # For each week we agregate the scores for each team up to 5 mathes
    cur.execute("DROP TABLE IF EXISTS agg;")
    cur.execute("""
        CREATE TABLE agg AS
        SELECT
            updated_at,
            team,
            SUM(pts)              AS last5_points,
            SUM(gf_per_match)     AS goals_for,
            SUM(ga_per_match)     AS goals_against,
            STRING_AGG(result, '' ORDER BY rn DESC) AS form,
            COUNT(*)              AS match_count
        FROM last5
        GROUP BY updated_at, team
        HAVING COUNT(*) = 5;
    """)

    print("######################### agg")
    print(pd.read_sql_query("""
        SELECT *
        FROM agg
    """, conn).head())

    # Ranking -> for each week we calculate ranking 1-20 depending
    # on the previous aggregations 
    cur.execute("DROP TABLE IF EXISTS ranked;")
    cur.execute("""
        CREATE TABLE ranked AS
        SELECT
            updated_at,
            team,
            goals_for,
            goals_against,
            form,
            last5_points,
            ROW_NUMBER() OVER (
                PARTITION BY updated_at
                ORDER BY
                    last5_points DESC,
                    (goals_for - goals_against) DESC,
                    goals_against ASC,
                    team ASC
            ) AS form_ranking
        FROM agg;
    """)

    print("######################### ranked")
    print(pd.read_sql_query("""
        SELECT *
        FROM ranked
    """, conn).head())

    # Finally get the best 3 teams in the specific week
    df_out = pd.read_sql_query("""
        SELECT *
        FROM ranked
        WHERE form_ranking <= 3
        ORDER BY updated_at, form_ranking;
    """, conn)

    print(df_out.head())


    df_out.to_csv(f"{PATH}/recent_form.csv", index=False)

    print(f"✔ Saved: {PATH}/recent_form.csv")


if __name__ == "__main__":
    main()
