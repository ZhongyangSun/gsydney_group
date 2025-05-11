with engine.begin() as conn:
    conn.exec_driver_sql(
        """
        -- Drop and recreate metrics view
        DROP VIEW IF EXISTS analysis.vw_sa2_metrics CASCADE;
        CREATE VIEW analysis.vw_sa2_metrics AS
        SELECT
            s.sa2_code21,
            s.sa4_name21,
            COALESCE(b.biz_per_1k,        0) AS business,
            COALESCE(t.stops_raw,         0) AS stops,
            COALESCE(p.poi_raw,           0) AS poi,
            COALESCE(sch.schools_per_1k_youth, 0) AS schools,
            pop.total_people
        FROM raw.sa2_my               AS s
        LEFT JOIN analysis.vw_metric_business AS b USING (sa2_code21)
        LEFT JOIN analysis.vw_metric_stops    AS t USING (sa2_code21)
        LEFT JOIN analysis.vw_metric_poi      AS p USING (sa2_code21)
        LEFT JOIN analysis.vw_metric_schools  AS sch USING (sa2_code21)
        LEFT JOIN raw.population_my           AS pop USING (sa2_code21)
        WHERE pop.total_people >= 100;

        -- Drop and recreate z-score view (includes SA4 name now)
        DROP VIEW IF EXISTS analysis.vw_sa2_zscores CASCADE;
        CREATE VIEW analysis.vw_sa2_zscores AS
        SELECT
            sa2_code21,
            sa4_name21,
            business,
            stops,
            poi,
            schools,
            (business - AVG(business) OVER ()) / NULLIF(STDDEV_POP(business) OVER (), 0) AS z_business,
            (stops    - AVG(stops)    OVER ()) / NULLIF(STDDEV_POP(stops)    OVER (), 0) AS z_stops,
            (poi      - AVG(poi)      OVER ()) / NULLIF(STDDEV_POP(poi)      OVER (), 0) AS z_poi,
            (schools  - AVG(schools)  OVER ()) / NULLIF(STDDEV_POP(schools)  OVER (), 0) AS z_schools
        FROM analysis.vw_sa2_metrics;

        -- Final scores table: z_total and sigmoid score
        DROP TABLE IF EXISTS analysis.sa2_scores;
        CREATE TABLE analysis.sa2_scores AS
        SELECT
            sa2_code21,
            sa4_name21,
            z_business,
            z_stops,
            z_poi,
            z_schools,
            (z_business + z_stops + z_poi + z_schools) AS z_total,
            1 / (1 + EXP(-(z_business + z_stops + z_poi + z_schools))) AS score
        FROM analysis.vw_sa2_zscores;

        -- Add primary key and index
        ALTER TABLE analysis.sa2_scores
            ADD CONSTRAINT sa2_scores_pk PRIMARY KEY (sa2_code21);

        CREATE INDEX IF NOT EXISTS idx_sa2_scores_score
            ON analysis.sa2_scores (score);
        """
    )

print("✅ analysis.sa2_scores table created successfully.")