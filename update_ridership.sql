-- use to update ridership using new stop level ridership (APC) numbers from SEPTA
--download new bus and trolley ridership shapefiles from SEPTA GIS portal
--use postgis shapefile importer to load into DB
CREATE TABLE surfacetransit_loads_2019 AS(
    WITH tblA AS (
    SELECT
        stop_id,
        stop_name,
        route,
        direction,
        sequence,
        sign_up,
        mode,
        source,
        weekday_bo,
        weekday_le,
        weekday_bo - weekday_le change,
        geom
    FROM bus_ridership_spring2019
    ),
        tblB AS (
        SELECT
            *,
            SUM(weekday_bo) OVER (PARTITION BY route, direction ORDER BY sequence) weekday_tbo,
            SUM(weekday_le) OVER (PARTITION BY route, direction ORDER BY sequence) weekday_tle
        FROM tblA
        ORDER BY sequence
        ),
    tblC AS (
        SELECT
            stop_id,
            stop_name,
            route,
            direction,
            sequence,
            sign_up,
            mode,
            source,
            weekday_bo,
            weekday_le,
            weekday_bo - weekday_le change,
            geom
        FROM trolley_ridership_spring2018
        ),
    tblD AS (
        SELECT
            *,
            SUM(weekday_bo) OVER (PARTITION BY route, direction ORDER BY sequence) weekday_tbo,
            SUM(weekday_le) OVER (PARTITION BY route, direction ORDER BY sequence) weekday_tle
        FROM tblC
        ORDER BY sequence
        ),
    tblE AS(
        SELECT
            *,
            weekday_tbo - weekday_tle weekday_lo
        FROM tblB
        --ORDER BY route, direction, sequence

        UNION ALL

        SELECT
            *,
            weekday_tbo - weekday_tle weekday_lo
        FROM tblD
        )
    SELECT *
    FROM tblE
    ORDER BY route, direction, sequence
);
COMMIT;