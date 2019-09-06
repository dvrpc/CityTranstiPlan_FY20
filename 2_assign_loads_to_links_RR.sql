--this is script 2 for RR, which should actually be run after the RR script labeled #3
--this script assigns loads to model links in sql

--BEFORE RUNNING THIS -- run 3_fill_in_fromto_RR to enable this join

CREATE TABLE loaded_rail_links AS(
    WITH tblA AS(
        SELECT 
            l.*,
            p.portion
        FROM lineroutes_linkseq l
        INNER JOIN lrid_portions p
        ON l.lrid = p.lrid
        )
    SELECT 
        a.*,
        r.loads::BIGINT,
        (r.loads::BIGINT*a.portion) AS load_portion
    FROM regionalrail_loads_fromto r
    RIGHT JOIN tblA a
    ON a.fromto::BIGINT = r.fromto
    AND a.linename = r.linename
    WHERE a.lrname LIKE 'sepr%'
    ORDER BY linename, lrid, lrseq
    );
COMMIT;

--this get's wonky on the trunk lines in/towards the city
--see what it looks like if I just apply what I have without doing the loads drop down (py script)

--Assumption: ridership distributed across line routes by number of vehicle journeys
--Assumption: if more than one stop is on a link (sometimes up to 6), the load is averaged - it is usually very similar



---AFTER PYTHON
--summarize and join to geometries to view
--line level results -----NOT VERY USEFUL FOR RAIL
CREATE TABLE loaded_rail_links_linelevel AS(
    WITH tblA AS(
        SELECT 
            no,
            CONCAT(CAST(fromnodeno AS text), CAST(tonodeno AS text)) AS fromto,
            r_no,
            CONCAT(CAST("r_fromno~1" AS text), CAST(r_tonodeno AS text)) AS r_fromto,
            geom
        FROM "2015base_link"
        ),
    tblB AS(
        SELECT
            lrid,
            tsys,
            linename,
            direction,
            stopsserved,
            numvehjour,
            fromto,
            COUNT(fromto) AS times_used,
            SUM(CAST(load_portion AS numeric)) AS total_load
        FROM loaded_rail_links
        WHERE tsys = 'RR'
        GROUP BY lrid, tsys, linename, direction, stopsserved, numvehjour, fromto
        ),
    tblC AS(
        SELECT
            b.*,
            a.geom,
            aa.geom AS geom2
        FROM tblB b
        LEFT JOIN tblA a
        ON b.fromto = a.fromto
        LEFT JOIN tblA aa
        ON b.fromto = aa.r_fromto
    )
    SELECT
        lrid,
        tsys,
        linename,
        direction,
        stopsserved,
        numvehjour,
        fromto,
        times_used,
        ROUND(total_load, 0),
        CASE WHEN geom IS NULL THEN geom2
            ELSE geom
            END
            AS geometry
    FROM tblC);
COMMIT;

--aggregate further (and loose line level attributes) for segment level totals

CREATE TABLE loaded_rail_links_segmentlevel AS(
    WITH tblA AS(
        SELECT 
            no,
            CONCAT(CAST(fromnodeno AS text), CAST(tonodeno AS text)) AS fromto,
            r_no,
            CONCAT(CAST("r_fromno~1" AS text), CAST(r_tonodeno AS text)) AS r_fromto,
            geom
        FROM "2015base_link"
        ),
    tblB AS(
        SELECT
            fromto,
            COUNT(fromto) AS times_used,
            SUM(CAST(load_portion AS numeric)) AS total_load
        FROM loaded_rail_links
        WHERE tsys = 'RR'
        GROUP BY fromto
        ),
    tblC AS(
        SELECT
            b.*,
            a.geom,
            aa.geom AS geom2
        FROM tblB b
        LEFT JOIN tblA a
        ON b.fromto = a.fromto
        LEFT JOIN tblA aa
        ON b.fromto = aa.r_fromto
    )
    SELECT
        fromto,
        times_used,
        ROUND(total_load,0),
        CASE WHEN geom IS NULL THEN geom2
            ELSE geom
            END
            AS geometry
    FROM tblC);
COMMIT;
