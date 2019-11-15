--this is script 2, which calls for script 3 to be run in the middle
--this script assigns loads to model links in sql

--messing
WITH tblA AS(
SELECT lrid, tsys, UNNEST(fromnodeseq) AS fromn, UNNEST(tonodeseq) AS ton
FROM lineroutes
LIMIT 100
)
SELECT lrid, tsys, CONCAT (fromn,ton) AS fromto
FROM tblA


SELECT spid, gtfsid, linkno, fromonode, int(stoppoints.tonode)
FROM stoppoints
WHERE gtfsid <> 0

-- need rank column for line routes to use a number to identify the fromto links in order for each line route
-- need to create an unnested intermediate table, then can add a new SERIAL identifier which will be in the correct order (call it order)
CREATE TABLE lineroutes_unnest AS(
	WITH tblA AS(
		SELECT lrid, tsys, linename, lrname, direction, stopsserved, numvehjour, UNNEST(fromnodeseq) AS fromn, UNNEST(tonodeseq) AS ton
		FROM lineroutes)
	SELECT lrid, tsys, linename, lrname, direction, stopsserved, numvehjour, CONCAT (fromn,ton) AS fromto
	FROM tblA);
COMMIT;

ALTER TABLE lineroutes_unnest
ADD COLUMN total_order  SERIAL;
COMMIT;

CREATE TABLE lineroutes_linkseq AS(
    SELECT
        lrid,
        tsys,
        linename,
        lrname,
        direction,
        stoppsserved,
        numvehjour,
        fromto,
        RANK() OVER(
            PARTITION BY lrid
            ORDER BY total_order
            ) lrseq
    FROM lineroutes_unnest
    );
COMMIT;

--also need to split out LR GTFSid seq and create rank column too
CREATE TABLE lineroutes_unnest_gtfs AS(
    SELECT lrid, tsys, linename, lrname, direction, stopsserved, numvehjour, UNNEST(gtfsidseq) AS gtfs
    FROM lineroutes
    );
COMMIT;

ALTER TABLE lineroutes_unnest_gtfs
ADD COLUMN total_order SERIAL;
COMMIT;

CREATE TABLE lineroutes_gtfs AS(
    SELECT
        lrid,
        tsys,
        linename,
        lrname,
        direction,
        stopsserved,
        numvehjour,
        gtfs,
        RANK() OVER(
            PARTITION BY lrid
            ORDER BY total_order
            ) gtfsseq
    FROM lineroutes_unnest_gtfs
    );
COMMIT;

-- divide ridership across line routes by number of vehicle journeys (evenly to start)
CREATE TABLE lrid_portions AS(
	WITH tblA AS(
		SELECT 
			linename,
			CAST(SUM(numvehjour) as NUMERIC)
		FROM lineroutes
		GROUP BY linename
		
		),
	tblB AS(
		SELECT 
			lrid,
			linename,
			CAST(numvehjour as NUMERIC)
		FROM lineroutes
		)
	SELECT
		tblB.lrid,
		tblB.linename,
		ROUND((tblB.numvehjour/tblA.sum),2) portion
	FROM tblB
	INNER JOIN tblA
	ON tblA.linename = tblB.linename
	WHERE tblA.sum <> 0
	ORDER BY linename, lrid
	);
COMMIT;

------------------------------------ABOVE HERE ALREADY INCLUDES ALL RAIL---------------------------------------------------------------------

--get stoppoints ready to join to line route links with fromto field 
--first manually updated 7 recrods; tonode field had 2 values. In each case, one was a repeat of the fromnode, so it was removed.
--then line up stop points with links they are on and the portion of the passenger load they should receive
CREATE TABLE linkseq_withloads AS(
    WITH tblA AS(
        SELECT spid, gtfsid, linkno, CONCAT(fromonode, CAST(tonode AS numeric)) AS fromto
        FROM stoppoints
        WHERE gtfsid <> 0
        ),
    tblB AS(
        SELECT 
            l.*,
            p.portion
        FROM lineroutes_linkseq l
        INNER JOIN lrid_portions p
        ON l.lrid = p.lrid
        ),
    tblC AS(
        SELECT
            l.lrid,
            l.tsys,
            l.linename,
            l.lrname,
            l.direction,
            l.stopsserved,
            l.numvehjour,
            l.fromto,
            l.lrseq,
            l.portion,
            a.spid, 
            a.gtfsid,
            a.linkno
        FROM tblB l
        LEFT JOIN tblA a
        ON a.fromto = l.fromto
        ORDER BY lrid, lrseq
        ),
    tblD AS(
        SELECT *
        FROM surfacetransit_loads
        WHERE weekday_lo > 0
        )
    SELECT
        c.*,
        d.weekday_lo,
        (d.weekday_lo*c.portion) AS load_portion
    FROM tblC c
    LEFT JOIN tblD d
    ON c.gtfsid = d.stop_id
    AND c.linename = d.route
    WHERE c.lrname LIKE 'sepb%'
    ORDER BY lrid, lrseq
    );
COMMIT;

--Assumption: ridership distributed across line routes by number of vehicle journeys
--Assumption: if more than one stop is on a link (sometimes up to 6), the load is averaged - it is usually very similar


--clean up repeats from links that have multiple stops (average loads)
--requires losing detail on gtfsid, but can always get it from the previous table
CREATE TABLE linkseq_cleanloads AS(
	WITH tblA AS(
		SELECT lrid, tsys, linename, direction, stopsserved, numvehjour, fromto, lrseq, COUNT(DISTINCT(gtfsid)), sum(load_portion)
		FROM linkseq_withloads
		GROUP BY lrid, tsys, linename, direction, stopsserved, numvehjour, fromto, lrseq
	)
	SELECT 
		lrid,
		tsys, 
		linename,
		direction,
		stopsserved,
		numvehjour,
		fromto,
		lrseq,
		count,
		sum/count AS load_portion_avg
	FROM tblA
	ORDER BY lrid, lrseq
	);
COMMIT;

--now assign loads to links between stop points in Python
--using script: fill_in_linkloads.py

---AFTER PYTHON
--summarize and join to geometries to view
--line level results
CREATE TABLE loaded_links_linelevel AS(
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
            SUM(CAST(load_portion_avg AS numeric)) AS total_load
        FROM loaded_links
        WHERE tsys = 'Bus'
        OR tsys = 'Trl'
        OR tsys = 'LRT'
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

CREATE TABLE loaded_links_segmentlevel AS(
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
            SUM(CAST(load_portion_avg AS numeric)) AS total_load
        FROM loaded_links
        WHERE tsys = 'Bus'
        OR tsys = 'Trl'
        OR tsys = 'LRT'
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
