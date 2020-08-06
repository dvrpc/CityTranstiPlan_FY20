--this is script 2, which calls for script 3 to be run in the middle
--this script assigns loads to model links in sql
'''
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
'''

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
-- UPDATE July 7, 2020: table names with "july" at end were updated on this date to redistribute load portions considering direction
-- UPDATE August 6, 2020: table names with "rider2019" at end were updated to incorporate spring 2019 bus and spring 2018 trolley ridership from SEPTA (downloaded on 8/6/20)

CREATE TABLE lrid_portions_rider2019 AS(
	WITH tblA AS(
		SELECT 
			linename,
            direction,
			CAST(SUM(numvehjour) as NUMERIC)
		FROM lineroutes
		GROUP BY linename, direction
		
		),
	tblB AS(
		SELECT 
			lrid,
			linename,
            direction,
			CAST(numvehjour as NUMERIC)
		FROM lineroutes
		)
	SELECT
		tblB.lrid,
		tblB.linename,
        tblB.direction,
		ROUND((tblB.numvehjour/tblA.sum),2) portion
	FROM tblB
	INNER JOIN tblA
	ON tblA.linename = tblB.linename
    AND tblA.direction = tblB.direction
	WHERE tblA.sum <> 0
	ORDER BY linename, lrid
	);
COMMIT;

------------------------------------ABOVE HERE ALREADY INCLUDES ALL RAIL---------------------------------------------------------------------

--update concatenated text tonode fields to allow for future joining
--in each case, one of the values was the same as the from node, so the tonode value was replaced with the remaining value
--first update for where the fromnode matches the 2nd value in the concatenated tonode
WITH tblA AS(
	SELECT 
		spid,
		gtfsid,
		spname,
		fromonode,
		tonode,
		SPLIT_PART(tonode, ',', 1) as tn1,
		SPLIT_PART(tonode, ',', 2) as tn2
	FROM stoppoints
	WHERE tonode LIKE '%,%'
	ORDER by fromonode DESC
	),
tblB AS(
	SELECT *
	FROM tblA
	WHERE fromonode = CAST(tn1 AS numeric)
	OR fromonode = CAST(tn2 AS numeric)
	)
UPDATE stoppoints
SET tonode = tn1
FROM tblA
WHERE stoppoints.fromonode = CAST(tblA.tn2 AS numeric)

--then update for where the fromnode matches the 1st value in the concatenated tonode
WITH tblA AS(
	SELECT 
		spid,
		gtfsid,
		spname,
		fromonode,
		tonode,
		SPLIT_PART(tonode, ',', 1) as tn1,
		SPLIT_PART(tonode, ',', 2) as tn2
	FROM stoppoints
	WHERE tonode LIKE '%,%'
	ORDER by fromonode DESC
	),
tblB AS(
	SELECT *
	FROM tblA
	WHERE fromonode = CAST(tn1 AS numeric)
	OR fromonode = CAST(tn2 AS numeric)
	)
UPDATE stoppoints
SET tonode = tn2
FROM tblA
WHERE stoppoints.fromonode = CAST(tblA.tn1 AS numeric)


--get stoppoints ready to join to line route links with fromto field 
--first manually updated 7 recrods; tonode field had 2 values. In each case, one was a repeat of the fromnode, so it was removed.
--then line up stop points with links they are on and the portion of the passenger load they should receive
CREATE TABLE linkseq_withloads_bus_rider2019 AS(
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
        INNER JOIN lrid_portions_rider2019 p
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
        --for buses only (will repeat later for trolleys)
        WHERE l.tsys = 'Bus'
        ORDER BY lrid, lrseq
        ),
    tblD AS(
        SELECT *
        FROM surfacetransit_loads_2019
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

--repeating above for Trolleys
CREATE TABLE linkseq_withloads_trl_rider2019 AS(
    WITH tblA AS(
        SELECT spid, gtfsid, linkno, CONCAT(fromonode, CAST(tonode AS numeric)) AS fromto
        FROM stoppoints
        ),
    tblB AS(
        SELECT 
            l.*,
            p.portion
        FROM lineroutes_linkseq l
        INNER JOIN lrid_portions_rider2019 p
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
        --for trolleys only
        WHERE l.tsys = 'Trl' OR l.tsys = 'LRT'
        ORDER BY lrid, lrseq
        ),
    tblD AS(
        SELECT *
        FROM surfacetransit_loads_2019
        WHERE weekday_lo > 0
        )
    SELECT
        c.*,
        d.weekday_lo,
        (d.weekday_lo*c.portion) AS load_portion
    FROM tblC c
    LEFT JOIN tblD d
    ON c.spid = (d.stop_id + 100000)
    AND c.linename = d.route
    WHERE c.lrname LIKE 'sepb%'
    ORDER BY lrid, lrseq
    );
COMMIT;

CREATE TABLE linkseq_withloads_rider2019 AS(
    SELECT *
    FROM linkseq_withloads_bus_rider2019
    UNION ALL
    SELECT *
    FROM linkseq_withloads_trl_rider2019
    );
COMMIT;

--Assumption: ridership distributed across line routes by number of vehicle journeys
--Assumption: if more than one stop is on a link (sometimes up to 6), the load is averaged - it is usually very similar


--clean up repeats from links that have multiple stops (average loads)
--requires losing detail on gtfsid, but can always get it from the previous table
CREATE TABLE linkseq_cleanloads_rider2019 AS(
--CREATE TABLE linkseq_cleanloads AS(
	WITH tblA AS(
		SELECT lrid, tsys, linename, direction, stopsserved, numvehjour, fromto, lrseq, COUNT(DISTINCT(gtfsid)), sum(load_portion)
		FROM linkseq_withloads_rider2019
        --FROM linkseq_withloads
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

--************************************************--------
--now assign loads to links between stop points in Python
--using script: fill_in_linkloads.py

---AFTER PYTHON
--summarize and join to geometries to view
--line level results
CREATE TABLE loaded_links_linelevel_rider2019 AS(
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
        FROM loaded_links_rider2019
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

CREATE TABLE loaded_links_segmentlevel_rider2019 AS(
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
        FROM loaded_links_rider2019
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

---segment level totals with split from/to to allow for summing directionsal segment level loads
--added 01/06/20 to help Al with Frankford Ave project mapping
--updated 07/07/2020
CREATE TABLE loaded_links_segmentlevel_test_rider2019 AS(
    WITH tblA AS(
        SELECT 
            no,
            CAST(fromnodeno AS text),
            CAST(tonodeno AS text),
            CONCAT(CAST(fromnodeno AS text), CAST(tonodeno AS text)) AS fromto,
            r_no,
            CONCAT(CAST("r_fromno~1" AS text), CAST(r_tonodeno AS text)) AS r_fromto,
            CAST("r_fromno~1" AS text) AS r_from,
            CAST(r_tonodeno AS text) AS r_to,
            geom
        FROM "2015base_link"
        ),
    tblB AS(
        SELECT
            fromto,
            COUNT(fromto) AS times_used,
            SUM(CAST(load_portion_avg AS numeric)) AS total_load
        FROM loaded_links_rider2019
        WHERE tsys = 'Bus'
        OR tsys = 'Trl'
        OR tsys = 'LRT'
        GROUP BY fromto
        ),
    tblC AS(
        SELECT
            b.*,
            a.no,
            a.fromnodeno,
            a.tonodeno,
            --a.r_no,
            --a.r_from,
            --a.r_to,
            a.geom,
            aa.r_no,
            aa.r_from,
            aa.r_to,
            aa.geom AS geom2
        FROM tblB b
        LEFT JOIN tblA a
        ON b.fromto = a.fromto
        LEFT JOIN tblA aa
        ON b.fromto = aa.r_fromto
    )
    SELECT
        fromto,
        CASE WHEN no IS NULL THEN r_no
	    ELSE no
	    END
	    AS linkno,
        CASE WHEN fromnodeno IS NULL THEN r_from
	    ELSE fromnodeno
	    END
	    AS fromnodeno,
        CASE WHEN tonodeno IS NULL THEN r_to
	    ELSE tonodeno
	    END
	    AS tonodeno,	    
        times_used,
        ROUND(total_load,0),
        CASE WHEN geom IS NULL THEN geom2
            ELSE geom
            END
            AS geometry
    FROM tblC);
COMMIT;

