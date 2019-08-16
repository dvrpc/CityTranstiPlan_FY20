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