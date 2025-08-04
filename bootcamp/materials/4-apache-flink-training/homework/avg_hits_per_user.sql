--- This SQL query calculates the average number of hits per user (host) from the processed IP host aggregated data.
SELECT 
    host,
    AVG(num_hits) AS avg_hits
FROM processed_iphost_aggregated
    WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY host
ORDER BY avg_hits DESC

-- Resultset

SELECT * FROM processed_iphost_aggregated