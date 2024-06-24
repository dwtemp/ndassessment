SELECT MovieID, MIN(Rating) AS Min, MAX(Rating) AS Max, ROUND(AVG(Rating), 3) AS Avg
FROM df_ratings
GROUP BY MovieID
ORDER BY MovieID