SELECT a.UserID, a.MovieID, a.Rating, a.Timestamp, b.Min, b.Max, b.Avg
FROM df_ratings AS a
LEFT JOIN df_ratings_aggr AS b
ON a.MovieID = b.MovieID
ORDER BY a.UserID, a.MovieID