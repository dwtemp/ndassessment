SELECT a.UserID, b.Title, a.Preference FROM
    (SELECT UserID, MovieID, Preference FROM
        (SELECT UserID, MovieID, row_number() over (partition by UserID order by UserID ASC, Rating DESC, Timestamp DESC) AS Preference
        FROM df_ratings)
    WHERE Preference <= 3
    ORDER BY UserID, Preference) AS a
    LEFT JOIN df_movies AS b
    ON a.MovieID = b.MovieID