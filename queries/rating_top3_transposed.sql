SELECT c.UserID, c.Pref1, c.Pref2, d.Title AS Pref3
    FROM
        (SELECT a.UserID, a.Pref1, b.Title AS Pref2
        FROM
            (SELECT UserID, Title AS Pref1
            FROM df_ratings_top3
            WHERE Preference = 1) AS a
        JOIN
            (SELECT UserID, Title
            FROM df_ratings_top3
            WHERE Preference = 2) AS b
        ON a.UserID = b.UserID) AS c
    JOIN
        (SELECT UserID, Title
        FROM df_ratings_top3
        WHERE Preference = 3) AS d
    ON c.UserID = d.UserID
    ORDER BY c.UserID ASC