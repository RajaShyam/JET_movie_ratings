--For a given month, what are the 5 movies whose average monthly ratings increased the most compared with the previous month?

WITH avg_ratings_current AS
  (SELECT asin,
          YEAR,
          MONTH,
          title,
          avg(ratings) AS avg_rating
   FROM Jet_analysis.movie_ratings
   WHERE YEAR=2003
     AND MONTH=7
   GROUP BY 1,2,3,4),

avg_ratings_previous AS
  (SELECT asin,
          YEAR,
          MONTH,
          title,
          avg(ratings) AS avg_rating
   FROM Jet_analysis.movie_ratings
   WHERE YEAR=2003
     AND MONTH=6
   GROUP BY 1,2,3,4),

final_ratings AS
  (SELECT ap.asin,
          ap.title,
          ac.avg_rating AS current_avg_rating,
          ap.avg_rating AS previous_avg_rating
   FROM avg_ratings_current ac
   INNER JOIN avg_ratings_previous ap ON ac.asin = ap.asin)

SELECT title,
       previous_avg_rating,
       current_avg_rating
FROM final_ratings
WHERE current_avg_rating > previous_avg_rating
ORDER BY title ASC
LIMIT 5