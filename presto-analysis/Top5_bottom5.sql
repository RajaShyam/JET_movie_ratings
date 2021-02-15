-- What are the top 5 and bottom 5 movies in terms of overall average review ratings for a given month?


WITH avg_ratings AS
  (SELECT asin,
          YEAR,
          MONTH,
          title,
          avg(ratings) AS avg_rating
   FROM Jet_analysis.movie_ratings
   WHERE YEAR=2003
     AND MONTH=5
   GROUP BY 1,2,3,4)

SELECT title, avg_rating from (
SELECT *,
       dense_rank() OVER (ORDER BY avg_rating DESC, title ASC) AS top_five,
       dense_rank() OVER (ORDER BY avg_rating ASC, title ASC) AS bottom_five
FROM avg_ratings )
where top_five <=5 or bottom_five <=5
