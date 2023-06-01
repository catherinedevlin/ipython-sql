import pytest
from sql.query_util import extract_tables_from_query


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            """
    SELECT t.*
    FROM tracks_with_info t
    JOIN genres_fav
    ON t.GenreId = genres_fav.GenreId
    """,
            ["tracks_with_info", "genres_fav"],
        ),
        (
            """
    SELECT city FROM Customers
    UNION
    SELECT city FROM Suppliers""",
            ["Customers", "Suppliers"],
        ),
        (
            """
                                             SELECT OrderID, Quantity,
CASE
    WHEN Quantity > 30 THEN 'The quantity is greater than 30'
    WHEN Quantity = 30 THEN 'The quantity is 30'
    ELSE 'The quantity is under 30'
END AS QuantityText
FROM OrderDetails;""",
            ["OrderDetails"],
        ),
        (
            """
SELECT COUNT(CustomerID), Country
FROM Customers
GROUP BY Country
HAVING COUNT(CustomerID) > 5;""",
            ["Customers"],
        ),
        (
            """
SELECT LEFT(sub.date, 2) AS cleaned_month,
       sub.day_of_week,
       AVG(sub.incidents) AS average_incidents
  FROM (
        SELECT day_of_week,
               date,
               COUNT(incidnt_num) AS incidents
          FROM tutorial.sf_crime_incidents_2014_01
         GROUP BY 1,2
       ) sub
 GROUP BY 1,2
 ORDER BY 1,2""",
            ["sf_crime_incidents_2014_01"],
        ),
        (
            """
                                             SELECT incidents.*,
       sub.incidents AS incidents_that_day
  FROM tutorial.sf_crime_incidents_2014_01 incidents
  JOIN ( SELECT date,
          COUNT(incidnt_num) AS incidents
           FROM tutorial.sf_crime_incidents_2014_01
          GROUP BY 1
       ) sub
    ON incidents.date = sub.date
 ORDER BY sub.incidents DESC, time
                                             """,
            ["sf_crime_incidents_2014_01", "sf_crime_incidents_2014_01"],
        ),
    ],
    ids=["join", "union", "case", "groupby", "subquery", "subquery_join"],
)
def test_extract(query, expected):
    tables = extract_tables_from_query(query)
    assert expected == tables


def test_invalid_query():
    query = "SELECT city frm Customers"
    tables = extract_tables_from_query(query)
    assert [] == tables
