---
jupytext:
  notebook_metadata_filter: myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.5
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Product Analytics

+++

Product analytics is the process of analyzing users' behaviours when they interact with a product or service. It helps to understand which features users like, what challenges they face when using the product or service, and at what point they turn away. Product teams use these insights to improve the product or service.

In this tutorial, we will demonstrate how to perform product analytics using SQL for an e-commerce website.

+++

## Metrics

+++

Let's look at some common metrics used in product analytics.

`Growth Rate`: User growth rate is the speed at which a business gains new users over a particular period. It is usually measured within a monthly period.

`Retention`: User retention is an important metric that looks at what percentage of first-time users returned in subsequent periods. 

Both these metrics will help to understand how well the users are interacting with the E-commerce platform. 

+++

## Dataset

For this tutorial, we will generate a small dataset `user_activity`. It consists of three columns: `user_id`, `date`, `activity_count`.

- **user_id** : the unique identifier of the user
- **date**: the date on which a user interaction has taken place
- **activity_count**: the number of interactions made by the user on that date. If the user never used this app before this month, this is considered their sign-up month.

First, we'll install the required packages.

```{code-cell} ipython3
:tags: [hide-output]

%pip install jupysql duckdb-engine --quiet
```

Now, load the extension and connect to an in-memory DuckDB database:

```{code-cell} ipython3
%load_ext sql
%sql duckdb://
```

JupySQL allows users to run SQL queries easily using `%sql` and `%%sql` magics. We will use these magics to generate the dataset:

```{code-cell} ipython3

%%sql
CREATE TABLE user_activity (
  user_id INT NOT NULL,
 date DATE NOT NULL,
  activity_count INT NOT NULL,
  PRIMARY KEY (user_id, date)
);
INSERT INTO user_activity (user_id, date, activity_count)
VALUES
  (1, '2021-01-01', 5),
  (1, '2021-02-01', 3),
  (1, '2021-03-01', 2),
  (2, '2021-01-01', 10),
  (3, '2021-02-01', 1),
  (3, '2021-03-01', 0),
  (4, '2021-02-01', 6),
  (5, '2021-01-01', 4),
  (5, '2021-02-01', 5),
  (5, '2021-03-01', 6),
  (6, '2021-03-01', 7),
  (7, '2021-03-01', 10);
```

Let's verify that the table is populated correctly.

```{code-cell} ipython3

%%sql
SELECT * FROM user_activity
```

## Growth 

As defined above, the growth rate is the percentage increase of the total number of users each month. 

We first calculate the total number of users in each month. JupySQL allows users to save query snippets using `--save` argument and use these snippets to compose larger queries.

```{code-cell} ipython3

%%sql --save monthly_user_count
Select MONTH(date) as month, COUNT(DISTINCT user_id) AS total_users
FROM user_activity
GROUP BY MONTH(date)
```

Here, we will group the dataset by the month of the date, and then count the number of distinct users as the total number of users.
The command uses `--with` argument to refer to the snippet saved in the previous query.
Also, note that '/' in SQL between two integers performs integer division. For example, 10/3 would be 3 instead of 3.33333. So the result needs to be multiplied by 1.0 to convert it to float. 


```{code-cell} ipython3

%%sql --with monthly_user_count
SELECT c1.month as PrevMonth, c2.month as CurrentMonth,ROUND((c2.total_users - c1.total_users)*1.0/c1.total_users*100, 2) AS Growth_Rate_in_Percentage
FROM monthly_user_count c1, monthly_user_count c2
WHERE c1.month = c2.month - 1
```


The user growth rate between January and February is 33.33% while that of the February-March period is 25%.

+++

The use of self join in the query might be confusing. Here is a brief explanation of what the self join is doing. After we run the command
`FROM monthly_user_count c1, monthly_user_count c2`
The table we get is a cartesian product of these three rows: 

```{code-cell} ipython3

%%sql --with monthly_user_count
SELECT c1.month AS 'c1.month', c1.total_users AS 'c1.total_users', c2.month AS 'c2.month', c2.total_users AS 'c2.total_users'
FROM monthly_user_count c1, monthly_user_count c2
```

Then, with **WHERE c1.month = c2.month - 1**, we filter out the total number of users for subsequential months.

```{code-cell} ipython3

%%sql --with monthly_user_count
SELECT c1.month AS 'c1.month', c1.total_users AS 'c1.total_users', c2.month AS 'c2.month', c2.total_users AS 'c2.total_users'
FROM monthly_user_count c1, monthly_user_count c2
WHERE c1.month = c2.month - 1
```

As shown above, we calculate the final growth rate using c1.total_users and c2.total_users.

+++

## Retention

+++

The period over which user retention is calculated can vary across companies, Here, we define retention as the percentage of users who still use the app one month after their first login.

+++

We will first create two query snippets : `first_time_users` and `retention_users`.

```{code-cell} ipython3

%%sql --save first_time_users

SELECT MONTH(date) AS month, COUNT(DISTINCT u.user_id) AS first_time_users
FROM user_activity u
INNER JOIN (
  SELECT user_id, MIN(date) AS first_login
  FROM user_activity
  GROUP BY user_id
) t ON u.user_id = t.user_id AND u.date = t.first_login
GROUP BY MONTH(date)
```

From the results, we can see that in January, 3 users started to use the app. Similarly, 2 users started using the app in the month of February,  and 2 users start using in March.

+++

Then, for each month, we calculate the number of users who still use the app after one month of first-login

```{code-cell} ipython3

%%sql --save retention_users
SELECT MONTH(first_login) AS month, COUNT(DISTINCT u. user_id) AS retention_users
FROM user_activity u
INNER JOIN (
SELECT user_id, MIN(date) AS first_login
FROM user_activity
GROUP BY user_id) t 
ON u.user_id = t.user_id
WHERE MONTH(date) = MONTH(first_login) +1
GROUP BY MONTH(first_login)
```

Here, the condition `WHERE MONTH(date) = MONTH(first_login) + 1` ensured that we only consider users who still using the app for at least one month since signing up on the platform. 
As we can see, 2 out of 3 users continue to use the app beyond a month.

+++

Now, we will join the `first_time_users` and `retention_users` tables and calculate the retention rate.

```{code-cell} ipython3

%%sql --with first_time_users --with retention_users
SELECT f.month, first_time_users, IFNULL(retention_users, 0) AS retention_users, ROUND(retention_users * 1.0 / first_time_users, 4)*100 AS retention_rate
FROM first_time_users f 
FULL OUTER JOIN retention_users r
ON f.month = r.month
```

## Summary

In this tutorial, we learnt how to use cell magics in JupySQL and easily run SQL queries. We also learnt how we can formulate complex queries using `--save` and `--with` arguments. These tools come in handy when performing complex data analytics tasks like product analytics.