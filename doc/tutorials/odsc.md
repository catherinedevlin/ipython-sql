# SQL Driven Machine Learning Tutorial: Customer Churn Prediction
In this tutorial, we will walk through an example of using SQL-driven machine learning to predict customer churn. We will be using the JupySQL package to run SQL queries within a Jupyter Notebook and a public dataset available on Kaggle.

Prerequisites
Install Jupyter Notebook
Install the JupySQL package using pip:
diff

```sql
!pip install jupysql duckdb duckdb-engine seaborn --upgrade -q
```

We'll download the Telco Customer Churn dataset from Kaggle (I found a Github link) and save it as `churn.csv`.

## Initial setup
First, let's load the JupySQL extension and connect to an SQLite database in memory.

```sql
%load_ext sql
%sql duckdb://
```

### Import the dataset
Now we will import the dataset into a new table called customer_churn.

```sql
from urllib.request import urlretrieve
_ = urlretrieve(
    "https://raw.githubusercontent.com/treselle-systems/customer_churn_analysis/master/WA_Fn-UseC_-Telco-Customer-Churn.csv",
    "churn.csv",
)
```

We can try querying the data and see it works, which means the engine is connected, my data is there locally.

```sql
SELECT * FROM churn.csv LIMIT 3;
```

Before diving into the machine learning process, let's explore the dataset to get a better understanding of the features and target variable.


## Exploratory Data Analysis (EDA)
Before diving into data preprocessing and model training, let's perform some exploratory data analysis (EDA) to gain insights into the dataset and identify potential relationships between features and the target variable (customer churn).

Summary statistics
Let's start by calculating summary statistics for the numerical columns in the dataset.

```sql
SELECT
    AVG(tenure) AS avg_tenure,
    AVG(MonthlyCharges) AS avg_MonthlyCharges,
    AVG(TotalCharges) AS avg_TotalCharges
FROM
    churn.csv;
```

### First problem!
It appears that the `TotalCharges` column is being treated as a `VARCHAR` instead of a `numeric data type`. To address this issue, let's cast the `TotalCharges` column to a numeric data type before querying and calculating the average.

First, let's find out if there are any non-numeric values in the TotalCharges column:

```sql
SELECT
    TotalCharges
FROM
    churn.csv
WHERE
    NOT regexp_matches(TotalCharges, '^([0-9]+(\.[0-9]+)?)$')
LIMIT 10;
```

If there are any non-numeric values, you can either remove those rows or replace them with appropriate values. For this example, let's replace them with `NULL`. In our case we'll just cast it directly and continue with our analysis.


You can cast directly the `TotalCharges` column to a numeric data type and calculate the averages. DuckDB has a built in feature for this called `TRY_CAST()`. The query from our previous cell (above, `cell 5`) should now return the average values for `tenure`, `MonthlyCharges`, and `TotalCharges.






```sql
# %sql select 
# customerID, gender, SeniorCitizen, Partner, Dependents, tenure, 
# PhoneService, MultipleLines, InternetService, OnlineSecurity, OnlineBackup, DeviceProtection, 
# TechSupport, StreamingTV, StreamingMovies, Contract, PaperlessBilling, PaymentMethod, MonthlyCharges, 
# TotalCharges, Churn 
# FROM churn.csv limit 3

```

```sql
CREATE TABLE cleaned_churn AS
SELECT
    AVG(tenure) AS avg_tenure,
    AVG(MonthlyCharges) AS avg_MonthlyCharges,
    AVG(TRY_CAST(TotalCharges AS FLOAT)) AS avg_TotalCharges
FROM
    churn.csv;
```

```sql
CREATE TABLE cleaned_churn AS
SELECT
    AVG(tenure) AS avg_tenure,
    AVG(MonthlyCharges) AS avg_MonthlyCharges,
    AVG(TRY_CAST(TotalCharges AS FLOAT)) AS avg_TotalCharges
FROM
    churn.csv;
```

```sql
%sql select * from cleaned_churn limit 3
```

### Churn distribution
Now let's check the distribution of churn in the dataset.

```sql
SELECT
    Churn,
    COUNT(*) AS count,
    COUNT(*) * 1.0 / (SELECT COUNT(*) FROM churn.csv) AS percentage
FROM
    churn.csv
GROUP BY
    Churn;
```

### Churn by contract type
Let's explore the relationship between contract type and churn.

```sql magic_args="--save churn_by"
SELECT
    Contract,
    Churn,
    COUNT(*) AS count,
    COUNT(*) * 1.0 / (SELECT COUNT(*) FROM churn.csv WHERE Contract = c.Contract) AS percentage
FROM
    churn.csv AS c
GROUP BY
    Contract,
    Churn;
```

### Churn by payment method
We can also examine the relationship between payment methods and customer churn.

```sql
SELECT
    PaymentMethod,
    Churn,
    COUNT(*) AS count,
    COUNT(*) * 1.0 / (SELECT COUNT(*) FROM churn.csv WHERE PaymentMethod = p.PaymentMethod) AS percentage
FROM
    churn.csv AS p
GROUP BY
    PaymentMethod,
    Churn
limit 3;
```

### Correlation matrix
Finally, let's create a correlation matrix to explore relationships between the numerical features and customer churn.

```sql magic_args="df <<"
SELECT tenure, MonthlyCharges, TRY_CAST(TotalCharges AS FLOAT) as TotalCharges, Churn FROM churn.csv;
```

```sql
import seaborn as sns
import matplotlib.pyplot as plt
data_corr = df.DataFrame()
data_corr['Churn'] = data_corr['Churn'].apply(lambda x: 1 if x == 'Yes' else 0)
correlation_matrix = data_corr.corr()

plt.figure(figsize=(8, 6))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm')
plt.title('Correlation Matrix')
plt.show()
```

These EDA techniques provide valuable insights into the dataset and can help guide the feature engineering and model selection process. Based on the EDA results, you might consider focusing on specific features, transforming variables, or trying different machine learning algorithms to improve model performance.


Another cool thing we can do to simply get the imidiate insights pop out to us is use jupysql's built in functionality to view our data distribution:

```sql
%sqlcmd profile --table "churn.csv"
```

We can also save it as an HTML report if we'd like to!


## Data preprocessing
To prepare the data for machine learning, we'll need to preprocess it. We will start by converting the categorical columns into numerical values using one-hot encoding. We'll also normalize the continuous features to bring them to the same scale.

```sql
CREATE TABLE customer_churn_encoded AS
SELECT
    customerID,
    gender,
    SeniorCitizen,
    Partner,
    Dependents,
    tenure,
    PhoneService,
    MultipleLines,
    CASE
        WHEN InternetService = 'DSL' THEN 1
        ELSE 0
    END AS InternetService_DSL,
    CASE
        WHEN InternetService = 'Fiber optic' THEN 1
        ELSE 0
    END AS InternetService_Fiber_optic,
    CASE
        WHEN InternetService = 'No' THEN 1
        ELSE 0
    END AS InternetService_No,
    OnlineSecurity,
    OnlineBackup,
    DeviceProtection,
    TechSupport,
    StreamingTV,
    StreamingMovies,
    Contract,
    PaperlessBilling,
    PaymentMethod,
    MonthlyCharges,
    TotalCharges,
    Churn
FROM
    churn.csv;
```
Now, we will normalize the continuous features (tenure, MonthlyCharges, and TotalCharges).

```sql
SELECT
    *,
    (tenure - (SELECT MIN(tenure) FROM customer_churn_encoded)) 
        (SELECT MAX(tenure) - MIN(tenure) FROM customer_churn_encoded) AS tenure_normalized,
    (MonthlyCharges - (SELECT MIN(MonthlyCharges) FROM customer_churn_encoded)) 
        (SELECT MAX(MonthlyCharges) - MIN(MonthlyCharges) FROM customer_churn_encoded) AS MonthlyCharges_normalized,
    (TotalCharges - (SELECT MIN(TotalCharges)) FROM customer_churn_encoded)) 
(SELECT MAX(TotalCharges) - MIN(TotalCharges) FROM customer_churn_encoded) AS TotalCharges_normalized
FROM customer_churn_encoded;
```

```sql
CREATE TABLE customer_churn_normalized AS
SELECT
    *,
    (tenure - (SELECT MIN(tenure) FROM customer_churn_encoded)) 
        (SELECT MAX(tenure) - MIN(tenure) FROM customer_churn_encoded) AS tenure_normalized,
    (MonthlyCharges - (SELECT MIN(MonthlyCharges) FROM customer_churn_encoded)) 
        (SELECT MAX(MonthlyCharges) - MIN(MonthlyCharges) FROM customer_churn_encoded) AS MonthlyCharges_normalized,
    (TotalCharges - (SELECT MIN(TotalCharges)) FROM customer_churn_encoded)) 
(SELECT MAX(TotalCharges) - MIN(TotalCharges) FROM customer_churn_encoded) AS TotalCharges_normalized
FROM
customer_churn_encoded;
```


To train and test our model, we will split the data into training (80%) and testing (20%) sets.

```sql
### Splitting the data into training and testing sets
%%sql
CREATE TABLE customer_churn_train AS
SELECT * FROM customer_churn_normalized
WHERE random() < 0.8;

CREATE TABLE customer_churn_test AS
SELECT * FROM customer_churn_normalized
EXCEPT
SELECT * FROM customer_churn_train;
```


Training the model
We will now train a logistic regression model using the training data. We will use SQLite's logistic regression functions.

```sql
CREATE TABLE customer_churn_model AS
SELECT
    LOGISTIC_REGRESSION('churn ~
        gender +
        SeniorCitizen +
        Partner +
        Dependents +
        tenure_normalized +
        PhoneService +
        MultipleLines +
        InternetService_DSL +
        InternetService_Fiber_optic +
        InternetService_No +
        OnlineSecurity +
        OnlineBackup +
        DeviceProtection +
        TechSupport +
        StreamingTV +
        StreamingMovies +
        Contract +
        PaperlessBilling +
        PaymentMethod +
        MonthlyCharges_normalized +
        TotalCharges_normalized',
        1,
        0.001,
        1000,
        1e-4
    ) AS model
FROM
    customer_churn_train;
```
Testing the model
With the model trained, we can now test it on our test dataset and calculate the accuracy.


```sql
CREATE TABLE customer_churn_test_predictions AS
SELECT
    *,
    LOGISTIC_REGRESSION_PREDICT(model,
        gender,
        SeniorCitizen,
        Partner,
        Dependents,
        tenure_normalized,
        PhoneService,
        MultipleLines,
        InternetService_DSL,
        InternetService_Fiber_optic,
        InternetService_No,
        OnlineSecurity,
        OnlineBackup,
        DeviceProtection,
        TechSupport,
        StreamingTV,
        StreamingMovies,
        Contract,
        PaperlessBilling,
        PaymentMethod,
        MonthlyCharges_normalized,
        TotalCharges_normalized
    ) AS predicted_churn
FROM
    customer_churn_test,
    customer_churn_model;
```

Now let's find out how accurate our model is by calculating as follows:

```sql
SELECT
    COUNT(*) * 1.0 / (SELECT COUNT(*) FROM customer_churn_test) AS accuracy
FROM
    customer_churn_test_predictions
WHERE
    predicted_churn = Churn;
```

This will return the accuracy of our logistic regression model in predicting customer churn. You can experiment with different machine learning algorithms and feature selection techniques to improve the model's performance.


## Model Interpretation
Now that we have our model, let's interpret the feature coefficients to understand which factors are most important in predicting customer churn.


```sql
SELECT
    LOGISTIC_REGRESSION_COEF(model) AS coefficients
FROM
    customer_churn_model;
```

You can examine the coefficients to understand the relative importance of each feature in the model. A positive coefficient indicates that the feature increases the likelihood of churn, while a negative coefficient indicates that the feature decreases the likelihood of churn.

## Cross-validation
To get a better understanding of our model's performance, we can use cross-validation. Let's perform 5-fold cross-validation on our dataset.

```sql
CREATE TABLE customer_churn_cv_results AS
SELECT
    LOGISTIC_REGRESSION_CV('churn ~
        gender +
        SeniorCitizen +
        Partner +
        Dependents +
        tenure_normalized +
        PhoneService +
        MultipleLines +
        InternetService_DSL +
        InternetService_Fiber_optic +
        InternetService_No +
        OnlineSecurity +
        OnlineBackup +
        DeviceProtection +
        TechSupport +
        StreamingTV +
        StreamingMovies +
        Contract +
        PaperlessBilling +
        PaymentMethod +
        MonthlyCharges_normalized +
        TotalCharges_normalized',
        1,
        0.001,
        1000,
        1e-4,
        5
    ) AS cv_results
FROM
    customer_churn_normalized;
```

Now let's calculate the average accuracy across all folds.

```sql
SELECT
    AVG(accuracy) AS avg_accuracy
FROM
    customer_churn_cv_results;
```

This will give you a better idea of the model's generalization performance on unseen data.

## Conclusion
In this tutorial, we walked through a SQL-driven machine learning example using the JupySQL package to predict customer churn. We explored the dataset, preprocessed the data, split it into training and testing sets, trained a logistic regression model, tested it, and performed cross-validation to measure the model's performance.

You can continue to experiment with different machine learning algorithms and feature selection techniques to improve your model's performance. Additionally, you can integrate this SQL-driven approach into your data pipeline to automate and scale your machine learning projects.
