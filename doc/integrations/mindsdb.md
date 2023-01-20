# Mindsdb tutorial
In this guide we'll show an integration with MindsDB.

We will use Jupysql to run queries on top of MindsDB.
Train the model on top of it via SQL.
Once the model is ready, we will use sklearn-evaluation to generate plots and evaluate our model.

MindsDB is a powerful machine learning platform that enables users to easily build and deploy predictive models. One of the key features of MindsDB is its integration with Jupysql, which allows users to connect to and query databases from Jupyter notebooks. In this article, we will take a deeper dive into the technical details of this integration, and provide examples of how it can be used in practice. We will explore a customer churn dataset and generate predictions if our customer will churn or not. Once we're done with that we will evaluate our model and see how easy it is through a single line of code.

The integration between Jupysql and MindsDB is made possible by the use of the sqlite3 library. This library allows for easy communication between the two systems, and enables users to connect to a wide variety of databases and warehouses, including Redshift, Snowflake, Big query, DuckDB, SQLite, MySQL, and PostgreSQL. Once connected, users can run SQL queries directly from the MindsDB environment, making it easy to extract data from databases and use it to train predictive models.

Let's look at an example of how this integration can be used. Suppose we have a database containing customer churn data, and we want to use this data to train a model that predicts if a customer will churn or not. First, we would need to connect to the database from our Jupyter notebook using the jupysql library. This can be done using the following code:



```python
# Install required packages
%pip install PyMySQL jupysql sklearn-evaluation --upgrade
```

    Requirement already satisfied: PyMySQL in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (1.0.2)
    Requirement already satisfied: jupysql in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (0.5.2)
    Requirement already satisfied: sklearn-evaluation in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (0.9.0)
    Requirement already satisfied: sqlparse in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from jupysql) (0.4.3)
    Requirement already satisfied: prettytable<1 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from jupysql) (0.7.2)
    Requirement already satisfied: ploomber-core>=0.1.* in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from jupysql) (0.2.0)
    Requirement already satisfied: ipython-genutils>=0.1.0 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from jupysql) (0.2.0)
    Requirement already satisfied: ipython>=1.0 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from jupysql) (8.8.0)
    Requirement already satisfied: jinja2 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from jupysql) (3.1.2)
    Requirement already satisfied: sqlalchemy<2.0,>=0.6.7 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from jupysql) (1.4.46)
    Requirement already satisfied: black in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from sklearn-evaluation) (22.12.0)
    Requirement already satisfied: parso in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from sklearn-evaluation) (0.8.3)
    Requirement already satisfied: mistune in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from sklearn-evaluation) (2.0.4)
    Requirement already satisfied: decorator in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from sklearn-evaluation) (5.1.1)
    Requirement already satisfied: scikit-learn in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from sklearn-evaluation) (1.2.0)
    Requirement already satisfied: pandas in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from sklearn-evaluation) (1.5.2)
    Requirement already satisfied: tabulate in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from sklearn-evaluation) (0.9.0)
    Requirement already satisfied: matplotlib in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from sklearn-evaluation) (3.6.3)
    Requirement already satisfied: nbformat in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from sklearn-evaluation) (5.7.3)
    Requirement already satisfied: traitlets>=5 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from ipython>=1.0->jupysql) (5.8.1)
    Requirement already satisfied: backcall in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from ipython>=1.0->jupysql) (0.2.0)
    Requirement already satisfied: matplotlib-inline in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from ipython>=1.0->jupysql) (0.1.6)
    Requirement already satisfied: appnope in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from ipython>=1.0->jupysql) (0.1.3)
    Requirement already satisfied: prompt-toolkit<3.1.0,>=3.0.11 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from ipython>=1.0->jupysql) (3.0.36)
    Requirement already satisfied: pickleshare in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from ipython>=1.0->jupysql) (0.7.5)
    Requirement already satisfied: pygments>=2.4.0 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from ipython>=1.0->jupysql) (2.14.0)
    Requirement already satisfied: jedi>=0.16 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from ipython>=1.0->jupysql) (0.18.2)
    Requirement already satisfied: stack-data in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from ipython>=1.0->jupysql) (0.6.2)
    Requirement already satisfied: pexpect>4.3 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from ipython>=1.0->jupysql) (4.8.0)
    Requirement already satisfied: click in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from ploomber-core>=0.1.*->jupysql) (8.1.3)
    Requirement already satisfied: pyyaml in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from ploomber-core>=0.1.*->jupysql) (6.0)
    Requirement already satisfied: posthog in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from ploomber-core>=0.1.*->jupysql) (2.2.0)
    Requirement already satisfied: platformdirs>=2 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from black->sklearn-evaluation) (2.6.2)
    Requirement already satisfied: mypy-extensions>=0.4.3 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from black->sklearn-evaluation) (0.4.3)
    Requirement already satisfied: pathspec>=0.9.0 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from black->sklearn-evaluation) (0.10.3)
    Requirement already satisfied: tomli>=1.1.0 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from black->sklearn-evaluation) (2.0.1)
    Requirement already satisfied: MarkupSafe>=2.0 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from jinja2->jupysql) (2.1.1)
    Requirement already satisfied: numpy>=1.19 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from matplotlib->sklearn-evaluation) (1.24.1)
    Requirement already satisfied: python-dateutil>=2.7 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from matplotlib->sklearn-evaluation) (2.8.2)
    Requirement already satisfied: contourpy>=1.0.1 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from matplotlib->sklearn-evaluation) (1.0.7)
    Requirement already satisfied: kiwisolver>=1.0.1 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from matplotlib->sklearn-evaluation) (1.4.4)
    Requirement already satisfied: cycler>=0.10 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from matplotlib->sklearn-evaluation) (0.11.0)
    Requirement already satisfied: pillow>=6.2.0 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from matplotlib->sklearn-evaluation) (9.4.0)
    Requirement already satisfied: packaging>=20.0 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from matplotlib->sklearn-evaluation) (23.0)
    Requirement already satisfied: fonttools>=4.22.0 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from matplotlib->sklearn-evaluation) (4.38.0)
    Requirement already satisfied: pyparsing>=2.2.1 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from matplotlib->sklearn-evaluation) (3.0.9)
    Requirement already satisfied: fastjsonschema in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from nbformat->sklearn-evaluation) (2.16.2)
    Requirement already satisfied: jsonschema>=2.6 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from nbformat->sklearn-evaluation) (4.17.3)
    Requirement already satisfied: jupyter-core in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from nbformat->sklearn-evaluation) (5.1.3)
    Requirement already satisfied: pytz>=2020.1 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from pandas->sklearn-evaluation) (2022.7)
    Requirement already satisfied: scipy>=1.3.2 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from scikit-learn->sklearn-evaluation) (1.10.0)
    Requirement already satisfied: joblib>=1.1.1 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from scikit-learn->sklearn-evaluation) (1.2.0)
    Requirement already satisfied: threadpoolctl>=2.0.0 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from scikit-learn->sklearn-evaluation) (3.1.0)
    Requirement already satisfied: attrs>=17.4.0 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from jsonschema>=2.6->nbformat->sklearn-evaluation) (22.2.0)
    Requirement already satisfied: pyrsistent!=0.17.0,!=0.17.1,!=0.17.2,>=0.14.0 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from jsonschema>=2.6->nbformat->sklearn-evaluation) (0.19.3)
    Requirement already satisfied: ptyprocess>=0.5 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from pexpect>4.3->ipython>=1.0->jupysql) (0.7.0)
    Requirement already satisfied: wcwidth in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from prompt-toolkit<3.1.0,>=3.0.11->ipython>=1.0->jupysql) (0.2.5)
    Requirement already satisfied: six>=1.5 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from python-dateutil>=2.7->matplotlib->sklearn-evaluation) (1.16.0)
    Requirement already satisfied: requests<3.0,>=2.7 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from posthog->ploomber-core>=0.1.*->jupysql) (2.28.2)
    Requirement already satisfied: backoff<2.0.0,>=1.10.0 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from posthog->ploomber-core>=0.1.*->jupysql) (1.11.1)
    Requirement already satisfied: monotonic>=1.5 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from posthog->ploomber-core>=0.1.*->jupysql) (1.6)
    Requirement already satisfied: asttokens>=2.1.0 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from stack-data->ipython>=1.0->jupysql) (2.2.1)
    Requirement already satisfied: pure-eval in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from stack-data->ipython>=1.0->jupysql) (0.2.2)
    Requirement already satisfied: executing>=1.2.0 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from stack-data->ipython>=1.0->jupysql) (1.2.0)
    Requirement already satisfied: certifi>=2017.4.17 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from requests<3.0,>=2.7->posthog->ploomber-core>=0.1.*->jupysql) (2022.12.7)
    Requirement already satisfied: idna<4,>=2.5 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from requests<3.0,>=2.7->posthog->ploomber-core>=0.1.*->jupysql) (3.4)
    Requirement already satisfied: urllib3<1.27,>=1.21.1 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from requests<3.0,>=2.7->posthog->ploomber-core>=0.1.*->jupysql) (1.26.14)
    Requirement already satisfied: charset-normalizer<4,>=2 in /Users/idomi/opt/miniconda3/envs/py310/lib/python3.10/site-packages (from requests<3.0,>=2.7->posthog->ploomber-core>=0.1.*->jupysql) (3.0.1)
    Note: you may need to restart the kernel to use updated packages.



```python
import pandas as pd
from sklearn_evaluation import plot

# Import jupysql Jupyter extension to create SQL cells
%load_ext sql
%config SqlMagic.autocommit=False
```

**You'd need to make sure your MindsDB is up and reachable for the next stages. You can use either the local or the cloud version.**

**Note:** you will need to adjust the connection string according to the instance you're trying to connect to (url, user, password).
In addition you'd need to load [the dataset file](https://github.com/mindsdb/mindsdb-examples/blob/master/classics/customer_churn/raw_data/WA_Fn-UseC_-Telco-Customer-Churn.csv) into the DB, follow this guide on [how to do it](https://docs.mindsdb.com/sql/create/file).


```python
%sql mysql+pymysql://YOUR_EMAIL:YOUR_PASSWORD@cloud.mindsdb.com:3306
```


```python
%sql SHOW TABLES FROM files;
```

    *  mysql+pymysql://ido%40ploomber.io:***@cloud.mindsdb.com:3306
    2 rows affected.





<table>
    <tr>
        <th>Tables_in_files</th>
    </tr>
    <tr>
        <td>churn</td>
    </tr>
    <tr>
        <td>home_rentals</td>
    </tr>
</table>




```sql
%%sql 
SELECT *
FROM files.churn
LIMIT 5;
```

    *  mysql+pymysql://ido%40ploomber.io:***@cloud.mindsdb.com:3306
    5 rows affected.





<table>
    <tr>
        <th>customerID</th>
        <th>gender</th>
        <th>SeniorCitizen</th>
        <th>Partner</th>
        <th>Dependents</th>
        <th>tenure</th>
        <th>PhoneService</th>
        <th>MultipleLines</th>
        <th>InternetService</th>
        <th>OnlineSecurity</th>
        <th>OnlineBackup</th>
        <th>DeviceProtection</th>
        <th>TechSupport</th>
        <th>StreamingTV</th>
        <th>StreamingMovies</th>
        <th>Contract</th>
        <th>PaperlessBilling</th>
        <th>PaymentMethod</th>
        <th>MonthlyCharges</th>
        <th>TotalCharges</th>
        <th>Churn</th>
    </tr>
    <tr>
        <td>7590-VHVEG</td>
        <td>Female</td>
        <td>0</td>
        <td>Yes</td>
        <td>No</td>
        <td>1</td>
        <td>No</td>
        <td>No phone service</td>
        <td>DSL</td>
        <td>No</td>
        <td>Yes</td>
        <td>No</td>
        <td>No</td>
        <td>No</td>
        <td>No</td>
        <td>Month-to-month</td>
        <td>Yes</td>
        <td>Electronic check</td>
        <td>29.85</td>
        <td>29.85</td>
        <td>No</td>
    </tr>
    <tr>
        <td>5575-GNVDE</td>
        <td>Male</td>
        <td>0</td>
        <td>No</td>
        <td>No</td>
        <td>34</td>
        <td>Yes</td>
        <td>No</td>
        <td>DSL</td>
        <td>Yes</td>
        <td>No</td>
        <td>Yes</td>
        <td>No</td>
        <td>No</td>
        <td>No</td>
        <td>One year</td>
        <td>No</td>
        <td>Mailed check</td>
        <td>56.95</td>
        <td>1889.5</td>
        <td>No</td>
    </tr>
    <tr>
        <td>3668-QPYBK</td>
        <td>Male</td>
        <td>0</td>
        <td>No</td>
        <td>No</td>
        <td>2</td>
        <td>Yes</td>
        <td>No</td>
        <td>DSL</td>
        <td>Yes</td>
        <td>Yes</td>
        <td>No</td>
        <td>No</td>
        <td>No</td>
        <td>No</td>
        <td>Month-to-month</td>
        <td>Yes</td>
        <td>Mailed check</td>
        <td>53.85</td>
        <td>108.15</td>
        <td>Yes</td>
    </tr>
    <tr>
        <td>7795-CFOCW</td>
        <td>Male</td>
        <td>0</td>
        <td>No</td>
        <td>No</td>
        <td>45</td>
        <td>No</td>
        <td>No phone service</td>
        <td>DSL</td>
        <td>Yes</td>
        <td>No</td>
        <td>Yes</td>
        <td>Yes</td>
        <td>No</td>
        <td>No</td>
        <td>One year</td>
        <td>No</td>
        <td>Bank transfer (automatic)</td>
        <td>42.3</td>
        <td>1840.75</td>
        <td>No</td>
    </tr>
    <tr>
        <td>9237-HQITU</td>
        <td>Female</td>
        <td>0</td>
        <td>No</td>
        <td>No</td>
        <td>2</td>
        <td>Yes</td>
        <td>No</td>
        <td>Fiber optic</td>
        <td>No</td>
        <td>No</td>
        <td>No</td>
        <td>No</td>
        <td>No</td>
        <td>No</td>
        <td>Month-to-month</td>
        <td>Yes</td>
        <td>Electronic check</td>
        <td>70.7</td>
        <td>151.65</td>
        <td>Yes</td>
    </tr>
</table>




```sql
%%sql
CREATE MODEL mindsdb.customer_churn_predictor
FROM files
  (SELECT * FROM churn)
PREDICT Churn;
```

    *  mysql+pymysql://ido%40ploomber.io:***@cloud.mindsdb.com:3306
    (pymysql.err.ProgrammingError) (1149, " model 'customer_churn_predictor' already exists in project mindsdb!")
    [SQL: CREATE MODEL mindsdb.customer_churn_predictor
    FROM files
      (SELECT * FROM churn)
    PREDICT Churn;]
    (Background on this error at: https://sqlalche.me/e/14/f405)


## Training the model

Training the model have 3 different statuses: Generating, Training, Complete.
Since it's a pretty small dataset it'd take a few minutes to get to the complete status.

Once the status is "complete", move on to the next section.

**Waiting for the below cell to show complete**


```sql
%%sql
SELECT status
FROM mindsdb.models
WHERE name='customer_churn_predictor';
```

    *  mysql+pymysql://ido%40ploomber.io:***@cloud.mindsdb.com:3306
    1 rows affected.





<table>
    <tr>
        <th>status</th>
    </tr>
    <tr>
        <td>complete</td>
    </tr>
</table>



Now that our model is reeady to generate predictions, we can start using it.
In the cell below we'll start by getting a single prediction.

We are classifying if a user will churn, it's confidence and the explanation based on a few input parameters such as their internet service, if they have phone service, dependents and more.


```sql
%%sql
SELECT Churn, Churn_confidence, Churn_explain
FROM mindsdb.customer_churn_predictor
WHERE SeniorCitizen=0
AND Partner='Yes'
AND Dependents='No'
AND tenure=1
AND PhoneService='No'
AND MultipleLines='No phone service'
AND InternetService='DSL';
```

    *  mysql+pymysql://ido%40ploomber.io:***@cloud.mindsdb.com:3306
    1 rows affected.





<table>
    <tr>
        <th>Churn</th>
        <th>Churn_confidence</th>
        <th>Churn_explain</th>
    </tr>
    <tr>
        <td>Yes</td>
        <td>0.7752808988764045</td>
        <td>{&quot;predicted_value&quot;: &quot;Yes&quot;, &quot;confidence&quot;: 0.7752808988764045, &quot;anomaly&quot;: null, &quot;truth&quot;: null, &quot;probability_class_No&quot;: 0.4756, &quot;probability_class_Yes&quot;: 0.5244}</td>
    </tr>
</table>



We can get a batch of multiple entries.

In the cell bellow we're getting 5 rows (customers) with different parameters such as monthly charges and contract type.


```sql
%%sql
SELECT t.customerID, t.Contract, t.MonthlyCharges, m.Churn
FROM files.churn AS t
JOIN mindsdb.customer_churn_predictor AS m
LIMIT 5;
```

    *  mysql+pymysql://ido%40ploomber.io:***@cloud.mindsdb.com:3306
    5 rows affected.





<table>
    <tr>
        <th>customerID</th>
        <th>Contract</th>
        <th>MonthlyCharges</th>
        <th>Churn</th>
    </tr>
    <tr>
        <td>7590-VHVEG</td>
        <td>Month-to-month</td>
        <td>29.85</td>
        <td>Yes</td>
    </tr>
    <tr>
        <td>5575-GNVDE</td>
        <td>One year</td>
        <td>56.95</td>
        <td>No</td>
    </tr>
    <tr>
        <td>3668-QPYBK</td>
        <td>Month-to-month</td>
        <td>53.85</td>
        <td>Yes</td>
    </tr>
    <tr>
        <td>7795-CFOCW</td>
        <td>One year</td>
        <td>42.3</td>
        <td>No</td>
    </tr>
    <tr>
        <td>9237-HQITU</td>
        <td>Month-to-month</td>
        <td>70.7</td>
        <td>Yes</td>
    </tr>
</table>



## Classifier evaluation

Now that our model is ready, we want and should evaluate it.
We will query the actual and predicted values from MindsDB to evaluate our model.

Once we have the values we can plot them using sklearn-evaluation.
We will start first by getting all of our customers into a `pandas dataframe`.

**Note:** Take a close look on the query below, by saving it into a variable we can compose complex and longer queries.


```sql
%%sql result << SELECT t.customerID, t.Contract, t.MonthlyCharges, m.Churn, 
t.Churn as actual
FROM files.churn AS t
JOIN mindsdb.customer_churn_predictor AS m;
```

    *  mysql+pymysql://ido%40ploomber.io:***@cloud.mindsdb.com:3306
    7043 rows affected.


In the cell below, we're saving the query output into a dataframe.

We then, take the predicted churn values and the actual churn values into seperate variables.


```python
df = result.DataFrame()
y_pred = df.Churn
y_test = df.actual
```

## Plotting
Now that we have the values needed to evaluate our model, we can plot it into a confusion matrix:


```python
plot.ConfusionMatrix.from_raw_data(y_test, y_pred, normalize=False)
```




    <sklearn_evaluation.plot.classification.ConfusionMatrix at 0x1083dd480>




    
![png](mindsDB_files/mindsDB_19_1.png)
    


Additionally we can generate a classification report for our model and compare it with other different models or previous iterations.


```python
target_names = ["No churn", "churn"]

report = plot.ClassificationReport.from_raw_data(
    y_test, y_pred, target_names=target_names
)
```


    
![png](mindsDB_files/mindsDB_21_0.png)
    


## Conclusion

In conclusion, the integration between Jupysql and MindsDB is a powerful tool for building and deploying predictive models. It allows easy data extraction and manipulation, and makes it simple to deploy models into production. This makes it a valuable tool for data scientists, machine learning engineers, and anyone looking to build predictive models. With this integration, the process of data extraction, cleaning, modeling, and deploying can all be done in one place: your Jupyter notebook. MindsDB on the other hand is making it a more efficient and streamlined process reducing the need for compute.
