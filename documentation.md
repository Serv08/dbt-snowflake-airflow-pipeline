| TOOL      | DESCRIPTION      |
| --------- | ---------------- |
| dbt       | transformation   |
| snowflake | data warehousing |
| airflow   | orchestration    |

# Table of Contents:
0. [Table of Contents:](#table-of-contents)
1. [Step 1: Setting up dbt, Snowflake database](#step-1-setting-up-dbt-snowflake-database)
	- [Install dbt Core](#install-dbt-core)
	- [Set up database in Snowflake](#set-up-database-in-snowflake)
	- [Initialize dbt](#initialize-dbt)
2. [Step 2: Configuring `dbt_project.yml` and packages file](#step-2-configuring-dbt_projectyml-and-packages-file)
	- [Configuring models](#configuring-models)
	- [Installing third-party libraries for creating surrogate keys](#installing-third-party-libraries-for-creating-surrogate-keys)
3. [Step 3: Create source and staging tables](#step-3-create-source-and-staging-tables)
	- [Create source table by configuring `tpch_sources.yaml` file](#create-source-table-by-configuring-tpch_sourcesyaml-file)
	- [Creating queries](#creating-queries)
4. [Step 4: Transformed Models (fact tables and data marts)](#step-4-transformed-models-fact-tables-and-data-marts)
	- [Understanding *Fact tables*](#understanding-fact-tables)
	- [INTERMEDIATE MODELS](#intermediate-models)
	- [Creating intermediate order items model](#creating-intermediate-order-items-model)
5. [Step 5: Create Macros](#step-5-create-macros)
	- [Understanding Macro](#understanding-macro)
	- [Creating Macro](#creating-macro)
	- [Using macro function](#using-macro-function)
	- [Create a summary of the order items model](#create-a-summary-of-the-order-items-model)
	- [Creating a fact table](#creating-a-fact-table)
6. [Step 6: Generic and Singular Tests](#step-6-generic-and-singular-tests)
	- [Generic test](#generic-test)
	- [Singular test](#singular-test)
7. [Step 7: Orchestration using Airflow](#step-7-orchestration-using-airflow)
	- [Creating connection to Snowflake in Airflow](#creating-connection-to-snowflake-in-airflow)
8. [Commands when ran from reboot](#commands-when-ran-from-reboot)



# Step 1: Setting up dbt, Snowflake database

*For this setup, I am using WSL with Ubuntu distro.*
## [Install dbt Core](https://docs.getdbt.com/docs/core/pip-install)
```shell
pip install dbt-snowflake
```

```shell
pip install dbt-core
```

## Set up database in Snowflake
1. Create warehouse, database, row.
2. In database, create schema. Write dbt tables into.
3. For role, the role is assigned in user in snowflake.

```SQL
use role accountadmin; -- snowflake supper user to setup wh, db, and role

create warehouse dbt_wh with warehouse_size='x-small'; -- warehouse
create database if not exists dbt_db; -- database
create role if not exists dbt_role; -- role

show grants on warehouse dbt_wh;

-- this grants the create role to the user by indicating their name 
grant usage on warehouse dbt_wh to role dbt_role;
grant role dbt_role to user servin;
grant all on database dbt_db to role dbt_role;

-- use the assigned for this database to the user
use role dbt_role;

create schema dbt_db.dbt_schema;
```

In this project, 

When dropping warehouse later
```SQL
use role accountadmin;

drop warehouse if exists dbt_wh;
drop database if exists dbt_db;
drop role if exists dbt_role;
```

## Initialize dbt
```
dbt init
```

Enter the credentials needed based on the created database in Snowflake.

|              | input                | description                                                                                                                                                                                                                                                                                                                |
| ------------ | -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Project name | *data_pipeline*      | This will be the name of your project folder.                                                                                                                                                                                                                                                                              |
| Database env | `Snowflake`          | Choose the number for `Snowflake`.                                                                                                                                                                                                                                                                                         |
| Account      | `account_identifier` | For your account, go to your `Snowflake` environment then go to `Account` and get your [`account_identifier`](https:..docs.snowflake.com/en/user-guide/admin-account/identifier) from `Locator`.<br>In your web URL, you can also get your `account_identifier`:<br>`https://<account_identifier>.snowflakecomputing.com`. |
| Role         | *dbt_role*           | This is the role to be used on the project which was set earlier in the Snowflake env.                                                                                                                                                                                                                                     |
| Warehouse    | *dbt_wh*             | This is the warehouse from the Snowflake env.                                                                                                                                                                                                                                                                              |
| Database     | *dbt_db*             | This is the database from the Snowflake env.                                                                                                                                                                                                                                                                               |
| Schema       | *dbt_schema*         | This is the schema from the database in Snowflake env.                                                                                                                                                                                                                                                                     |
| threads      | *10*                 | [This controls the maximum number of models that will get sent to Snowflake for execution at any given time.](https://select.dev/posts/snowflake-dbt-threads#:~:text=To%20summarize%20it%20more%20simply,execution%20at%20any%20given%20time.)                                                                             |
When using WSL, you can check your Snowflake profile in `.dbt/profiles.yaml`.


# Step 2: Configuring `dbt_project.yml` and packages file

## Configuring models
Remove the `/examples` from `/models` create `/staging` folder.
Your folder should now have 
```bash
analysis/
dbt_packages/
logs/
macros/
models/
	marts/
	staging/
seeds/
snapshots/
tests/
dbt_project.yml
```

`staging/` files are one-to-one with your source file. This basically means that it takes the data from the source file into a metadata that DBT can use directly (*instead of taking the data directly from the source*).

On the `dbt_project.yml`, modify the `models` to be used:
```yml
models:
	data_pipeline:
		staging: # this model is materialized as view
			+materialized: view
			snowflake_warehouse: dbt_wh
		marts:   # this model is materialized as table
			+materialized: tabel
			snowflake_warehouse: dbt_wh
```
`dbt_project.yml` tells DBT where to look for models. Models is where the SQL logic will run
## Installing third-party libraries for creating surrogate keys
To add third-party libraries, create `packages.yml` file in the `data_pipeline/` folder

```bash
analysis/
dbt_packages/
logs/
macros/
models/
	marts/
	staging/
seeds/
snapshots/
tests/
dbt_project.yml
packages.yml
```

```yml
pacakages:
	- package: dbt-labs/dbt_utils  # this is a common DBT package
	version: 1.3.0
```

To install these packages, run this in the terminal
```bash
dbt deps
```
This installs the packages and other dbt dependencies.

[test types](https://docs.getdbt.com/docs/build/data-tests)



# Step 3: Create source and staging tables

## Create source table by configuring `tpch_sources.yaml` file
```YAML
version: 2

sources:
  - name: tpch
    database: snowflake_sample_data
    schema: tpch_sf1
    tables:
      - name: orders
        columns:
          - name: o_orderkey
            tests:
              - unique
              - not_null
      - name: lineitem
        columns:
          - name: l_orderkey
            tests:
              - relationships:
                  to: source('tpch', 'orders')
                  field: o_orderkey
```
To break down, the sources used is from `tpch` dataset that is built-in in Snowflake environment. We then accessed the the `orderkeys` from both `orders` and `lineitem`. 

For the `o_orderkey`, the test done checks whether all `o_orderkey`, the PM from table `orders`, are `unique` and `not_null`.

The `l_orderkey` is the FK of the `lineitem` table that references `o_orderkey` from the `orders` table. It is tested if for its relation ship with `orders` table.

## Creating queries
After configuring the `tpch_sources.yaml` file in the `models/staging/`, extract the data from the source with an SQL query `stg_tpch_orders.sql`
```SQL
SELECT
	*
FROM
	{{ source('tpch', 'orders') }}
```

To run the queries and tests in the `models/staging`, run the command:
```bash
dbt run
```

Running this command will create a table named `stg_tpch_orders` under `dbt_schema` within `dbt_db` in the Snowflake environment.  

You can modify the query in `stg_tpch_orders.sql` by renaming the field names of the original source.
```SQL
SELECT
    o_orderkey as order_key,
    o_custkey as customer_key,
    o_orderstatus as status_code,
    o_totalprice as total_price,
    o_orderdate as order_date
FROM
    {{ source('tpch', 'orders') }}
```

You also need to query the fields from `lineitems` from the source. For this, you need to create another query file, `stg_tpch_line_items.sql`
```SQL
SELECT
    {{
        dbt_utils.generate_surrogate_key([
            'l_orderkey',
            'l_linenumber'
        ])
    }} AS order_item_key,
    l_orderkey AS order_key,
    l_partkey AS part_key,
    l_linenumber AS line_number,
    l_quantity AS quantity,
    l_extendedprice AS extended_price,
    l_discount AS discount_percentage,
    l_tax AS tax_rate
FROM
    {{ source('tpch', 'lineitem') }}
```

The `order_item_key` is a **surrogate key** used in the `stg_tpch_line_items` table. This is created using `dbt_utils.generate_surrogate_key()` function and references the `l_orderkey` and `l_linenumber` fields.

To run a specific model, this is the format:
```bash
dbt run -s <query_file_name>
```

Since we haven't run `stg_tpch_line_items.sql`, we can do this with the following command:
```bash
dbt run -s stg_tpch_line_items
```


# Step 4: Transformed Models (fact tables and data marts)

## Understanding *Fact tables*
In a company, business transformations are needed, hence, quantitative data are needed to perform calculation. Remember that staging tables are one-to-one with the source table, thus, we cannot use them directly to perform such calculations. In this part, we introduce ***fact tables***.

**Fact tables**: 
- [These are tables that have quantitative metrics that are used for calculation or is a result of calculations](https://builtin.com/articles/fact-table-vs-dimension-table#:~:text=A%20fact%20table%20is%20one,and%20time%2Dseries%20financial%20data.)
- These tables references quantitative values from other tables, hence, it always have FK/s.

## INTERMEDIATE MODELS
Before jumping to Fact tables, let us introduce **intermediate models.** Intermediate models are ambiguous models that intermediate staging, marts, or report models. We can use intermediate models to centralized necessary tables for the other tables *(such as fact tables)* rather than directly referencing them from our staging models.
## Creating intermediate order items model
We can create an intermediate table in the `models/marts/` with a query named `int_order_items.sql`
```SQL
SELECT
    line_item.order_item_key,
    line_item.part_key,
    line_item.line_number,
    line_item.extended_price,
    orders.order_key,
    orders.customer_key,
    orders.order_date
FROM
    {{ ref('stg_tpch_orders') }} AS orders
JOIN
    {{ ref('stg_tpch_line_items') }} AS line_item
    ON orders.order_key = line_item.order_key
ORDER BY
    orders.order_date
```

To run this, make sure you run both the referenced models from the `staging/`
```bash
dbt run -s stg_tpch_orders
dbt run -s stg_tpch_line_items
dbt run -s int_order_items
```


# Step 5: Create Macros

## Understanding Macro
***Macro functions*** are functions that are essential in data transformation. They are necessary in building business logics that are used across multiple models. 
It is a reusable SQL code similar to that of a function in programming language.

## Creating Macro
In the `macros/` folder, create an SQL file named `pricing.sql` (*the file name doesn't really matter*)
```macro
{% macro discounted_amount(extended_price, discount_percentage, scale=2) %}
    (-1 * {{extended_price}} * {{discount_percentage}})::decimal(16, {{ scale }})
{% endmacro %}
```

`discounted_amount` is the name of the is macro function. It takes two parameters, represented by `extended_price` and `discount_percentage`. 

Another parameter is `scale` which is defaulted to the value of 2. `scale` is used to determine the number of decimal places.

For the logic of the function, we have :
```macro
(-1 * {{extended_price}} * {{discount_percentage}})::decimal(16, {{ scale }})
```
`extended_price` is multiplied to the value of `discount_percentage` that is set to negative indicated by the multiplication of `-1` while `::decimal(16, {{ scale }})` indicates the type of the returned value.

## Using macro function
We can now use the macro function to our fact table query.
```SQL
SELECT
    line_item.order_item_key,
    line_item.part_key,
    line_item.line_number,
    line_item.extended_price,
    orders.order_key,
    orders.customer_key,
    orders.order_date,
    {{ discounted_amount('line_item.extended_price', 'line_item.discount_percentage') }} AS item_discount_amount
FROM
    {{ ref('stg_tpch_orders') }} AS orders
JOIN
    {{ ref('stg_tpch_line_items') }} AS line_item
    ON orders.order_key = line_item.order_key
ORDER BY
    orders.order_date
```

Our macro function, `discounted_amount`, takes `'line_item.extended_price'` and `'line_item.discount_percentage'` as parameters and is aliased as `item_discount_amount` 
*(`scales` is set to default value)*

## Create a summary of the order items model
In the same folder as `int_orders_items.sql`, create `int_orders_items_summary.sql`
```SQL
SELECT
    order_key,
    sum(extended_price) AS gross_item_sales_amount,
    sum(item_discount_amount) AS item_discount_amount
FROM
    {{ ref('int_order_items') }}
GROUP BY
    order_key
```
This summarizes the intermediate model by using the `GROUP BY` clause to `order_key`

## Creating a fact table
In the `models/marts/`, create `fct_orders.sql`
```SQL
SELECT
    orders.*,
    order_item_summary.gross_item_sales_amount,
    order_item_summary.item_discount_amount
FROM
    {{ ref('stg_tpch_orders') }} AS orders
JOIN
    {{ ref('int_order_items_summary') }} AS order_item_summary
    ON orders.order_key = order_item_summary.order_key
ORDER BY
    order_date
```

This is how your structure should look like by now:
```bash
analysis/
dbt_packages/
logs/
macros/
	pricing.sql
models/
	marts/
		fct_orders.sql
		int_orders_items_summary.sql
		int_orders_items.sql
	staging/
		stg_tcph_line_items.sql
		stg_tcph_orders.sql
		tpch_sources.yml
seeds/
snapshots/
tests/
dbt_project.yml
packages.yml
```


# Step 6: Generic and Singular Tests

***The purpose of tests is to run queries that returns 0. If it returns a non-zero output, then the test failed.***

[2 types of tests in dbt](https://docs.getdbt.com/docs/build/data-tests#:~:text=A%20singular%20data,should%20use%20them!):
- ***Singular test*** - SQL queries that returns failing rows. Simplest form of data test.
- ***Generic test*** - These are test query that are parameterized *(macros)* which means it can be reused. Once defined, they can be reference to a `.yml` file.

## Generic test
Under `models/marts/`, create `generic_test.yml` *(file name doesn't matter)*
```yml
models:
  - name: fct_orders
    columns:
      - name: order_key # test order_key col if unique, not_null, and its relationships with the reference
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('stg_tpch_orders')
              field: order_key
              severity: warn
      - name: status_code # test values under accepted_values
        tests:
          - accepted_values:
              values: ['P', 'O', 'F']
```
What this code do is test 2 things in the `fct_orders` table. 
The first test is to check `order_key` field if it is unique, non empty, and its relationship with `stg_tpch_orders`.
The second test checks the `status_code` field where the only accepted values within that field are `P`, `O`, and `F`.

## Singular test
For Singular Test, under `tests/` folder, create a query `fct_orders_discount.sql`
```sql
SELECT
	*
FROM
	{{ ref('fct_orders') }}
WHERE
	item_discount > 0
```
This query checks `item_discount` field if the value is greater than `0`. Take note that all values for `item_discount` should be less than `0` it represents a decimal value.

Another singular test for checking date range: `tests/fct_orders_date_valid.sql`
```SQL
SELECT
    *
FROM
    {{ ref('fct_orders') }}
WHERE
    date(order_date) > CURRENT_DATE()
    OR date(order_date) < date('1990-01-01')
```
This query returns rows where the date in the `order_date` field is beyond the current time (`CURRENT_DATE()`) or before `01-01-1990`. If no such rows exist, then this test will be successful.

This is how your structure should look like by now:
```bash
analysis/
dbt_packages/
logs/
macros/
	pricing.sql
models/
	marts/
		fct_orders.sql
		generic_test.yml
		int_orders_items_summary.sql
		int_orders_items.sql
	staging/
		stg_tcph_line_items.sql
		stg_tcph_orders.sql
		tpch_sources.yml
seeds/
snapshots/
tests/
	fct_orders_discount.sql
	fct_orders_valid_date.sql
dbt_project.yml
packages.yml
```


# Step 7: Orchestration using Airflow

To orchestrate with `dbt` (*deploy `dbt` DAG*) with `Airflow`, we can use [***Cosmos***](https://astronomer.github.io/astronomer-cosmos/) by Astronomer.

To  install `astro`, I used [`brew`](https://docs.brew.sh/Homebrew-on-Linux) in my `WSL` environment:
```bash
brew install astro
```

To make a new project with `Airflow`, we need to create project where we can initialize it. This should be outside the `data-pipeline` folder.
```bash
mkdir dbt-dag && cd dbt-dag
astro dev init
```

This is what your shoulder would look like:
```bash
.astro/
dags/
include/
plugins/
tests/
.dockerignore
.env
airflow_settings.yaml
Dockerfile
packages.txt
README.md
requirements.txt
```

Modify the `Dockerfile`
```Docker
FROM quay.io/astronomer/astro-runtime:12.2.0

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate
```

Modify `requirements.txt`
```text
astronomer-cosmos
apache-airflow-providers-snowflake
```

Run the project
```bash
astro dev start
```
This will run a container with the following configuration from the `Dockerfile` and install the packages in the `requirements.txt`.

Upon successful run, go and log in to the `localhost:8080/login/`.
By default, the username and password is `admin`.

In order to deploy `data_pipeline` with `dbt`, copy the  `data_pipeline` folder into `dbt-dag/dags/dbt/`. 
```bash
cp -r home/user/data_pipeline home/user/dbt-dag/dags/dbt
```
Namespacing is important as to not confuse the configuration file where to look.

Modify the `dbt_dag` file in `dags/dbt`:
```python
import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",
        profile_args={
            "database": "dbt_db",
            "schema": "dbt_schema"
        },
    )
)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig("/usr/local/airflow/dags/dbt/data_pipeline",),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
    schedule_interval="@daily",
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="dbt_dag",
)
```
Make sure the `database` and `schema` within `profile_args` in the `profile_config` are the same database and schema created in the Snowflake environment.
```python
 profile_args={
            "database": "dbt_db",
            "schema": "dbt_schema"
        },
```

## Creating connection to Snowflake in Airflow
1. Go to `localhost:8080/connection/list`
2. Create new connection
3. Fill up the connection configuration 
	- Connection id: `snowflake_conn`
	- Connection type: `Snowflake`
	For the following details, you can check `home/user/.dbt/profiles.yaml`
	- Login: `<your_snowflake_account>`
	- Password: `<your_snowflake_password>`
	- Warehouse: `dbt_wh`
	- Database: `dbt_db`
	- Role: `dbt_role`
		
After configuring the connection, you should be able to successfully run `dbt_dag` on DAGs section in Airflow.


# Commands when ran from reboot
---
1. Start the container by running `astro dev start` inside `dbt-dag` project or run the container in Docker GUI.
2. `dbt docs generate` to generate report and `dbt docs serve` to launch documentation locally.
