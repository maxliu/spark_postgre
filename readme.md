# Query Postgre from Spark (2.1.1)

The popurse of this blog ( including source code) is to show a reliable way to connect to PostgreSQL server from Spark 2.1.1.

## Preparation to use Postgre in Spark.

### 1) Copy driver jar file to the folder of "lib". Otherwise the workers won't able to use this driver. 
In this case, the file is `postgresql-42.1.1.jar`

### 2) Add the following to scala code.
	
  val driver = "org.postgresql.Driver"
  Class.forName(driver)
  connectionProperties.put("driver", driver)

## Data for testing. 
This is a "popular" SQL question:) given a table of employee with their salaries and departments, find the highest three
slaralies in each department.

The employee table in PostgreSQL database.

### The schema

create table employee (
    id int,
    name char(50),
    salary int,
    department char(50)
    );

#### The data

	+---+--------------------+------+--------------------+
	| id|                name|salary|          department|
	+---+--------------------+------+--------------------+
	|  1|Joe              ...| 70000|IT               ...|
	|  2|Henry            ...| 80000|Sales            ...|
	|  3|Sam              ...| 60000|Sales            ...|
	|  4|Max              ...| 90000|IT               ...|
	|  5|Janet            ...| 69000|IT               ...|
	|  6|Randy            ...| 85000|IT               ...|
	+---+--------------------+------+--------------------+

#### Desired result.
	+--------------------+--------------------+------+
	|          department|                name|salary|
	+--------------------+--------------------+------+
	|IT               ...|Max              ...| 90000|
	|IT               ...|Randy            ...| 85000|
	|IT               ...|Joe              ...| 70000|
	|Sales            ...|Henry            ...| 80000|
	|Sales            ...|Sam              ...| 60000|
	+--------------------+--------------------+------+

## Query by Spark.

#### 1) Read table to a dataframe.

    val employees_table = spark.read.jdbc(jdbc_url, "employee", connectionProperties).cache()

#### 2) Register it.

    employees_table.createGlobalTempView("employee")

#### 3) Run query.

    spark.sql("""
        select department, name, salary
        from (
             select department, name, salary,dense_rank() over(partition by department order by salary desc) salary_rank
             from global_temp.employee
             )  t
        where salary_rank <= 3
        order by department, salary desc
      """)

## Query by Postgre.

#### 1) Define the query.

    var query_str = """
        (select e.department, name, e.salary
        from employee e
        where e.salary in 
              (
                select distinct salary as salary_d
                from employee 
                where department=e.department
                order by salary_d desc
                limit 3
              ) 
        order by e.department, e.salary desc) as e_q
        """

#### 2) Send qury to postgre server and return a spark dataframe.

    spark.read.jdbc(jdbc_url,query_str , connectionProperties)

## _**`please note that the query above (query_str) is not going work in spark sql in Spark 2.1.1 becaue it doesn't allow subqueries to acces out layer varibles.`**_


	

