package com.sparkPostgre

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.storage._

import com.typesafe.config.{ConfigObject, ConfigValue, ConfigFactory, Config}  


object spark_postgresql{

  val driver = "org.postgresql.Driver"

  val config = ConfigFactory.load()
  val spark_master = config.getString("sparkmaster")
  val jdbcHostname = config.getString("hostname")
  val jdbcPort = config.getString("hostport")
  val jdbcDatabase = config.getString("database")

  val jdbc_url = s"jdbc:postgresql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"
  val connectionProperties = new Properties()

  connectionProperties.put("user", config.getString("username"))
  connectionProperties.put("password", config.getString("userpassword"))
  connectionProperties.put("driver", driver)

  Class.forName(driver)

  val spark = SparkSession.builder()
         .master(spark_master)
         .appName("cache-spark-test")
         .config("spark.jars", "lib/postgresql-42.1.1.jar")
         .getOrCreate()

  spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  spark.conf.set("spark.core.connection.ack.wait.timeout", "6000")
  spark.conf.set("spark.akka.frameSize", "100")

  //query by spark
  def queryBySpark():DataFrame = {
  
    val employees_table = spark.read.jdbc(jdbc_url, "employee", connectionProperties).cache()
    
    employees_table.printSchema
    
    employees_table.show

    employees_table.createGlobalTempView("employee")

    spark.sql("""
        select department, name, salary
        from (
             select department, name, salary,dense_rank() over(partition by department order by salary desc) salary_rank
             from global_temp.employee
             )  t
        where salary_rank <= 3
        order by department, salary desc
      
      """)
  }
  
  // query by postgresql
  def queryByPostgre():DataFrame = {

    // This query is not going to work in Spark.
    // Spark < 2.0 doesn't support subquery in WHERE clause
    // Spark >=2.0 supports subquery in WHERER clause but doesn't allow subquery to 
    // access outer layer varibles.

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


    spark.read.jdbc(jdbc_url,query_str , connectionProperties)
  }

  def main(args: Array[String]){
  
     print("\n\n\n\nquery by ")

     queryBySpark().show

     print("\n\n\n\nquery by postgre")

     queryByPostgre().show

     spark.stop
  }


}
