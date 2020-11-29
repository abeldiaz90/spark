import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.log4j.{ LogManager, Logger }
import org.apache.spark.sql.SaveMode

object Principal {
  val LOGGER: Logger = LogManager.getLogger("Conexiones")
  val SPARK = SparkSession.builder
    .master("local")
    .appName("Conexiones Base de Datos")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  LOGGER.info("SE CREA EL DRIVER DE SPARK SESSION")

  var jdbcHostname = "DESKTOP-4K4N6J4"
  var jdbcPort = 1433
  var jdbcDatabase = "personas"
  var jdbcUsername = "SA"
  var jdbcPassword = "Imperio90"
  var jdbcUrlSqlServer = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

  def main(args: Array[String]): Unit = {
    Conexion(args(0).toInt)
  }

  def Conexion(Opcion: Int): Unit = {
    Opcion match {
      case 1 => sqlServer("DESKTOP-4K4N6J4", 1433, "personas", "SA", "Imperio90")
      case 2 => postgress("localhost", 5432, "postgres", "postgres", "Imperio90")
      case 3 => MySql("127.0.0.1", 3306, "tienda", "root", "Imperio90")
      case 4 => Oracle("localhost", 3306, "orcl", "Sys as Sysdba", "Imperio90")
    }
  }

  def sqlServer(Servidor: String, Puerto: Int, BD: String, Usuario: String, Password: String): Unit = {
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    LOGGER.info("EL PROCESO SELECCIONADO ES  SQL SERVER")
    import java.util.Properties
    val connectionProperties = new Properties()
    connectionProperties.put("user", s"${Usuario}")
    connectionProperties.put("password", s"${Password}")
    val tabla_personas = SPARK.read.jdbc(jdbcUrlSqlServer, "personas", connectionProperties)
    LOGGER.info("SE HACE LA CONEXION A SQL SERVER")
    tabla_personas.printSchema()
    tabla_personas.show()
    EscribirArchivo(tabla_personas)
    EscribirTablaDB(tabla_personas, jdbcUrlSqlServer, "sparktable");
  }

  def postgress(Servidor: String, Puerto: Int, BD: String, Usuario: String, Password: String): Unit = {
    Class.forName("org.postgresql.Driver");
    LOGGER.info("EL PROCESO SELECCIONADO ES POSTGRESS")
    val jdbcUrl = s"jdbc:postgresql://${Servidor}:${Puerto}/${BD}"
    import java.util.Properties
    val connectionProperties = new Properties()
    connectionProperties.put("user", s"${Usuario}")
    connectionProperties.put("password", s"${Password}")
    val tabla_personas = SPARK.read.jdbc(jdbcUrl, "personas", connectionProperties)
    tabla_personas.printSchema()
    tabla_personas.show()
    EscribirArchivo(tabla_personas)
    EscribirTablaDB(tabla_personas, jdbcUrlSqlServer, "sparktable");
  }

  def MySql(Servidor: String, Puerto: Int, BD: String, Usuario: String, Password: String): Unit = {
    Class.forName("com.mysql.jdbc.Driver");
    LOGGER.info("EL PROCESO SELECCIONADO ES MYSQL")
    val jdbcUrl = s"jdbc:mysql://${Servidor}:${Puerto}/${BD}?useTimezone=true&serverTimezone=UTC"
    import java.util.Properties
    val connectionProperties = new Properties()
    connectionProperties.put("user", s"${Usuario}")
    connectionProperties.put("password", s"${Password}")
    val tabla_personas = SPARK.read.jdbc(jdbcUrl, "personas", connectionProperties)
    tabla_personas.printSchema()
    tabla_personas.show()
    EscribirArchivo(tabla_personas)
    EscribirTablaDB(tabla_personas, jdbcUrlSqlServer, "sparktable");
  }

  def Oracle(Servidor: String, Puerto: Int, BD: String, Usuario: String, Password: String): Unit = {
    Class.forName("oracle.jdbc.driver.OracleDriver");
    LOGGER.info("EL PROCESO SELECCIONADO ES ORACLE")
    val jdbcUrl = s"jdbc:oracle:thin:@localhost:1521:orcl"
    import java.util.Properties
    val connectionProperties = new Properties()
    connectionProperties.put("user", s"${Usuario}")
    connectionProperties.put("password", s"${Password}")
    val tabla_personas = SPARK.read.jdbc(jdbcUrl, "personas", connectionProperties)
    tabla_personas.printSchema()
    tabla_personas.show()
    EscribirArchivo(tabla_personas)
    EscribirTablaDB(tabla_personas, jdbcUrlSqlServer, "sparktable");
  }

  def EscribirArchivo(datos: DataFrame): Unit = {
    val tsvWithHeaderOptions: Map[String, String] = Map(("delimiter", "\t"), ("header", "true"))
    datos.coalesce(1).write.mode(SaveMode.Append).options(tsvWithHeaderOptions).csv("output/path")
  }

  def EscribirTablaDB(df: DataFrame, url: String, tablename: String) {
    df.write.format("jdbc").mode("append").option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").option("url", url)
      .option("dbtable", tablename)
      .option("user", "SA")
      .option("password", "Imperio90")
      .save()
    LOGGER.info("SE ESCRIBE LA TABLA DE SPARK");
  }
}