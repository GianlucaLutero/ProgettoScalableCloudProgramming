
import java.sql.{Connection,DriverManager}

object DatabaseConnection {
  val url = "jdbc:mysql://localhost/soccer?useLegacyDatetimeCode=false&serverTimezone=UTC"
  val driver = "com.mysql.cj.jdbc.Driver"
  val username = "root"
  val password = "root"
  var connection:Connection = _
  try {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement
    val rs = statement.executeQuery("SELECT player_name FROM player")
    while (rs.next) {
      val host = rs.getString("player_name")
    //  val user = rs.getString("user")
      println("host = %s".format(host))
    }
  } catch {
    case e: Exception => e.printStackTrace
  }
  connection.close
}
