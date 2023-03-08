
import java.sql.Timestamp

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Challenge extends App {

  import scala.util.Random
  import java.time.{LocalDateTime, Instant, ZoneId}
  import slick.jdbc.PostgresProfile.api._
  import scala.concurrent.ExecutionContext.Implicits.global

  val categories = List("category1", "category2", "category3")
  val actions = List("action1", "action2", "", "action3")
  val labels = List("label1", "label2", "label3", null)
  val searchTerms = List("term1", "term2", "term3")
  val ips = List("192.168.1.1", "192.168.1.2", "192.168.1.3")
  val userIds = (1 to 100).map(_.toString)
  val firstNames = List("Emma", "Liam", "Ava", null, "Noah", "Sophia")
  val lastNames = List("Smith", "Johnson", "Brown", "Davis", "Garcia")
  val browsers = List("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36 (Google Chrome)", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:83.0) Gecko/20100101 Firefox/83.0 (Mozilla Firefox)", "Mozilla/5.0 (Windows NT 10.0; Trident/7.0; rv:11.0) like Gecko (Microsoft Internet Explorer/Edge)")

  /**
   * Generates Strings like
   * 192.168.1.3;2023-03-05T15:03:28;Liam Brown;22;category2;action2;label2;term2;Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36 (Google Chrome)
   */
  def generateRandomData: String = {
    val random = new Random()
    // random date in the next seven days from now
    val date = LocalDateTime.ofInstant(
      Instant.ofEpochSecond(Instant.now().getEpochSecond + Random.nextInt(7 * 86400)
      ), ZoneId.systemDefault())
    val ip = ips(random.nextInt(ips.length))
    val userId = userIds(random.nextInt(userIds.length))
    val category = categories(random.nextInt(categories.length))
    val action = actions(random.nextInt(actions.length))
    val label = labels(random.nextInt(labels.length))
    val searchTerm = searchTerms(random.nextInt(searchTerms.length))
    val firstName = firstNames(random.nextInt(firstNames.length))
    val lastName = lastNames(random.nextInt(lastNames.length))
    val browser = browsers(random.nextInt(browsers.length))
    s"$ip;$date;$firstName $lastName;$userId;$category;$action;$label;$searchTerm;$browser"
  }

  // Defining Case classes representing the data model
  case class User(userId: Int, firstName: String, lastName: String)
  case class Browser(browserId: Int, browserName: String)
  case class Category(categoryId: Int, categoryName: String)
  case class Action(actionId: Int, actionName: String)
  case class Label(labelId: Int, label: String)
  case class Event(eventId: Int, ipAddress: String, eventDate: Timestamp, userId: Int, categoryId: Int, actionId: Int, labelId: Int,
                   browserId: Int, searchTerm : String)

  class Users(tag: Tag) extends Table[User](tag, "users") {
    def userId = column[Int]("id", O.PrimaryKey)
    def firstName = column[String]("first_name")
    def lastName = column[String]("last_name")
    def * = (userId, firstName, lastName) <> (User.tupled, User.unapply)
  }

  class Browsers(tag: Tag) extends Table[Browser](tag, "browser") {
    def browserId = column[Int]("id", O.PrimaryKey)
    def browserName = column[String]("browser_name")
    def * = (browserId, browserName) <> (Browser.tupled, Browser.unapply)
  }

  class Categories(tag: Tag) extends Table[Category](tag, "category") {
    def categoryId = column[Int]("id", O.PrimaryKey)
    def categoryName = column[String]("category_name")
    def * = (categoryId, categoryName) <> (Category.tupled, Category.unapply)
  }

  class Actions(tag: Tag) extends Table[Action](tag, "action") {
    def actionId = column[Int]("id", O.PrimaryKey)
    def actionName = column[String]("action_name")
    def * = (actionId, actionName) <> (Action.tupled, Action.unapply)
  }

  class Labels(tag: Tag) extends Table[Label](tag, "label") {
    def labelId = column[Int]("id", O.PrimaryKey)
    def label = column[String]("label")
    def * = (labelId, label) <> (Label.tupled, Label.unapply)
  }

  class Events(tag: Tag) extends Table[Event](tag, "event") {
    def eventId = column[Int]("id", O.PrimaryKey)
    def ipAddress = column[String]("ip_address")
    def eventDate = column[Timestamp]("event_date")
    def userId = column[Int]("user_id")
    def categoryId = column[Int]("category_id")
    def actionId = column[Int]("action_id")
    def labelId = column[Int]("label_id")
    def browserId = column[Int]("browser_id")
    def searchTerm = column[String]("search_term")
    def * = (eventId, ipAddress, eventDate, userId, categoryId, actionId, labelId, browserId, searchTerm) <> ((Event.apply _).tupled, Event.unapply)

    def user = foreignKey("USER_FK", userId, TableQuery[Users])(_.userId)
    def browser = foreignKey("BROWSER_FK", browserId, TableQuery[Browsers])(_.browserId)
    def category = foreignKey("CATEGORY_FK", categoryId, TableQuery[Categories])(_.categoryId)
    def action = foreignKey("ACTION_FK", actionId, TableQuery[Actions])(_.actionId)
    def label = foreignKey("LABEL_FK", labelId, TableQuery[Labels])(_.labelId)

    // Unique constraint to avoid duplicates
    //def eventsUnique = index("events_unique", (userId, ipAddress, eventDate), unique = true)
  }


  // Create the table schema and run the corresponding SQL statement
  val createTables = DBIO.seq(
    TableQuery[Events].schema.createIfNotExists,
    TableQuery[Actions].schema.createIfNotExists,
    TableQuery[Users].schema.createIfNotExists,
    TableQuery[Labels].schema.createIfNotExists,
    TableQuery[Categories].schema.createIfNotExists,
    TableQuery[Browsers].schema.createIfNotExists
  )

  // Define database configuration
  val db = Database.forURL("jdbc:postgresql://postgres:5432/testdb",
    user = "postgres",
    password = "postgres",
    driver = "org.postgresql.Driver"
  )

  saveToDatabase(Iterator.fill(30000)(generateRandomData))

  def saveToDatabase(data: Iterator[String]): Unit = {

    try Await.result(db.run(createTables), Duration.Inf) catch {
      case e: Exception => e.printStackTrace()
    }

    data.foreach(record => {
      val cols = record.split(";")

      // USER
      val user = User(cols(3).toInt, cols(2).split(" ")(0), cols(2).split(" ")(1))
      val userPresent = userExists(user.userId)
      // insert the user only if it does not already exist
      val insertUsers = userPresent.flatMap { exists =>
        if (exists) {
          DBIO.successful(0)
        } else {
          DBIO.seq(TableQuery[Users] += user)
        }
      }

      // LABEL
      val label = Label(cols(6).hashCode, cols(6))
      val labelPresent = labelExists(label.labelId)
      // insert the label only if it does not already exist
      val insertLabels = labelPresent.flatMap {exists =>
        if (exists) {
          DBIO.successful(0)
        } else {
          DBIO.seq(TableQuery[Labels] += label)
        }
      }

      // ACTION
      val action = Action(cols(5).hashCode, cols(5))
      val actionPresent = actionExists(action.actionId)
      // insert the action only if it does not already exist
      val insertActions =  actionPresent.flatMap { exists =>
        if (exists) {
          DBIO.successful(0)
        } else {
          DBIO.seq(TableQuery[Actions] += action )
        }
      }

      // CATEGORY
      val category =  Category(cols(4).hashCode, cols(4))
      val categoryPresent = categoryExists(category.categoryId)
      // insert the category only if it does not already exist
      val insertCategories = categoryPresent.flatMap{ exists =>
        if (exists) {
          DBIO.successful(0)
        } else {
          DBIO.seq(TableQuery[Categories]  += category)
        }
      }

      // BROWSER
      val browser = Browser(cols.slice(8, cols.length).mkString(";").hashCode, cols.slice(8, cols.length).mkString(";"))
      val browserPresent = browserExists(browser.browserId)
      // insert the category only if it does not already exist
      val insertBrowsers = browserPresent.flatMap{ exists =>
        if(exists){
          DBIO.successful(0)
        } else {
          DBIO.seq(TableQuery[Browsers]+= browser)
        }
      }

      val insertTables = insertUsers.andThen(insertLabels).andThen(insertActions).andThen(insertCategories).andFinally(insertBrowsers)
      val usersAndLabelsAction = insertTables.transactionally

      val event = Event(record.hashCode, cols(0), Timestamp.valueOf(LocalDateTime.parse(cols(1))), user.userId, category.categoryId, action.actionId, label.labelId, browser.browserId, cols(7))
      val insertEvents = TableQuery[Events]  += event

      // Wait until all the insertions are done
      try Await.result(db.run(usersAndLabelsAction.andThen(insertEvents).transactionally), Duration.Inf) catch {
        case e: Exception => e.printStackTrace()
      }
    })
  }

  def userExists(userId: Int): DBIOAction[Boolean, NoStream, Effect.Read] = {
    TableQuery[Users].filter(_.userId === userId).exists.result
  }

  def categoryExists(categoryId: Int): DBIOAction[Boolean, NoStream, Effect.Read] = {
    TableQuery[Categories].filter(_.categoryId === categoryId).exists.result
  }

  def labelExists(labelId: Int): DBIOAction[Boolean, NoStream, Effect.Read] = {
    TableQuery[Labels].filter(_.labelId === labelId).exists.result
  }

  def actionExists(actionId: Int): DBIOAction[Boolean, NoStream, Effect.Read] = {
    TableQuery[Actions].filter(_.actionId === actionId).exists.result
  }

  def browserExists(browserId: Int): DBIOAction[Boolean, NoStream, Effect.Read] = {
    TableQuery[Browsers].filter(_.browserId === browserId).exists.result
  }


}