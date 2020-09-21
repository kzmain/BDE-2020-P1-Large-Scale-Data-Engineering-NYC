import org.apache.spark.sql.functions._
def reorg(datadir :String) 
{
  val t0 = System.nanoTime()

    val person = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                       load(datadir + "/person.*csv.*")
                       .drop("firstName")
                       .drop("lastName")
                       .drop("gender")
                       .drop("creationDate")
                       .drop("locationIP")
                       .drop("browserUsed")
                       .withColumn("bday", month($"birthday")*100 + dayofmonth($"birthday")).drop("birthday")
                       .cache()

    val knows  = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                       load(datadir + "/knows.*csv.*")
    val loc_df = person.select("personId", "locatedIn").cache()

    //Ensure the same city
     println("REORG: ENSURE THE SAME CITY")
    val knows1 = knows.join(loc_df.withColumnRenamed("locatedIn", "ploc"),    "personId")
                      .join(loc_df.withColumnRenamed("locatedIn", "floc")
                                  .withColumnRenamed("personId", "friendId"), "friendId")
                      .filter($"ploc" === $"floc")
                      .select("personId", "friendId")

    //Ensure mutual friend
    println("REORG: ENSURE MUTUAL FRIEND")
    val knows2 = knows1.join(knows1.withColumnRenamed("friendId", "validation")
                                   .withColumnRenamed("personId", "friendId"), "friendId")
                       .filter($"personId" === $"validation")
                       .select("personId", "friendId")

    knows2.write.format("parquet").mode("overwrite").save(datadir + "/knows_kk.parquet")
    
    //Get friend list
    println("REORG: GET ALL PEOPLE LIST")
    val person_list = knows2.select("personId").dropDuplicates("personId")
    person_list.cache()

    //Remove none-useful person
    println("REORG: REMOVE NONE_USEFULE PERSON")
    person.join(person_list, "personId").write.format("parquet").mode("overwrite").save(datadir + "/person_kk.parquet")
    
    val interest = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                       load(datadir + "/interest.*csv.*").cache()
    //Remove none-useful interests
    println("REORG: REMOVE NONE_USEFULE INTEREST")                   
    interest.join(person_list, "personId").write.format("parquet").mode("overwrite").save(datadir + "/interest_kk.parquet")

  val t1 = System.nanoTime()
  println("reorg time: " + (t1 - t0)/1000000 + "ms")
}

def cruncher(datadir :String, a1 :Int, a2 :Int, a3 :Int, a4 :Int, lo :Int, hi :Int) :org.apache.spark.sql.DataFrame =
{
   val t0 = System.nanoTime()
    
  val person   = spark.read.format("parquet").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                   load(datadir + "/person_kk.parquet").cache()

  val interest = spark.read.format("parquet").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                   load(datadir + "/interest_kk.parquet").cache()
    
  val knows    = spark.read.format("parquet").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                       load(datadir + "/knows_kk.parquet").cache()
  // Filter birthday
  val birth_filter = person.filter($"bday" >= lo && $"bday" <= hi)
  var knows1 = knows.join(birth_filter, "personId")

  // Get who like a1
  val like_a1 = interest
                .filter($"interest" === a1)           
                .withColumnRenamed("personId", "pid")
                .withColumn("fan", lit(true)).drop("interest")
  // Filter Friend NOT like a1 
  val knows2 = knows1.join(like_a1, knows1("friendId") === like_a1("pid"), "left_outer").filter($"fan").drop("fan").drop("pid")
  
  // Filter Person like a1 
  val knows3 = knows2.join(like_a1, knows2("personId") === like_a1("pid"), "left_outer").filter($"fan".isNull).drop("fan").drop("pid")

  // Get score
  val score    = interest.filter($"interest" isin (a2, a3, a4)).groupBy("personId").agg(count("personId") as "score")


  // keep only the (p, f, score) columns and sort the result
  val ret      = knows3.join(score, "personId")
.select($"score", $"personId".alias("p"), $"friendId".alias("f"))
.orderBy(desc("score"), asc("p"), asc("f"))

  ret.show(1000) // force execution now, and display results to stdout

  val t1 = System.nanoTime()
  println("cruncher time: " + (t1 - t0)/1000000 + "ms")

  return ret
}