var interests: org.apache.spark.sql.DataFrame;
var persons  : org.apache.spark.sql.DataFrame;

def reorg(datadir :String) 
{
  val t0 = System.nanoTime()

  val person = spark.read.format("csv")
                    .option("header", "true")
                    .option("delimiter", "|")
                    .option("inferschema", "true")
                    .load(datadir + "/person.*csv.*")
                    .select("personId", "birthday", "locatedIn").cache()
                    
  val knows  = spark.read.format("csv")
                        .option("header", "true")
                        .option("delimiter", "|")
                        .option("inferschema", "true")
                        .load(datadir + "/knows.*csv.*")
  
  val loc_df = person.select("personId", "locatedIn").cache()


  var nknows = knows
  .join(loc_df.withColumnRenamed("locatedIn", "ploc"),    "personId")
  .join(loc_df.withColumnRenamed("locatedIn", "floc")
              .withColumnRenamed("personId", "friendId"), "friendId")
  .filter($"ploc" === $"floc")
  .select("personId", "friendId").cache()

  nknows = nknows
  .join(nknows.withColumnRenamed("friendId", "validation")
              .withColumnRenamed("personId", "friendId"), "friendId")
  .filter($"personId" === $"validation")
  .select("personId", "friendId")
  .groupBy("personId").agg(collect_list("friendId").as("friendId")).cache()

  val person_list = nknows.select("personId").dropDuplicates("personId").cache()

  var nperson = nknows
    .join(person, "personId")
    .withColumn("bday", month($"birthday")*100 + dayofmonth($"birthday"))
    .drop("birthday")
    .drop("locatedIn")
    .withColumnRenamed("bday", "birthday")

  nperson.write.format("parquet").mode("overwrite").save("person.parquet")

  person.unpersist()
  nknows.unpersist()
  nperson.unpersist()

  val interest  = spark.read.format("csv")
                        .option("header", "true")
                        .option("delimiter", "|")
                        .option("inferschema", "true")
                        .load(datadir + "/interest.*csv.*").cache()

  var ninterest = interest.join(person_list, "personId")
  .groupBy("interest")
  .agg(collect_list("personId").as("personId"))

  ninterest.write.format("parquet").mode("overwrite").save("interest.parquet")
  

  val t1 = System.nanoTime()
  println("reorg time: " + (t1 - t0)/1000000 + "ms")
}

def cruncher(datadir :String, a1 :Int, a2 :Int, a3 :Int, a4 :Int, lo :Int, hi :Int) :org.apache.spark.sql.DataFrame =
{
   val t0 = System.nanoTime()

  val interest = spark.read.format("parquet").load("interest.parquet").cache()
  val person   = spark.read.format("parquet").load("person.parquet").cache()

  // Filter Person between birthdays
var target = person
.filter($"birthday" >= lo && $"birthday" <= hi)
.drop("birthday")
.withColumn("friendId", explode($"friendId"))

// Filter Friend not like a1 
import org.apache.spark.sql.functions._
val like_a1 = interest
                .filter($"interest" === a1)
                .withColumn("personId", explode($"personId"))
                .withColumnRenamed("personId", "pid")
                .withColumn("fan", lit(true)).drop("interest").cache()

target = target.join(like_a1, target("friendId") === like_a1("pid"), "left_outer")
                .filter($"fan")
                .drop("fan")
                .drop("pid")

// Filter Person like a1 
target = target.join(like_a1, target("personId") === like_a1("pid"), "left_outer")
                .filter($"fan".isNull)
                .drop("fan")
                .drop("pid").cache()  

  // select the relevant (personId, interest) tuples, and add a boolean column "nofan" (true iff this is not a a1 tuple)
  val score    = interest.filter($"interest" isin (a2, a3, a4))
                .withColumn("personId", explode($"personId"))
                        .groupBy("personId")
                        .agg(count("personId") as "score")
                        .cache()  

  val ret =   target.join(score, "personId")
.select($"score", $"personId".alias("p"), $"friendId".alias("f"))
.orderBy(desc("score"), asc("p"), asc("f"))

  // ret.show(1000) // force execution now, and display results to stdout

  val t1 = System.nanoTime()
  println("cruncher time: " + (t1 - t0)/1000000 + "ms")

  return ret
  // return ret
}
