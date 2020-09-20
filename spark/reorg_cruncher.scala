def reorg(datadir :String) 
{
  val t0 = System.nanoTime()

  val person   = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                       load(datadir + "/person.*csv.*")
  val knows    = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                       load(datadir + "/knows.*csv.*")
  val loc_df = person.select("personId", "locatedIn").cache()
  var nknows = knows
.join(loc_df.withColumnRenamed("locatedIn", "ploc"),    "personId")
.join(loc_df.withColumnRenamed("locatedIn", "floc")
            .withColumnRenamed("personId", "friendId"), "friendId")
.filter($"ploc" === $"floc")
.select("personId", "friendId")


nknows = nknows
.join(nknows.withColumnRenamed("friendId", "validation")
            .withColumnRenamed("personId", "friendId"), "friendId")
.filter($"personId" === $"validation")
.select("personId", "friendId")

nknows.write.format("parquet").save(datadir + "knows.parquet")

val person_list = nknows.select("personId").dropDuplicates("personId")
person_list.cache()

var nperson = person_list
    .join(person, "personId")
    .withColumn("bday", month($"birthday")*100 + dayofmonth($"birthday"))
    .drop("birthday")
    .drop("locatedIn")
    .withColumnRenamed("bday", "birthday")

nperson.write.format("parquet").save(datadir + "/person.parquet")


val interest  = spark.read.format("csv")
                        .option("header", "true")
                        .option("delimiter", "|")
                        .option("inferschema", "true")
                        .load(datadir + "/interest.*csv.*")

var ninterest = interest.join(person_list, "personId")

ninterest.write.format("parquet").save(datadir + "/interest.parquet")

  val t1 = System.nanoTime()
  println("reorg time: " + (t1 - t0)/1000000 + "ms")
}

def cruncher(datadir :String, a1 :Int, a2 :Int, a3 :Int, a4 :Int, lo :Int, hi :Int) :org.apache.spark.sql.DataFrame =
{
   val t0 = System.nanoTime()

  val interest = spark.read.format("parquet").load(datadir + "/interest.parquet").cache()
val person   = spark.read.format("parquet").load(datadir + "/person.parquet").cache()
val knows    = spark.read.format("parquet").load(datadir + "/knows.parquet").cache()

  // Filter Person between birthdays
val target = person.filter($"birthday" >= lo && $"birthday" <= hi).drop("birthday")
var nknows = knows.join(target, "personId")
nknows.count()
// Filter Friend not like a1 
import org.apache.spark.sql.functions._
val like_a1 = interest
                .filter($"interest" === a1)           
                .withColumnRenamed("personId", "pid")
                .withColumn("fan", lit(true)).drop("interest")

nknows = nknows.join(like_a1, nknows("friendId") === like_a1("pid"), "left_outer").filter($"fan").drop("fan").drop("pid")
nknows.count()
// Filter Person like a1 
nknows = nknows.join(like_a1, nknows("personId") === like_a1("pid"), "left_outer").filter($"fan".isNull).drop("fan").drop("pid")
nknows.count()
  // select the relevant (personId, interest) tuples, and add a boolean column "nofan" (true iff this is not a a1 tuple)
  val score    = interest.filter($"interest" isin (a2, a3, a4))
                        .groupBy("personId")
                        .agg(count("personId") as "score")

val ret = nknows.join(score, "personId")
.select($"score", $"personId".alias("p"), $"friendId".alias("f"))
.orderBy(desc("score"), asc("p"), asc("f"))


  //// keep only the (p, f, score) columns and sort the result
  // val ret      = bidir.select($"p", $"f", $"score").orderBy(desc("score"), asc("p"), asc("f"))

  // ret.show(1000) // force execution now, and display results to stdout

  val t1 = System.nanoTime()
  println("cruncher time: " + (t1 - t0)/1000000 + "ms")

  return ret
}
