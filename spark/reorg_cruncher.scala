def reorg(datadir :String) 
{
  val t0 = System.nanoTime()

    val person   = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                      load(datadir + "/person.*csv.*").cache()

//    person.write.format("parquet").save(datadir + "/person.parquet")
    
    val interest = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                       load(datadir + "/interest.*csv.*").cache()
    
//    interest.write.format("parquet").save(datadir + "/interest.parquet")
    
     val knows    = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                        load(datadir + "/knows.*csv.*").cache()
    
//     knows.write.format("parquet").save(datadir + "/knows1.parquet")

  
  val loc_know=knows.join(person.select($"personId",$"locatedIn".alias("ploc"),$"birthday"),"personId").join(person.select($"personId".alias("friendId"),$"locatedIn".alias("floc")),"friendId").filter(
    $"ploc"===$"floc").cache
  loc_know.select($"personId",$"birthday").distinct.write.format("csv").save(datadir+"/new_person")  
  
  val new_know=loc_know.select($"personId",$"friendId").distinct.cache
  new_know.write.format("csv").save(datadir+"/new_knows")
  
  val interest_loc=loc_know.join(interest,"personId").cache
  val new_interest=interest_loc.select($"personId",$"interest").distinct.write.format("csv").save(datadir+"/new_interest")
  new_interest.write.format("csv").save(datadir+"/new_interest")
  

  val t1 = System.nanoTime()
  println("reorg time: " + (t1 - t0)/1000000 + "ms")
}

def cruncher(datadir :String, a1 :Int, a2 :Int, a3 :Int, a4 :Int, lo :Int, hi :Int) :org.apache.spark.sql.DataFrame =
{
   val t0 = System.nanoTime()

  // load the three tables
//   val person   = spark.read.parquet(datadir + "/person.*parquet*")
    
//   val interest = spark.read.parquet(datadir + "/interest.*parquet*")
    
//   val knows    = spark.read.parquet(datadir + "/knows.*parquet*")
   
  
  
//val person   = spark.read.format("parquet").option("header", "true").option("delimiter", "|").option("inferschema", "true").
//                   load(datadir + "/person.parquet")

//val interest = spark.read.format("parquet").option("header", "true").option("delimiter", "|").option("inferschema", "true").
//                   load(datadir + "/interest.parquet")
    
// val knows    = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
 //                      load(datadir + "/knows.*csv.*")

  
 // val person=spark.read.option("header", "false").csv(datadir+"/new_person").toDF("personId","birthday")
 // val interest=spark.read.option("header", "false").csv(datadir+"/new_interest").toDF("personId","interest")
 //val knows=spark.read.option("header", "false").csv(datadir+"/new_knows").toDF("personId","friendId") 
  
  val person   = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                       load(datadir + "/new_person.*csv.*")
  val interest = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                       load(datadir + "/new_interest.*csv.*")
  val knows    = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                       load(datadir + "/new_knows.*csv.*")
  
 
  // select the relevant (personId, interest) tuples, and add a boolean column "nofan" (true iff this is not a a1 tuple)
  val focus    = interest.filter($"interest" isin (a1, a2, a3, a4)).
                          withColumn("nofan", $"interest".notEqual(a1))

  // compute person score (#relevant interests): join with focus, groupby & aggregate. Note: nofan=true iff person does not like a1
  //val scores   = person.join(focus, "personId").
  //                      groupBy("personId", "locatedIn", "birthday").
  //                      agg(count("personId") as "score", min("nofan") as "nofan")
  
  val scores   = person.join(focus, "personId").
                        groupBy("personId", "birthday").
                        agg(count("personId") as "score", min("nofan") as "nofan")

  // filter (personId, score, locatedIn) tuples with score>1, being nofan, and having the right birthdate
  val cands    = scores.filter($"score" > 0 && $"nofan").
                        withColumn("bday", month($"birthday")*100 + dayofmonth($"birthday")).
                        filter($"bday" >= lo && $"bday" <= hi)

  // create (personId, ploc, friendId, score) pairs by joining with knows (and renaming locatedIn into ploc)
 // val pairs    = cands.select($"personId", $"locatedIn".alias("ploc"), $"score").
   //                    join(knows, "personId")
  val pairs    = cands.select($"personId", $"score").
                       join(knows, "personId")  

  
  // re-use the scores dataframe to create a (friendId, floc) dataframe of persons who are a fan (not nofan)
  //val fanlocs  = scores.filter(!$"nofan").select($"personId".alias("friendId"), $"locatedIn".alias("floc"))
 val fanlocs  = scores.filter(!$"nofan").select($"personId".alias("friendId"))
  
  
  // join the pairs to get a (personId, ploc, friendId, floc, score), and then filter on same location, and remove ploc and floc columns
  val results  = pairs.join(fanlocs, "friendId").
 //                      filter($"ploc"===$"floc").
                       select($"personId".alias("p"), $"friendId".alias("f"), $"score")

  // do the bidirectionality check by joining towards knows, and keeping only the (p, f, score) pairs where also f knows p
  val bidir    = results.join(knows.select($"personId".alias("f"), $"friendId"), "f").filter($"p"===$"friendId")

  // keep only the (p, f, score) columns and sort the result
  val ret      = bidir.select($"p", $"f", $"score").orderBy(desc("score"), asc("p"), asc("f"))

  ret.show(1000) // force execution now, and display results to stdout

  val t1 = System.nanoTime()
  println("cruncher time: " + (t1 - t0)/1000000 + "ms")

  return ret
}
