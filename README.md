# Spark for stars
Author : YANG XIAN  

In this project we want to develop an algorithm, which can partition a big amount of huge files of stars (approx. 5G each) stored in a Hadoop File System on a multi-server cluster, into smaller files (128M) with the requirement that stars near to each other should stay together in the same ouput file, in order to simplify the downstream use and processing of these astronomic data.  
  
Each star (record) includes a series of attributes orginally, among others the Right Ascension (`RA`) and the Declination (`Decl`). See  
https://de.wikipedia.org/wiki/%C3%84quatoriales_Koordinatensystem

 &ensp; &ensp; 
 &ensp; &ensp; 
  
## I. Calculation of the boundaries of the observed region


Firstly, we ignore the celestial coordinate system and take a simplified point of view as if it was a Cartesian coordinate system. That means, `RA` and `Decl` are just considered to be x-coordinate and y-coordinate. Altough this is not true in the practice, it will help us develop the key parts of our algorithm. In order to split big files into smaller ones, we would like to cut the entire celestrial region to analyse into small identical rectangle cases, where we speak of a _grid_.
 
  
To get started, we calculate the boundaries of the celestial region, considered as a big rectangle containing all the stars. A Spark-friendly approach to do this is to define a `case classe Zone` which represents an abritrary rectangle

```scala
  case class Zone(minRA: Double, maxRA: Double,
                  minDecl: Double, maxDecl: Double) {
    def closuringZone(b: Zone): Zone = {
      Zone(min(minRA, b.minRA), max(maxRA, b.maxRA),
        min(minDecl, b.minDecl), max(maxDecl, b.maxDecl))
    }
    def toString: String = {
      "minRA : %f, maxRA : %f, minDecl : %f, maxDecl : %f"
        .format(minRA, maxRA, minDecl, maxDecl)
    }
  }
```
Which contains a method `closuringZone` to allow Spark to `reduce` 2 rectangles to 1 in the future.  
    
The function `convertToZone` allows to initialize the problem, in which it converts a point to a `Zone` by repeating its coordinates.  

```scala
  def convertToZone (input: Array[String]): Zone = {
    Zone(input(PetaSkySchema.s_ra).toDouble, input(PetaSkySchema.s_ra).toDouble,
      input(PetaSkySchema.s_decl).toDouble, input(PetaSkySchema.s_decl).toDouble)
  }
```

Finally, a `map` which converts all points to a `Zone` followed by a `reduce` which adding the zones up one by one till the last.  

```scala
  def calculateMaxMin(inputDir: String, sc: SparkContext):
  Zone = {
    sc.textFile(inputDir)
      .map(_.split(",").map(_.trim)) 
      .map(convertToZone)
      .reduce(_ closuringZone _)
  }
```
To verify the calculated region boundaries, a intermediate `main` function can print the result to the console or write it into a file.

```scala
  def main(args: Array[String]): Unit = {
    if (args.length >= 3) {
      val conf = new SparkConf().setAppName("Spark4Stars1-" + account)
      val sc = new SparkContext(conf)
      val result =
        if ("--tuple" == args(0))
          "What ?"
        else
          calculateMaxMin(args(1), sc).toString
      println(result)
    } else {
      println("Usage: spark-submit --class Spark4Stars1 /home/" + account + "/developped-assembly-1.0.jar " +
        "[ --caseClass | --tuple ] " +
        "hdfs:///user/" + account + "/data_repo")
    }
  }
```

 &ensp; &ensp; 
 &ensp; &ensp; 
  
## II. Set a grid and partition the region

Now that we have the boundaries coordinates of the celestial region, we are interested in partitioning the region into small rectangle cases. Considering that the whole file has a size about 5G, if we want to have outputs of 128M we'll need at least 40 cases.   
  
We would like to set a grid which has the same number of cuts on x-axis as on y-axis.
  
```scala
  case class Grid (nbMinCases: Int, outerBounds: Zone) {
    val nbCasesSqrt: Int =
      if (nbMinCases < 0 || nbMinCases > 26 * 26) {
        println("Try again!")
        1 
      } else
        ceil(sqrt(nbMinCases)).toInt 

    val gridRA: NumericRange[Double] = outerBounds.minRA to outerBounds.maxRA by
      (outerBounds.maxRA - outerBounds.minRA) / nbCasesSqrt

    val gridDecl: NumericRange[Double] = outerBounds.minDecl to outerBounds.maxDecl by
      (outerBounds.maxDecl - outerBounds.minDecl) / nbCasesSqrt
  }
```
Please notice that at this step, the class `Grid` is really a numeric grid with 2 arrays of cut coordinates, not a set of cases. 
   
We name the cases with two numbers resp. for x and y axes, according to their orders in the cuts, for example case "2.3" means it is the third on the x-axis and the fourth on the y axis (starting from 0).  The function `toWhichCaseIGo` decides for a star to which case it belongs

```scala
  def toWhichCaseIGo (ra: Double, decl: Double, grid: Grid): String = {
    var i:Int = 0
    var j:Int = 0

    while (ra > grid.gridRA(i)) {
      while (decl > grid.gridDecl(j))
        j += 1
      i += 1
    }

    i.toString + "." + j.toString
  }
```
  
We will also need a function reading in a raw record and extracting its `RA` and `Decl` values
```scala
  def keepRaDeclOnly (input: Array[String]): Array[Double] = {
    Array(input(PetaSkySchema.s_ra).toDouble, input(PetaSkySchema.s_decl).toDouble)
  }
```
and finally a function that sums this all up by doing MapReduce and returning one file per case as output

```scala
  def reformToPairRDD (inputDir: String, sc: SparkContext, grid: Grid)
    : RDD[(String, String)] =
    sc.textFile(inputDir)
      .map(_.split(",").map(_.trim)) // 2nd map transforms each line to a array of String
      .map(keepRaDeclOnly)
      .map(arr => (toWhichCaseIGo(arr(0),arr(1),grid), arr(0).toString + "," + arr(1).toString))
```
  
Please note that at output, each record becomes a Scala pair where the first element is the key, i.g. the name of the file to which it belongs.
  

We set a `main` function which sums this up by calling `reformToPairRDD` and calculate the size of the output files, displaying it in the console with the help of a histogram.

```scala
  def main(args: Array[String]): Unit = {

    if (args.length >= 3) {
      val conf = new SparkConf().setAppName("Spark4Stars2-" + account)
      val sc = new SparkContext(conf)

      val nbMinCases = args(0).toInt
      val inputDir = args(1)
      val outputDir = args(2)

      val maGrid = Grid(nbMinCases, calculateMaxMin(inputDir, sc))
      val RDDToWrite = reformToPairRDD(inputDir, sc, maGrid)

      writePairRDDToHadoopUsingKeyAsFileName(RDDToWrite, outputDir,
        maGrid.nbCasesSqrt * maGrid.nbCasesSqrt)

      RDDToWrite.countByKey().foreach {case (key, value) => println (key + "-->" +
        "*" * ceil(log10(value)).toInt)}
    }

    else {
      println("Usage: spark-submit --class Spark4Stars2 /home/" + account + "/developped-assembly-1.0.jar " +
        "nbMinCases " +
        "hdfs:///user/" + account + "/data_repo " + /* half useful for the moment */
        "hdfs:///user/" + account + "/result_repo")
    }
```

`002` :
```sh
6.3-->****
4.4-->****
3.7-->****
7.3-->****
4.5-->****
5.6-->****
3.4-->****
6.4-->****
0.0-->
2.7-->****
7.2-->*****
2.4-->****
3.5-->****
1.6-->****
5.7-->****
1.5-->****
4.6-->****
1.4-->****
2.6-->****
6.1-->****
7.1-->****
6.5-->
1.7-->****
2.5-->****
5.5-->****
3.6-->****
7.4-->****
6.2-->****
7.0-->
6.6-->***
4.7-->****
5.4-->***
```
`062` : 
```sh
1.2-->****
7.3-->***
0.0-->
7.2-->*****
1.6-->****
7.5-->*****
7.1-->****
1.7-->*****
1.3-->*
7.0-->
```
The number of stars in each output file is a logarithm of basis 10.
The output with no star sign means there is only one record in this output.

__Conclusion: The distribution of the sizes of the output is not very uniform. A load balancing is to be considered in the next part.__

 &ensp; &ensp; 
 &ensp; &ensp; 
  
## III. Better geometry and load balancing
In this part, we are going to ask 3 questions:  
  
1. How do we set up a true celestial coordinate system?
2. How do we define "close to a neighbouring zone" and how do we assign a star to plural cases in a RDD context?
3. How do we do the load balancing?

### 1. How do we set up a true celestial coordinate system?
We have to notice that once a point goes over 360° in RA, it begins at 0° again. 

```scala
  def convertToZone(input: Array[String]): Zone = {
    val ra = input(PetaSkySchema.s_ra).toDouble
    val decl = input(PetaSkySchema.s_decl).toDouble
    Zone(if (ra > 180) ra-360 else ra, if (ra > 180) ra-360 else ra, decl, decl)
  }
  
  def keepRaDeclOnly (input: Array[String]): Array[Double] = {
    val ra = input(PetaSkySchema.s_ra).toDouble
    Array(if (ra > 180) ra-360 else ra, input(PetaSkySchema.s_decl).toDouble)
  }

  def toWhichCasesIGo (ra: Double, decl: Double, grid: Grid):
  List[(String, String)] = {
    grid.Cases.filter(verifyBelonging(grid, _, ra, decl)).map(_.nom)
      .map((_, (if (ra < 0) ra+360 else ra).toString + "," + decl.toString)).toList
  }
```

### 2. How do we define "close to a neighbouring zone" and how do we assign a star to plural cases in a RDD context?
We define first a minimum distance for "being close to".
```scala
  val horizontalApprox = 0.1
  val verticalApprox = 0.1
```

If a star is located outside but closer to a case than the minimum distance, we should assign it to the case too. 
```scala
  def verifyBelonging (grid: Grid, casa: Case,
                            ra: Double, decl: Double): Boolean = {
    val assertion = ra >= casa.leftRA - horizontalApprox &&
      ra <= casa.rightRA + horizontalApprox &&
      decl >= casa.upperDecl - verticalApprox &&
      decl <= casa.lowerDecl + verticalApprox &&
      casa.counter < maxObsInOneCase

    if (assertion) {
      casa.counter += 1
      if (casa.counter == maxObsInOneCase)
        addCase(grid, casa.leftRA, casa.rightRA, casa.upperDecl, casa.lowerDecl,
          casa.nom + "second")
    }
    assertion
  }
``` 
  
The difficulty to address the close to another case problem, in terms of Spark, is that such a star can now have plural parental cases to which it belongs, therefore plural keys at output! A solution is to use the Spark function `flatMap`, which flattens an RDD of arrays of pairs to an RDD of pairs.
```scala
  def reformToPairRDD (inputDir: String, sc: SparkContext, grid: Grid)
  : RDD[(String, String)] =
    sc.textFile(inputDir)
      .map(_.split(",").map(_.trim))
      .map(keepRaDeclOnly)
      .flatMap(arr => toWhichCasesIGo(arr(0),arr(1), grid))
```


### 3. How do we do the load balancing?
First we consider a threshold of number of stars to each of the output case file
```scala
  val maxObsInOneCase = 10000
```
and assign to it a counter which counts the number of stars in it. Once the threshold being reached, we create another case of the same position and assign in the future stars in this zone to the new case.  
  
Before, in Part II, we didn't create a Scala class fore the cases and each time, we compared the coordinates of the star with the numerical grid. But here, the case is more important and a simple numeric grid wouldn't be enough. We create now a class for `Case`. 
```scala
  case class Case(leftRA: Double, rightRA: Double,
             upperDecl: Double, lowerDecl: Double, nom: String) {
    var counter: Int = 0
  }
```
Furthermore, the class `Grid` should also play a role of "manager of cases" which will also need a more complicated structure.
```scala
  case class Grid (nbMinCases: Int, outerBounds: Zone) {
    val nbCasesSqrt: Int =
      if (nbMinCases < 0 || nbMinCases > 26 * 26) {
        println("Try again!")
        1
      } else
        ceil(sqrt(nbMinCases)).toInt

    val caseLength = (outerBounds.maxRA - outerBounds.minRA) / nbCasesSqrt
    val caseHeight = (outerBounds.maxDecl - outerBounds.minDecl) / nbCasesSqrt

    var Cases = new ListBuffer[Case]

    for (i <- 0 until nbCasesSqrt; j <- 0 until nbCasesSqrt)
      Cases += Case(outerBounds.minRA + i * caseLength,
        outerBounds.minRA + (i+1) * caseLength,
        outerBounds.minDecl + i * caseHeight, outerBounds.minDecl + (i+1) * caseHeight,
        i.toString + "." + j.toString)
  }
```
It is to be noticed that a `ListBuffer` of cases is here necessary, which allows us to add new cases to it. 
```scala
  def addCase (grid: Grid, leftRA: Double, rightRA: Double,
                  upperDecl: Double, lowerDecl: Double, nom: String): Unit = {
    grid.Cases += Case(leftRA, rightRA, upperDecl, lowerDecl, nom)
  }
```
   
A histogram of output files shows that the distribution of stars are more balanced than before.

`002` : 
```scala
5.2-->****
6.3-->**
4.2-->****
4.4-->****
6.0-->**
2.3-->***
4.5-->****
5.6-->****
3.1-->****
3.4-->****
5.3-->****
6.4-->**
3.0-->****
4.1-->****
2.4-->***
3.5-->****
2.0-->***
4.6-->****
4.0-->****
3.3-->****
2.6-->***
6.1-->**
6.5-->**
2.1-->***
5.1-->****
3.2-->****
2.5-->***
5.5-->****
3.6-->****
6.2-->**
4.3-->****
5.0-->****
6.6-->**
2.2-->***
5.4-->****
```
`062` : 
```scala
0.1-->****
0.5second-->****
0.5-->****
0.3second-->****
0.0-->****
0.2second-->****
0.4-->****
0.4second-->****
0.3-->****
0.6second-->****
0.0second-->****
0.1second-->****
0.6-->****
0.2-->****
```
We can see that there are files with a "second" mark meaning that they were created in the middle of the processing. 

 &ensp; &ensp; 
 &ensp; &ensp; 

Below's the complete code for the work.  
    
Please note that in the source code, the file `Stark4Stars3.scala` is an old version of the solution of Part III and should be ignored.   
    
```scala
import java.lang.Math._

import TIW6RDDUtils.writePairRDDToHadoopUsingKeyAsFileName
import scala.collection.immutable.List
import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import schema.PetaSkySchema



object Spark4StarsFinal {
  val account = "XiansAccount"

  val horizontalApprox = 0.1
  val verticalApprox = 0.1
  val maxObsInOneCase = 10000

  case class Zone(minRA: Double, maxRA: Double,
             minDecl: Double, maxDecl: Double) {
    def closuringZone(b: Zone): Zone = {
      Zone(min(minRA, b.minRA), max(maxRA, b.maxRA),
        min(minDecl, b.minDecl), max(maxDecl, b.maxDecl))
    }

    def toString: String = {
      "minRA : %f, maxRA : %f, minDecl : %f, maxDecl : %f"
        .format(minRA, maxRA, minDecl, maxDecl)
    }
  }

  def convertToZone(input: Array[String]): Zone = {
    val ra = input(PetaSkySchema.s_ra).toDouble
    val decl = input(PetaSkySchema.s_decl).toDouble
    Zone(if (ra > 180) ra-360 else ra, if (ra > 180) ra-360 else ra, decl, decl)
  }

  def calculateMaxMin(inputDir: String, sc: SparkContext):
  Zone = {
    sc.textFile(inputDir)
      .map(_.split(",").map(_.trim))
      .map(convertToZone)
      .reduce(_ closuringZone _)
  }

  case class Case(leftRA: Double, rightRA: Double,
             upperDecl: Double, lowerDecl: Double, nom: String) {
    var counter: Int = 0
  }

  case class Grid (nbMinCases: Int, outerBounds: Zone) {
    val nbCasesSqrt: Int =
      if (nbMinCases < 0 || nbMinCases > 26 * 26) {
        println("Try again!")
        1
      } else
        ceil(sqrt(nbMinCases)).toInt

    val caseLength = (outerBounds.maxRA - outerBounds.minRA) / nbCasesSqrt
    val caseHeight = (outerBounds.maxDecl - outerBounds.minDecl) / nbCasesSqrt

    var Cases = new ListBuffer[Case]

    for (i <- 0 until nbCasesSqrt; j <- 0 until nbCasesSqrt)
      Cases += Case(outerBounds.minRA + i * caseLength,
        outerBounds.minRA + (i+1) * caseLength,
        outerBounds.minDecl + i * caseHeight, outerBounds.minDecl + (i+1) * caseHeight,
        i.toString + "." + j.toString)
  }

  def addCase (grid: Grid, leftRA: Double, rightRA: Double,
                  upperDecl: Double, lowerDecl: Double, nom: String): Unit = {
    grid.Cases += Case(leftRA, rightRA, upperDecl, lowerDecl, nom)
  }

  def verifyBelonging (grid: Grid, casa: Case,
                            ra: Double, decl: Double): Boolean = {
    val assertion = ra >= casa.leftRA - horizontalApprox &&
      ra <= casa.rightRA + horizontalApprox &&
      decl >= casa.upperDecl - verticalApprox &&
      decl <= casa.lowerDecl + verticalApprox &&
      casa.counter < maxObsInOneCase

    if (assertion) {
      casa.counter += 1
      if (casa.counter == maxObsInOneCase)
        addCase(grid, casa.leftRA, casa.rightRA, casa.upperDecl, casa.lowerDecl,
          casa.nom + "second")
    }
    assertion
  }

  def toWhichCasesIGo (ra: Double, decl: Double, grid: Grid):
  List[(String, String)] = {
    grid.Cases.filter(verifyBelonging(grid, _, ra, decl)).map(_.nom)
      .map((_, (if (ra < 0) ra+360 else ra).toString + "," + decl.toString)).toList
  }

  def keepRaDeclOnly (input: Array[String]): Array[Double] = {
    val ra = input(PetaSkySchema.s_ra).toDouble
    Array(if (ra > 180) ra-360 else ra, input(PetaSkySchema.s_decl).toDouble)
  }

  def reformToPairRDD (inputDir: String, sc: SparkContext, grid: Grid)
  : RDD[(String, String)] =
    sc.textFile(inputDir)
      .map(_.split(",").map(_.trim))
      .map(keepRaDeclOnly)
      .flatMap(arr => toWhichCasesIGo(arr(0),arr(1), grid))


  def main(args: Array[String]): Unit = {

    if (args.length >= 3) {
      val conf = new SparkConf().setAppName("Stark4Stars3-" + account)
      val sc = new SparkContext(conf)

      val nbFilesMin = args(0).toInt
      val inputDir = args(1)
      val outputDir = args(2)

      val maGrid = Grid(nbFilesMin, calculateMaxMin(inputDir, sc))
      val RDDToWrite = reformToPairRDD(inputDir, sc, maGrid)

      writePairRDDToHadoopUsingKeyAsFileName(RDDToWrite, outputDir,
        nbFilesMin)

      RDDToWrite.countByKey().foreach {case (key, value) => println (key + "-->" +
        "*" * ceil(log10(value)).toInt)}
    }



    else {
      println("Usage: spark-submit --class Spark4Stars2 /home/" +
        account + "/developped-assembly-1.0.jar " +
        "40 " +
        "hdfs:///user/" + account + "/data_repo " +
        "hdfs:///user/" + account + "/result_repo")
    }
  }
}
```





