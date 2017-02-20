object Sort
{
   def main(args: Array[String]) {

	val startTime = System.nanoTime()
	
	val inputfile=sc.textFile("hdfs:////root/ephemeral-hdfs/bin/inputfile.txt")

	val sortedFile = mapping(inputfile)
	
	val sortObj = sortingByKey(sortedFile)
	
	val outputLines = mappedKeys(sortObj)
	
	outputLines.saveAsTextFile("/root/out1")

	val endTime = System.nanoTime()

	println("Time taken for sorting: " + (endTime - startTime)/1000000000 + "	seconds")

	}
	
	def mapping(String filename) {
		return filename.map(line => (line.take(10),line.drop(10)))
	}
	
	def sortingByKey(String fName)
	{
		return sorted.sortByKey()
	}
	def mappedKeys(String ObjName)
	{
		return ObjName.map{case (k,v) => s"$k $v" }
	}

}

