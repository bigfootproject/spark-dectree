package bigfoot.helpers

class ArgumentParser (name : String = ""){
	type ArgNames = String
	type OptionMap = Map[String, Any]

	private var mapNameToValue =  Map[String, String]()
	private var mapNameToHelp = Map[String, String]()
	//private var mapNameArgumentToValue = Map[(String, String) , Any]()
	private var mapIndexToNameOfArgument = Map[Int, String]()
	private var mapTermToFullName = Map[String, String]()
	private var numArgument : Int = 0
	
	private var usageOptions = ""
	private var usageArgs = ""
	def usage : String = {
	    var help = "%s %s %s".format(name , usageOptions, usageArgs)
	    var listDescriptions = mapNameToHelp.toList.sortBy(x => x._1)
	    var h2 : String = ""
	    listDescriptions.foreach{ case (name, des) => h2 = "%s\n\t%s : %s".format(h2, name.padTo(20,' '), des) }  
	    "USAGE:\n%s%s".format(help, h2)
	}
	
	def addOption(term : String, fullName : String, help : String="", defaultValue : String = "0") {
	    mapTermToFullName = mapTermToFullName + (term -> fullName)
        mapNameToValue = mapNameToValue + (fullName -> defaultValue)
        mapNameToHelp = mapNameToHelp + (fullName -> help)
        usageOptions = "%s %s".format(usageOptions, "[%s %s]".format(fullName, fullName))
	}
	
	def addArgument(name : String, help : String ="", defaultValue : String = "") {
	    mapNameToValue = mapNameToValue + (name-> defaultValue)
	    mapNameToHelp = mapNameToHelp + (name -> help)
	    usageArgs = "%s %s".format(usageArgs, name)
	    mapIndexToNameOfArgument = mapIndexToNameOfArgument + (numArgument -> name)
	    numArgument = numArgument + 1
	}
	
	def get(name : String) : String = {
	    mapNameToValue(name)
	}
	
	def parse(args : Array[String]) : Map[ArgNames, Any] = {
        if (args.length == 0 && numArgument > 0)  println(usage)
        val arglist = args.toList
        var currentArgIndex = 0

        def nextOption(list: List[String]): OptionMap = {
            def isSwitch(s: String) = (s(0) == '-')
            list match {
                case Nil => mapNameToValue
                case string :: nextString :: tail => {
                    if (isSwitch(string)) {
                        if (string == "-h")
                        {
                            println(usage)
                            exit
                        }
                        val name = mapTermToFullName(string)
                        mapNameToValue = (
                            if (mapNameToValue.contains(name))
                            	mapNameToValue.updated(name, nextString)
                            else mapNameToValue)
                       nextOption(tail)
                    } else{
                        val name = mapIndexToNameOfArgument(currentArgIndex)
                        currentArgIndex = currentArgIndex + 1
                        mapNameToValue = (
                            if (mapNameToValue.contains(name))
                            	mapNameToValue.updated(name, string)
                            else mapNameToValue)
                        nextOption(nextString :: tail)
                    }
                }
                case string :: Nil => {
                    val name = mapIndexToNameOfArgument(currentArgIndex)
                        currentArgIndex = currentArgIndex + 1
                        mapNameToValue = (
                            if (mapNameToValue.contains(name))
                            	mapNameToValue.updated(name, string)
                            else mapNameToValue)
                        nextOption(Nil)
                }
                case option :: tail =>
                    println("Unknown option " + option)
                    exit(1)
            }
        }
        
        nextOption(arglist)
	    
	}
}