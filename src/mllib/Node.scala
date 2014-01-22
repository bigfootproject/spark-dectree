package mllib

trait Node extends Serializable {
    def condition: Any
    var feature: FeatureInfo
    def value : Any
    def left: Node
    def right: Node
    def isEmpty: Boolean
    def toStringWithLevel(level: Int): String
    override def toString: String = "\n" + toStringWithLevel(1)
}

/**
 * A left node
 */
class Empty(xValue: String = "Empty") extends Node {
    def this() = this("Empty")
    def isEmpty = true
    def value = xValue
    def condition: Nothing = throw new NoSuchElementException("empty.condition")
    def left: Nothing = throw new NoSuchElementException("empty.left")
    def right: Nothing = throw new NoSuchElementException("empty.right")
    var feature: FeatureInfo = _ //FeatureInfo("Empty", "d", 0)
    def toStringWithLevel(level: Int) = xValue
}

/**
 * An internal node
 */
class NonEmpty(xFeature: FeatureInfo, xCondition: Any, xLeft: Node, xRight: Node) extends Node{
    def value = xFeature.Name
    def isEmpty = false
    def condition = xCondition match { case s: Set[String] => s; case d: Double => d }
    def left = xLeft
    def right = xRight
    var feature: FeatureInfo = xFeature
    val (conditionLeft, conditionRight) = xCondition match {
        case s: Set[String] => (s.toString, "Not in " + s.toString )
        case d : Double => ("%s < %f".format(xFeature.Name, d), "%s >= %f".format(xFeature.Name, d))
    }

    def toStringWithLevel(level: Int) =
        feature.Name + "(%s)\n".format(condition match {
            case s : Set[String] => setToString(s)
            case d : Double => " < %f".format(d)
        	}) +
        	("".padTo(level, "|")).mkString("    ") + "-(yes)" + ("".padTo(level, "-")).mkString("") + left.toStringWithLevel(level + 1) + "\n" +
            ("".padTo(level, "|")).mkString("    ") + "-(no)" + ("".padTo(level, "-")).mkString("") + right.toStringWithLevel(level + 1)
            
   private def setToString(s : Set[String]) : String = {
        val s1 = s.toString
        val len = s1.length
        "{%s}".format(s1.substring(4, len-1))
    }
}