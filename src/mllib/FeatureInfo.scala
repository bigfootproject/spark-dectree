package machinelearning

import collection.immutable.TreeMap

/**
 * This class is representative for features
 * We have two feature categories : Numerical and Categorical
 * which are presented by NumericalFeature and CategoricalFeature, respectively
 * @Name: name of feature
 * @Type: type of feature: "d" is discrete feature( categorical feature); "c" is continuous feature (numerical feature)
 * @index: index of this feature in the whole data set, based zero
 */

// xName: Name of feature, such as temperature, weather...
// xType: type of feature: 0 (Continuous) or 1 (Category)
abstract class FeatureInfo(val Name: String, val Type: String, val index: Int) extends Serializable {

    override def toString() = "Index:" + index + " | Name: " + Name + " | Type: " + Type;

}

object FeatureInfo extends Serializable {
    //lazy val numericalTag : String = "c"
    //lazy val categoricalTag : String = "d"

    def apply(Name: String, Type: String, idx: Int) = {
        var nType = Type.trim
        var nName = normalizeString(Name)
        nType match {
            case "c" => new NumericalFeature(nName, nType, idx) // continuous feature
            case "d" => new CategoricalFeature(nName, nType, idx) // discrete feature
        }
    }

    private def normalizeString(s: String) = {
        var s1 = s.trim
        val len = s1.length
        if (len <= 2 || s1 == "\"\"") s1
        else if (s1(0) == '\"' && s1(len - 1) == '\"')
            s1.substring(1, len - 1)
        else s1
    }      
}

/**
 * This class is representative for numerical feature
 */
case class NumericalFeature(fName: String, fType: String, fIndex: Int) extends FeatureInfo(fName, fType, fIndex) {

}

/**
 * This class is representative for categorical feature
 */
case class CategoricalFeature(fName: String, fType: String, fIndex: Int) extends FeatureInfo(fName, fType, fIndex) {

 
}
