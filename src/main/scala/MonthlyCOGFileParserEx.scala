import java.security.MessageDigest

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.math

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

import org.apache.spark.storage


class Location(var lat:Double=0.0, var lon:Double=0.0) extends Serializable{
	def distance(l1:Location):Double={
		var R = 6371; // Radius of the earth in km
  		var dLat = (lat-l1.lat)*math.Pi/180  
  		var dLon = (lon-l1.lon)*math.Pi/180 
  		var a = 
    			math.sin(dLat/2) * math.sin(dLat/2) +
    			math.cos((lat)*math.Pi/180) * math.cos((l1.lat)*math.Pi/180) * 
    			math.sin(dLon/2) * math.sin(dLon/2)
    		 
  		var c = 2 * math.atan2(Math.sqrt(a), math.sqrt(1-a))
  		var d = R * c; // Distance in km
  		return d
	}

}


class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Location])
    kryo.register(classOf[MonthlyCOG])
    kryo.register(classOf[DSV])
  }
}

class MonthlyCOG extends Serializable{
	
def initDistrictsProvinceMap(sc:SparkContext,fileName:String,districtIndex:Int,provinceIndex:Int, delimiter:String=",")={

        var dpFile=sc.textFile(fileName).map(line=>(new DSV(line,",")))
        var dpFiltered=dpFile.filter(d=>((d.parts(0).contains("ID")==false)&&(d.parts(provinceIndex)!="")))        
        dpFiltered.map(p=>(p.parts(districtIndex),p.parts(provinceIndex))).collectAsMap()
        
}

def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _ => None }
def initDistrictsLocMap( sc:SparkContext,fileName:String, districtIndex:Int,  latIndex:Int,  lngIndex:Int,delimiter:String=",")={
        var dlFile=sc.textFile(fileName).map(line=>(new DSV(line,",")))
        
        dlFile.filter(p=>((p.parts(0).contains("ID")==false)&&(parseDouble(p.parts(latIndex))!=None)&&(parseDouble(p.parts(lngIndex))!=None))).map(p=>(p.parts(districtIndex),(p.parts(latIndex).toDouble,p.parts(lngIndex).toDouble))).collect()
        
}

val findMin=(a:(String, (Double,  String)), b:(String, (Double, String)))=>{
    var res:(String, (Double,  String))=null
    if((a._2._1 <= b._2._1))
        res=a
    else
        res=b
        
    println("Compared "+a._2._2+" to "+b._2._2)
    res
}


def assignDistrictProvince(sc:SparkContext,lat: Double, lng:Double, distProvinceMap:scala.collection.Map[String,String], distLocMap:Array[(String, (Double, Double))])={
    
	//     var t1=distLocMap.map{case(k,v)=>("current",(math.abs(v._1-lat),math.abs(v._2-lng),k))}
	var t1=distLocMap.map{case(k,v)=>("current",((new Location(v._1,v._2)).distance(new Location(lat,lng)),k))}     
     
     var t2=t1.reduceLeft(findMin)
     
     var dist=t2._2._2
     var prov=distProvinceMap.get(t2._2._2)
     var province="";	
     prov match{
	case None => province="None"
	case Some(x) =>province=x
     }
	
      println("District is "+dist+" Province is "+province)
     (dist,province)
     
}
	def cog2(s:Iterable[(String,String)])={
     		//s.foreach(println(._1+._2))
		 s.foreach(a=>println(a._1+" "+a._2))

		var total=0
		var x=0.0000000000000
		var y=0.0000000000000
		var z=0.0000000000000

		for(point<-s){
			var lat=point._1.toDouble*math.Pi/180
			var lon=point._2.toDouble*math.Pi/180
			var x1 = math.cos(lat) * math.cos(lon);
		        var y1 = math.cos(lat) * math.sin(lon);
        		var z1 = math.sin(lat);

			x+=x1
			y+=y1
			z+=z1
			total=total+1
		}

		x=x/total
		y=y/total
		z=z/total
	
		var Lon = math.atan2(y, x)
    		var Hyp = math.sqrt(x * x + y * y)
    		var Lat = math.atan2(z, Hyp)
	
		(Lat * 180 / math.Pi, Lon * 180 / math.Pi)

     	}
}
object MainMonthlyCOG extends Serializable{


                 val conf = new SparkConf().setMaster("yarn-client")
                        //setMaster("spark://messi.ischool.uw.edu:7077")
                .setAppName("MonthlyCOG1")
                .set("spark.storage.blockManagerHeartBeatMs", "300000")
                .set("spark.default.parallelism","40")
                //.set("spark.shuffle.consolidateFiles", "true")
                //.set("spark.kryo.registrator", "MyRegistrator")
                //.set("spark.akka.frameSize","120")
                // .set("spark.executor.memory", "20g")
                //.set("spark.kryoserializer.buffer.max.mb","40024")
                //.set("spark.kryoserializer.buffer.mb","2024")
                
		//.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                //val conf = new SparkConf().setMaster("yarn-client")
                //setMaster("spark://messi.ischool.uw.edu:7077")
                //.setAppName("MonthlyCOGAgg")
                //.set("spark.shuffle.consolidateFiles", "true")

                val sc = new SparkContext(conf)
	

	def main(args:Array[String]){

		val inputPath="hdfs:///user/mraza/Rwanda_In/"
		val outputPath = "hdfs:///user/mraza/Rwanda_Out/"
		
		val inputFileName="MobilityFiles/"+args(0)
		//val inputFileName="0501-decrypted.txt"
		val outputFileName=inputFileName+"out"


var mc = new MonthlyCOG()

var distProvinceMap=mc.initDistrictsProvinceMap(sc,inputPath+"Districts.csv",1,2)

var distLocMap=mc.initDistrictsLocMap(sc,inputPath+"Towers2.csv",4,3,2)

		var latIndex=6
		var lngIndex=7
	
/* Input File Format
L24356882,060314,6pm-8am,095,None,None,-1.7974202800000008,30.4099454,1
L08749652,060319,6pm-8am,010,None,None,-1.6758302900000002,29.22216666,1
L60858692,060317,6pm-8am,002,None,None,0.0,0.0,1
L89355411,060315,6pm-8am,094,None,None,0.0,0.0,1

 */

var dsvData = sc.textFile(inputPath+inputFileName,10).map(line=>(new DSV(line,","))).filter(d=>(
                        d.parts(latIndex)!="NaN" &&d.parts(lngIndex)!="NaN"))
                

var gravity=dsvData.map(d=>(d.parts(0),(d.parts(latIndex),d.parts(lngIndex)))).groupByKey().map{case (k,v)=>(k,mc.cog2(v))}
      
    var gravityLoc=gravity.map{case(k,v)=>(k,(mc.assignDistrictProvince(sc,v._1,v._2,distProvinceMap,distLocMap)))}
		
		var result=gravityLoc.join(gravity)	
		//Subscriber, District, Province, Lat, Long
		var flatResult=result.map{case(k,v)=>(k,v._1._1,v._1._2,v._2._1,v._2._2)}

		flatResult.saveAsTextFile(outputPath+outputFileName)


	} 
}
