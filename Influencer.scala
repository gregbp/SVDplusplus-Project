class Influencer () {

  private var _entity : String=""
  private var _id: org.apache.spark.graphx.VertexId = 0


  // Getters
  def entity = _entity
  def id = _id


  // Setters
  def entity_= (value:String):Unit = _entity = value
  def id_= (value:org.apache.spark.graphx.VertexId):Unit = _id = value


  def findInfluencer(): Unit ={



  }

  def add(x: Int, y: Int): Int ={
    var sum=x+y
    return sum

  }



}
