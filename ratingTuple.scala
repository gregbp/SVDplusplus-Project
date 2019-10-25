class ratingTuple (){

  private var _u : org.apache.spark.graphx.VertexId= 0
  private var _i: org.apache.spark.graphx.VertexId = 0
  private var _r: Double = 0.0



  // Getters
  def u = _u
  def i = _i
  def r = _r




  // Setters
  def u_= (value:org.apache.spark.graphx.VertexId):Unit = _u = value
  def i_= (value:org.apache.spark.graphx.VertexId):Unit = _i = value
  def r_= (value:Double):Unit = _r = value






}
