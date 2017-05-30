import org.apache.hadoop.hive.ql.exec.UDF
import scala.math._

/**
  * Created by Administrator on 2017-05-30.
  */
class CoordinatesConvert extends UDF {

  def evaluate(xy: String, lat: String, lon: String) = {
    val pi = 3.1415926535 //원주율
    val a = 6378137.0 //장반경
    val f = 0.003352811 //편평률
    val b = 6356752.314 //단반경
    val e_1 = 0.00669438 //제1 이심률
    val e_2 = 0.006739497 //제2 이심률
    val k0 = 0.9996 //축적계수
    val x_plus = 2000000.0 //X축 가산값
    val y_plus = 1000000.0 //Y축 가산값
    val ori_lat = 38.0 //원점 위도
    val ori_lon = 127.5 //원점 경도
    val nEmpty = 4207498.0192

    val gd_wgs_x = lat.toDouble
    val gd_wgs_y = lon.toDouble

    val TT = pow(tan(gd_wgs_x / 180 * pi), 2)
    val CC = e_2 * pow(cos(gd_wgs_y * 180 * pi), 2)
    val AA = ((gd_wgs_y / 180 * pi) - (ori_lon / 180 * pi)) * cos(gd_wgs_x / 180 * pi)
    val NN = a / sqrt(1 - e_1 * pow(sin(gd_wgs_x / 180 * pi), 2))
    val MM = a * ((1 - e_1 / 4 - 3 * pow(e_1, 2) / 64 - 5 * pow(e_1, 3) / 256) * (gd_wgs_x / 180 * pi) + (-3 * e_1 / 8 + -3 * pow(e_1, 2) / 32 + -45 * pow(e_1, 3) / 1024) * sin(2 * gd_wgs_x / 180 * pi) + (15 * pow(e_1, 2) / 256 + 45 * pow(e_1, 3) / 1024) * sin(4 * gd_wgs_x / 180 * pi) - 35 * pow(e_1, 3) / 3072 * sin(6 * gd_wgs_x / 180 * pi))
    val XX = x_plus + k0 * (MM - nEmpty + NN * tan(gd_wgs_x / 180 * pi) * (pow(AA, 2) / 2 + pow(AA, 4) * (5 - TT + 9 * CC + 4 * pow(CC, 2)) / 24 + pow(AA, 6) * (61 - 58 * TT + pow(TT, 2) + 600 * CC - 330 * e_2)))
    val YY = y_plus + (NN * (AA + pow(AA, 3) * (1 - TT + CC) / 6 + pow(AA, 5) * (5 - 18 * TT + pow(TT, 2) + 72 * CC - 58 * e_2) / 120)) * k0
    // X  <-->  Y  값 교환
    val gd_utmk_x = YY
    val gd_utmk_y = XX
    if (xy == "x") gd_utmk_x
    else
      gd_utmk_y
  }

}
