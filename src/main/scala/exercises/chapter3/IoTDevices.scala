package exercises.chapter3

import org.apache.spark.sql.{SparkSession, functions => F}

case class DeviceIoTData(
                          battery_level: Long,
                          c02_level: Long,
                          cca2: String,
                          cca3: String,
                          cn: String,
                          device_id: Long,
                          device_name: String,
                          humidity: Long,
                          ip: String,
                          latitude: Double,
                          lcd: String,
                          longitude: Double,
                          scale: String,
                          temp: Long,
                          timestamp: Long
                        )

case class DeviceTempByCountry(
                                temp: Long,
                                device_name: String,
                                device_id: Long,
                                cca3: String
                              )

object IoTDevices {

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    val path = "data/chapter3/iot_devices.json"

    val ds = spark.read
      .json(path)
      .as[DeviceIoTData]

    println("=== IoT Devices Data ===")
    ds.show(5, truncate = false)

    println("\n=== Devices with temp > 30 and humidity > 70 ===")
    ds.filter(d => d.temp > 30 && d.humidity > 70)
      .show(5, truncate = false)

    println("\n=== Device Temp by Country (temp > 25) ===")
    val dsTemp = ds
      .filter(_.temp > 25)
      .map(d => DeviceTempByCountry(d.temp, d.device_name, d.device_id, d.cca3))

    dsTemp.show(5, truncate = false)

    println("\n=== First Device ===")
    println(dsTemp.first())

    println("\n=== Alternative query with select ===")
    ds
      .select($"temp", $"device_name", $"device_id", $"cca3")
      .where($"temp" > 25)
      .as[DeviceTempByCountry]
      .show(5, truncate = false)
  }
}
