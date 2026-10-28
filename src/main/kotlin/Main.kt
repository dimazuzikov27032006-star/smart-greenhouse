import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.flow.*
data class SensorReading(
    val sensorId: String,
    val value: Double,
    val timestamp: Long = System.currentTimeMillis()
)
suspend fun emitSensorData(channel: SendChannel<SensorReading>) {
    val sensorId = when (channel) {
        else -> "SENSOR"
    }
    for (i in 1..10) {
        delay(500)
        val reading = SensorReading(
            sensorId = sensorId,
            value = kotlin.random.Random.nextDouble(10.0, 35.0)
        )
        channel.send(reading)
    }
}
suspend fun emitSensorData(sensorId: String, channel: SendChannel<SensorReading>) {
    for (i in 1..10) {
        delay(500)
        val reading = SensorReading(
            sensorId = sensorId,
            value = kotlin.random.Random.nextDouble(10.0, 35.0)
        )
        channel.send(reading)
    }
}
fun sensorFlow(sensorId: String): Flow<SensorReading> = flow {
    for (i in 1..10) {
        delay(300)
        val reading = SensorReading(
            sensorId = sensorId,
            value = kotlin.random.Random.nextDouble(10.0, 35.0)
        )
        emit(reading)
    }
}
fun main() = runBlocking {
    println("=== КОРУТИНЫ + КАНАЛ ===")
    val channel = Channel<SensorReading>()
    val tempJob = launch { emitSensorData("TEMP", channel) }
    val humJob = launch { emitSensorData("HUM", channel) }
    val co2Job = launch { emitSensorData("CO2", channel) }
    var count = 0
    for (msg in channel) {
        println("[Канал] Получено: Датчик=${msg.sensorId}, Значение=${"%.2f".format(msg.value)}")
        count++
        if (count == 30) break
    }
    tempJob.join()
    humJob.join()
    co2Job.join()
    channel.close()
    println("\n=== FLOW: ЛЕНИВЫЙ ПОТОК ===")
    val processedFlow = sensorFlow("TEMP")
        .filter { it.value > 20.0 }
        .map { it.copy(value = it.value * 1.1) }
        .take(5)
        .onEach { println("[Flow] Обработано: $it") }
    println("(До toList: тишина)")
    val resultList = processedFlow.toList()
    println("Собрано элементов через toList: ${resultList.size}")
    val avg = processedFlow.fold(0.0 to 0) { acc, reading ->
        (acc.first + reading.value) to (acc.second + 1)
    }.let { (sum, count) ->
        if (count > 0) sum / count else 0.0
    }
    println("Среднее значение (через fold): ${"%.2f".format(avg)}")
    println("\n=== ВЫВОД ===")
    println("Каналы: императивный подход, ручное управление, данные идут по мере поступления.")
    println("Flow: декларативный, ленивый, цепочка операций выполняется как конвейер.")
}