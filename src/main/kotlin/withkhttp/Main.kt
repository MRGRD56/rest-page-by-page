@file:OptIn(ExperimentalCoroutinesApi::class)

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import khttp.responses.Response
import kotlinx.coroutines.*
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

data class MockPage<T>(
    val items: List<T>,
    val pageNumber: Int,
    val totalPages: Int,
    val totalItems: Int,
    val isLastPage: Boolean
)

data class MockItem(
    val id: UUID,
    val name: String,
    val description: String,
    val createdAt: Instant,
    val price: Double
)

val objectMapper = jacksonObjectMapper().apply {
    registerModule(JavaTimeModule())
}

inline fun <reified T> Response.convert() = objectMapper.readValue<T>(raw)

fun fetchPage(page: Int): MockPage<MockItem> {
    val response: Response = khttp.get("https://api.mrgrd56.ru/mock/ro",
        params = mapOf("page" to page.toString()),
        stream = true
    )

    return response.convert<MockPage<MockItem>>()
}

suspend fun main() = coroutineScope {
    val items = ConcurrentLinkedQueue<MockItem>()

    val (firstPageItems, _, totalPages, totalItems) = fetchPage(0)
    println("Fetched 1st page, totalPages=$totalPages")

    items.addAll(firstPageItems)

    if (totalPages > 1) {
        val dispatcher = Dispatchers.IO.limitedParallelism(30)

        (1..<totalPages).map { pageNumber ->
            launch(dispatcher) {
                fetchPage(pageNumber).let {
                    items.addAll(it.items)
                }
            }
        }.joinAll()
    }

    println("Fetched ${items.size} items, expected $totalItems")
}
