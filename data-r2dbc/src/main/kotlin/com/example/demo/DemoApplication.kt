package com.example.demo


import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.spi.ConnectionFactory
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.data.r2dbc.core.await
import org.springframework.data.r2dbc.core.awaitOne
import org.springframework.data.r2dbc.core.from
import org.springframework.data.r2dbc.core.into
import org.springframework.data.r2dbc.query.Criteria
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.server.ServerWebExchange
import java.time.Duration


@SpringBootApplication
class DemoApplication {

    @Bean
    fun connectionFactory(): ConnectionFactory {
        return PostgresqlConnectionFactory(
            PostgresqlConnectionConfiguration.builder()
                .host("localhost")
                .port(5432)
                .database("meetings")
                .username("yuranos")
                .password("")
                .connectTimeout(Duration.ofSeconds(10))
                .build()
        )
    }

    @Bean
    fun databaseClient(conFac: ConnectionFactory) = DatabaseClient.create(conFac)
}


fun main(args: Array<String>) {
    runApplication<DemoApplication>(*args)
}


@RestController
@RequestMapping("/meetings")
class MeetingsController(
    private val postRepository: PostRepository,
    private val databaseClient: DatabaseClient
) {

    @GetMapping("{id}")
    suspend fun findOne(@PathVariable id: Long): Meeting? =
        postRepository.find(id) ?: throw PostNotFoundException(id)

    @PostMapping
    suspend fun save() =
        postRepository.save(Meeting(null, 5, 5))
}


class PostNotFoundException(postId: Long) : RuntimeException("Post:$postId is not found...")

@RestControllerAdvice
class RestWebExceptionHandler {

    @ExceptionHandler(PostNotFoundException::class)
    suspend fun handle(ex: PostNotFoundException, exchange: ServerWebExchange) {

        exchange.response.statusCode = HttpStatus.NOT_FOUND
        exchange.response.setComplete().awaitFirstOrNull()
    }
}

@Component
class PostRepository(private val client: DatabaseClient) {

    suspend fun find(id: Long) =
        client.select().from<Meeting>()
            .matching(Criteria.where("id").`is`(id)).fetch().awaitOne()
    //client.execute("SELECT * FROM meetings;")
    //    .asType<Meeting>().fetch().awaitOne()

    suspend fun save(meeting: Meeting) =
        client.insert().into<Meeting>().table("meetings").using(meeting).await()
}

@Table("meetings")
data class Meeting(val id: Long? = null, @Column("location_id") val locationId: Long, @Column("timeslot_id") val timeSlotId: Long)

