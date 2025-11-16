@file:Suppress("WildcardImport", "NoWildcardImports", "MagicNumber")

package soa

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.integration.annotation.Gateway
import org.springframework.integration.annotation.MessagingGateway
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.integration.channel.QueueChannel
import org.springframework.integration.config.EnableIntegration
import org.springframework.integration.config.EnableMessageHistory
import org.springframework.integration.dsl.IntegrationFlow
import org.springframework.integration.dsl.MessageChannels
import org.springframework.integration.dsl.Pollers
import org.springframework.integration.dsl.PublishSubscribeChannelSpec
import org.springframework.integration.dsl.integrationFlow
import org.springframework.integration.handler.advice.RequestHandlerRetryAdvice
import org.springframework.integration.support.MessageBuilder
import org.springframework.integration.transformer.support.HeaderValueMessageProcessor
import org.springframework.messaging.Message
import org.springframework.messaging.MessageChannel
import org.springframework.retry.backoff.ExponentialBackOffPolicy
import org.springframework.retry.policy.SimpleRetryPolicy
import org.springframework.retry.support.RetryTemplate
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

private val logger = LoggerFactory.getLogger("soa.CronOddEvenDemo")

/**
 * Messaging Gateway for sending numbers into the integration flow.
 * This provides a simple interface to inject messages into the system.
 * Note: Check which channel this gateway sends messages to.
 */
@MessagingGateway
interface SendNumber {
    @Gateway(requestChannel = "numberChannel")
    fun sendNumber(number: Int)
}

/**
 * Service component that processes messages from the odd channel.
 * Uses @ServiceActivator annotation to connect to the integration flow.
 */
@Component
class SomeService {
    @ServiceActivator(inputChannel = "oddChannel")
    fun handle(p: Any) {
        logger.info("  🔧 Service Activator: Received [{}] (type: {})", p, p.javaClass.simpleName)
    }
}

/**
 * Header Processor that implements the logic to generate a custom
 * enrich header
 */
class ParityHeaderProcessor : HeaderValueMessageProcessor<String> {
    /**
     * The enrich header determines whether a number is even or odd
     */
    override fun processMessage(message: Message<*>): String {
        val n = message.payload as Int
        return if (n % 2 == 0) "even" else "odd"
    }

    /**
     * Allow overwriting headers if needed
     */
    override fun isOverwrite(): Boolean = true
}

/**
 * Spring Integration configuration for demonstrating Enterprise Integration Patterns.
 * This application implements a message flow that processes numbers and routes them
 * based on whether they are even or odd.
 *
 * **Your Task**: Analyze this configuration, create an EIP diagram, and compare it
 * with the target diagram to identify and fix any issues.
 */
@SpringBootApplication
@EnableIntegration
@EnableMessageHistory
@EnableScheduling
class IntegrationApplication(
    private val sendNumber: SendNumber,
) {
    /**
     * Creates an atomic integer source that generates sequential numbers.
     */
    @Bean
    fun integerSource(): AtomicInteger = AtomicInteger()

    /**
     * Main integration flow that polls the integer source and routes messages.
     * Polls every 100ms and routes based on even/odd logic.
     */
    @Bean
    fun sourceFlow(integerSource: AtomicInteger): IntegrationFlow =
        integrationFlow(
            source = { integerSource.getAndIncrement() },
            options = { poller(Pollers.fixedRate(100)) },
        ) {
            enrichHeaders {
                header("where", "Source Flow")
            }
            transform { num: Int ->
                logger.info("📥 Source generated number: {}", num)
                num
            }
            wireTap("tapChannel")
            channel("numberChannel")
        }

    /**
     * Scheduled task that sends every 1000ms negative random numbers via the gateway.
     */
    @Scheduled(fixedRate = 1000)
    fun sendNumber() {
        val number = -Random.nextInt(100)
        logger.info("🚀 Gateway injecting: {}", number)
        sendNumber.sendNumber(number)
    }

    /**
     * Main integration flow that polls the integer source and routes messages.
     * Polls every 100ms and routes based on even/odd logic.
     */
    @Bean
    fun numberFlow(integerSource: AtomicInteger): IntegrationFlow =
        integrationFlow("numberChannel") {
            enrichHeaders {
                header("parity", ParityHeaderProcessor())
                header("where", "Number Flow", true)
            }
            wireTap("tapChannel")
            route { p: Int ->
                val channel = if (p % 2 == 0) "evenChannel" else "oddChannel"
                logger.info("  🔀 Router: {} → {}", p, channel)
                channel
            }
        }

    /**
     * Integration flow for processing odd numbers.
     * Transforms integers to strings and logs the result.
     */
    @Bean
    fun evenFlow(): IntegrationFlow =
        integrationFlow("evenChannel") {
            enrichHeaders {
                header("where", "Even Flow", true)
            }
            wireTap("tapChannel")
            transform { obj: Int ->
                logger.info("  ⚙️  Even Transformer: {} → 'Number {}'", obj, obj)
                "Number $obj"
            }
            handle { p ->
                logger.info("  ✅ Even Handler: Processed [{}]", p.payload)
                logger.info("  🏷️  Even Handler: Enriched Headers [{}]", p.headers["parity"])
            }
        }

    /**
     * Defines a publish-subscribe channel for odd numbers.
     * Multiple subscribers can receive messages from this channel.
     */
    @Bean
    fun oddChannel(): PublishSubscribeChannelSpec<*> = MessageChannels.publishSubscribe()

    private val retryMap = ConcurrentHashMap<Int, AtomicInteger>()

    /**
     * Integration flow for processing even numbers.
     * Transforms integers to strings and logs the result.
     */
    @Bean
    fun oddFlow(retryAdvice: RequestHandlerRetryAdvice): IntegrationFlow =
        integrationFlow("oddChannel") {
            enrichHeaders {
                header("where", "Odd Flow", true)
                header("errorChannel", "errorChannel", true)
            }
            wireTap("tapChannel")
            transform { obj: Int ->
                logger.info("  ⚙️  Odd Transformer: {} → 'Number {}'", obj, obj)
                "Number $obj"
            }
            handle({ p ->
                // Simulate handling error
                val n = (p.payload as String).substringAfter("Number ").trim().toInt()
                val attempts = retryMap.computeIfAbsent(n) { AtomicInteger(0) }
                val randFailures = (0..3).random()
                if (attempts.getAndIncrement() < randFailures) {
                    logger.error("  ❌ Odd Handler: Failing attempt ${attempts.get()} for message [${p.payload}]")
                    throw RuntimeException("Simulated error")
                }

                logger.info("  ✅ Odd Handler: Processed [{}]", p.payload)
                logger.info("  🏷️  Odd Handler: Enriched Headers [{}]", p.headers["parity"])

                retryMap.remove(n)
            }) {
                advice(retryAdvice)
            }
        }

    // ---------------------------------------------------------------------------

    /**
     * Integration flow for Wire Tap.
     * Logs the history of components a message has gone through
     */
    @Bean
    fun tapFlow() =
        integrationFlow("tapChannel") {
            handle { p ->
                logHistory(p)
            }
        }

    /**
     * Logs the history of components a message has gone through
     */
    private fun logHistory(msg: Message<*>) {
        val where = msg.headers["where"] as String
        val history = msg.headers["history"] as List<*>
        var lastComponent = ""

        logger.info("  📜 $where: History {")
        history.forEach {
            val component =
                it
                    .toString()
                    .substringBefore("#")
                    .substringBefore(".")
                    .substringAfter("name=")
                    .substringBefore(",")
            if (component != lastComponent && component != "tapChannel") {
                logger.info("        Visited: {}", component)
                lastComponent = component
            }
        }
        logger.info("  }")
    }

    // ---------------------------------------------------------------------------

    /**
     * The Dead Letter Channel where failed messages are sent.
     */
    @Bean
    fun deadLetterChannel(): MessageChannel = QueueChannel()

    /**
     * Spring Retry advice with exponential backoff for configuring retry.
     */
    @Bean
    fun retryAdvice(): RequestHandlerRetryAdvice {
        val retryTemplate =
            RetryTemplate().apply {
                // Retry Policy: 3 attempts
                setRetryPolicy(
                    SimpleRetryPolicy().apply { maxAttempts = 3 },
                )

                // Backoff Policy: exponential backoff
                setBackOffPolicy(
                    ExponentialBackOffPolicy().apply {
                        initialInterval = 200
                        multiplier = 2.0
                        maxInterval = 2000
                    },
                )
            }

        return RequestHandlerRetryAdvice().apply {
            setRetryTemplate(retryTemplate)

            // When retries are exhausted -> deadLetterChannel
            setRecoveryCallback { context ->

                // Spring Retry 2.x stores the failed message under "message"
                val originalMessage = context.getAttribute("message") as? Message<*>

                val dlqMessage =
                    MessageBuilder
                        .withPayload(originalMessage?.payload ?: "UNKNOWN_PAYLOAD")
                        .setHeader("error", context.lastThrowable?.message)
                        .setHeader("originalMessage", originalMessage)
                        .build()

                deadLetterChannel().send(dlqMessage)

                dlqMessage
            }
        }
    }

    /**
     * Integration flow for inspecting messages landing in DLQ.
     */
    @Bean
    fun dlqFlow(): IntegrationFlow =
        integrationFlow {
            channel("deadLetterChannel")
            handle { msg ->
                val original = msg.headers["originalMessage"] as Message<*>
                val history = original.headers["history"] as List<*>
                val component =
                    history
                        .last()
                        .toString()
                        .substringBefore("#")
                        .substringBefore(".")
                        .substringAfter("name=")
                        .substringBefore(",")

                logger.warn("💀 DLQ received failed message: ${original?.payload}")
                logger.warn("  ❌ Error: ${msg.headers["error"]}")
                logger.warn("  📌 Component: $component")
            }
        }
}

fun main() {
    runApplication<IntegrationApplication>()
}
