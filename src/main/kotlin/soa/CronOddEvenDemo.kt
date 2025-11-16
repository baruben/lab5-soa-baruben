@file:Suppress("WildcardImport", "NoWildcardImports", "MagicNumber")

package soa

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.integration.annotation.Gateway
import org.springframework.integration.annotation.MessagingGateway
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.integration.config.EnableIntegration
import org.springframework.integration.config.EnableMessageHistory
import org.springframework.integration.dsl.IntegrationFlow
import org.springframework.integration.dsl.MessageChannels
import org.springframework.integration.dsl.Pollers
import org.springframework.integration.dsl.PublishSubscribeChannelSpec
import org.springframework.integration.dsl.integrationFlow
import org.springframework.integration.transformer.support.HeaderValueMessageProcessor
import org.springframework.messaging.Message
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

private val logger = LoggerFactory.getLogger("soa.CronOddEvenDemo")

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
 * Logs the component history through which a message has passed
 */
fun logHistory(msg: Message<*>) {
    val where = msg.headers["where"] as String
    val history = msg.headers["history"] as List<*>
    var lastComponent = ""

    logger.info("  📜 ${where}: History {")    
    history.forEach {
        val component = it.toString().substringBefore("#").substringBefore(".").substringAfter("name=").substringBefore(",")
        if (component != lastComponent && component != "tapChannel") {
            logger.info("        Visited: {}", component)
            lastComponent = component
        }
        
    }
    logger.info("  }")
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
     * Defines a publish-subscribe channel for even numbers.
     * Multiple subscribers can receive messages from this channel.
     */
    @Bean
    fun oddChannel(): PublishSubscribeChannelSpec<*> = MessageChannels.publishSubscribe()
    
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
     * Integration flow for processing even numbers.
     * Transforms integers to strings and logs the result.
     */
    @Bean
    fun oddFlow(): IntegrationFlow =
        integrationFlow("oddChannel") {
            enrichHeaders {
                header("where", "Odd Flow", true)
            }
            wireTap("tapChannel")
            transform { obj: Int ->
                logger.info("  ⚙️  Odd Transformer: {} → 'Number {}'", obj, obj)
                "Number $obj"
            }
            handle { p ->
                logger.info("  ✅ Odd Handler: Processed [{}]", p.payload)
                logger.info("  🏷️  Odd Handler: Enriched Headers [{}]", p.headers["parity"])
            }
        }

    /**
     * Integration flow for processing odd numbers.
     * Applies a filter before transformation and logging.
     * Note: Examine the filter condition carefully.
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
     * Integration flow for Wire Tap.
     */
    @Bean
    fun tapFlow() = integrationFlow("tapChannel") {
        handle { p -> 
            logHistory(p) 
        }
    }

    /**
     * Scheduled task that periodically sends negative random numbers via the gateway.
     */
    @Scheduled(fixedRate = 1000)
    fun sendNumber() {
        val number = -Random.nextInt(100)
        logger.info("🚀 Gateway injecting: {}", number)
        sendNumber.sendNumber(number)
    }
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
 * Messaging Gateway for sending numbers into the integration flow.
 * This provides a simple interface to inject messages into the system.
 * Note: Check which channel this gateway sends messages to.
 */
@MessagingGateway
interface SendNumber {
    @Gateway(requestChannel = "numberChannel")
    fun sendNumber(number: Int)
}

fun main() {
    runApplication<IntegrationApplication>()
}
