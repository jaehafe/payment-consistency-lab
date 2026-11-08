package lab.recon

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ReconApplication

fun main(args: Array<String>) {
    runApplication<ReconApplication>(*args)
}
