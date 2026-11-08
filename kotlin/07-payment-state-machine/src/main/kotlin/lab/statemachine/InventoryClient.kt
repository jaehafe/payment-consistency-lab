package lab.statemachine

import org.springframework.stereotype.Component

/** 결제 후속 단계: 재고 확보 (다른 서비스라고 가정) */
interface InventoryClient {
    fun reserve(orderId: String, sku: String)
}

class OutOfStockException(sku: String) : RuntimeException("재고 없음: $sku")

@Component
class FakeInventoryClient : InventoryClient {
    @Volatile var failReserve = false

    override fun reserve(orderId: String, sku: String) {
        if (failReserve) throw OutOfStockException(sku)
    }
}
