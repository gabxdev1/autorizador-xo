package br.com.gabxdev.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class MessageConsumer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(
            topics = "sensibilizar-limite",
            groupId = "alterar-ldc")
    public void consume(String idEfetivacao,
                        Acknowledgment acknowledgment) {
        log.info("alterando limite para id: {}", idEfetivacao);

        try {
            long delayMs = 200 + (long) (Math.random() * 500);
            Thread.sleep(delayMs);
            kafkaTemplate.send("topico-retorno-x0", idEfetivacao);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Processo interrompido durante o delay", e);
        }

        acknowledgment.acknowledge();
    }
}
