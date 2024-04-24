//package jpoint2024;
//
//import io.quarkus.scheduler.Scheduled;
//import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
//import jakarta.enterprise.context.ApplicationScoped;
//import org.eclipse.microprofile.reactive.messaging.Channel;
//import org.eclipse.microprofile.reactive.messaging.Emitter;
//import org.eclipse.microprofile.reactive.messaging.Message;
//import org.jboss.logging.Logger;
//
//import java.util.Random;
//
//@ApplicationScoped
//public class MessageGenerator {
//    private final Logger logger;
//    private final Random random;
//
//    private final Emitter<String> topicEmitter1;
//    private final Emitter<String> topicEmitter2;
//    private final Emitter<String> topicEmitter3;
//
//    public MessageGenerator(
//            Logger logger,
//            @Channel("topic1") Emitter<String> topicEmitter1,
//            @Channel("topic2") Emitter<String> topicEmitter2,
//            @Channel("topic3") Emitter<String> topicEmitter3
//    ) {
//        this.logger = logger;
//        this.topicEmitter1 = topicEmitter1;
//        this.topicEmitter2 = topicEmitter2;
//        this.topicEmitter3 = topicEmitter3;
//
//        this.random = new Random();
//    }
//
//    @Scheduled(every = "5s")
//    public void topic1() {
//        var key = String.valueOf(random.nextInt(10));
//        var value = String.valueOf(random.nextGaussian());
//
//        topicEmitter1.send(Message.of(value)
//                .addMetadata(OutgoingKafkaRecordMetadata.<String>builder()
//                        .withKey(key))
//        );
//    }
//
//    @Scheduled(every = "5s")
//    public void topic2() {
//        var key = String.valueOf(random.nextInt(10));
//        var value = String.valueOf(random.nextGaussian());
//
//        topicEmitter2.send(Message.of(value)
//                .addMetadata(OutgoingKafkaRecordMetadata.<String>builder()
//                        .withKey(key))
//        );
//    }
//
//    @Scheduled(every = "5s")
//    public void topic3() {
//        var key = String.valueOf(random.nextInt(10));
//        var value = String.valueOf(random.nextGaussian());
//
//        topicEmitter3.send(Message.of(value)
//                .addMetadata(OutgoingKafkaRecordMetadata.<String>builder()
//                        .withKey(key))
//        );
//    }
//
//}
