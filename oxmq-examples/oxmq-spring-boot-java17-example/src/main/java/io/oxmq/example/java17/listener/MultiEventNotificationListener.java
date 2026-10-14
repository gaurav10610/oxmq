package io.oxmq.example.java17.listener;

import io.oxmq.example.java17.model.NotificationEvent;
import io.oxmq.spring.annotation.Header;
import io.oxmq.spring.annotation.OxmqHandler;
import io.oxmq.spring.annotation.OxmqListener;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Demonstrates class-level {@link OxmqListener} with multiple {@link OxmqHandler} methods.
 * Incoming jobs are routed to the appropriate method by job name or payload type.
 */
@Component
@OxmqListener(queue = "${app.queues.notifications:java17-notifications}", concurrency = 5)
public class MultiEventNotificationListener {

    private static final Logger log = LoggerFactory.getLogger(MultiEventNotificationListener.class);

    private final List<NotificationEvent> deliveredEmails = new CopyOnWriteArrayList<>();
    private final List<NotificationEvent> deliveredSms = new CopyOnWriteArrayList<>();

    @OxmqHandler(name = "email")
    public void dispatchEmail(NotificationEvent event, @Header("jobId") String jobId) {
        log.info("[EMAIL DISPATCH] (jobId={}) To: {}, Subject: {}",
                jobId, event.recipient(), event.subject());
        deliveredEmails.add(event);
    }

    @OxmqHandler(name = "sms")
    public void dispatchSms(NotificationEvent event, @Header("jobId") String jobId) {
        log.info("[SMS DISPATCH] (jobId={}) To: {}, Content: {}",
                jobId, event.recipient(), event.content());
        deliveredSms.add(event);
    }

    public List<NotificationEvent> getDeliveredEmails() {
        return deliveredEmails;
    }

    public List<NotificationEvent> getDeliveredSms() {
        return deliveredSms;
    }

    public void clear() {
        deliveredEmails.clear();
        deliveredSms.clear();
    }
}
