package org.example;

import lombok.extern.slf4j.Slf4j;

import org.springframework.kafka.test.EmbeddedKafkaBroker;

import javax.swing.*;

//@Slf4j
public class Main {
    public static void main(String[] args) {
        EmbeddedKafkaBroker embeddedKafkaBroker = new EmbeddedKafkaBroker(1).kafkaPorts(9092);
        embeddedKafkaBroker.afterPropertiesSet();
        SwingUtilities.invokeLater(Chat::new);
        SwingUtilities.invokeLater(Chat::new);
    }
}

