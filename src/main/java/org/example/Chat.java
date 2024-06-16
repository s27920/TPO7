package org.example;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.swing.*;
import java.awt.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Chat extends JFrame {
    private JPanel panel1;
    private JTextArea chatArea;
    private JTabbedPane tabbedPane1;
    private JTextField chatInput;
    private JButton sendButton;
    private JButton joinCreateChatButton;
    private JTextArea activeChatsField;
    private JTextField ChatNameField;
    private JTextField userName;
    private JTextArea activeUsersField;

    private final MessageConsumer generalAdminChat;
    private MessageConsumer currChat;
    private MessageConsumer currChatAdmin;

    private final ExecutorService clientThreadPool;
    private final DateTimeFormatter formatter;

    private ConcurrentHashMap<String, Integer> activeClients;
    private final ConcurrentHashMap<String, Integer> activeChats;
    private String topic;
    private String id;
    private String adminChat = "SUPERSECRETADMINCHANNELPLEASEDONOTENTER";

    public Chat() throws HeadlessException {
        this.add(panel1);
        this.pack();
        this.setResizable(false);
        this.setDefaultCloseOperation(EXIT_ON_CLOSE);
        this.setVisible(true);
        generalAdminChat = new MessageConsumer(adminChat, "admin");
        activeChats = new ConcurrentHashMap<>();
        clientThreadPool = Executors.newFixedThreadPool(2);
        formatter = DateTimeFormatter.ofPattern("HH:mm:ss");

        joinCreateChatButton.addActionListener(e -> login());
        sendButton.addActionListener(e -> sendMessage(topic, id));
        clientThreadPool.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                if (currChatAdmin != null) {
                    StringBuilder builder = new StringBuilder();
                    getFromAdminChannels(currChatAdmin, activeClients);
                    for (Map.Entry<String, Integer> entry : activeClients.entrySet()) {
                        if ((entry.getValue() & 1) == 1) {
                            builder.append(entry.getKey()).append("\n");
                        }
                    }
                    String activeUsers = builder.toString();
                    SwingUtilities.invokeLater(() -> activeUsersField.setText(activeUsers));
                }
                getFromAdminChannels(generalAdminChat, activeChats);
                StringBuilder sets = new StringBuilder();
                for (Map.Entry<String, Integer> entry : activeChats.entrySet()) {
                    sets.append(entry.getKey()).append("\n");
                }
                String activeChats = sets.toString();
                SwingUtilities.invokeLater(() -> activeChatsField.setText(activeChats));
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

    }

    private void login() {
        if (joinCreateChatButton.getText().equals("join/create Chat")) {
            topic = ChatNameField.getText();
            id = userName.getText();
            if (!topic.isEmpty() && !id.isEmpty()) {
                createChatReader(topic, id);
                joinCreateChatButton.setText("log out");
                sendMessageToProducer(topic + "Admin", id);
                sendMessageToProducer(adminChat, topic +"\n");
                activeClients = new ConcurrentHashMap<>();
            }
        } else {
            destroyChatReader();
        }
    }

    private void createChatReader(String topic, String id) {
        currChat = new MessageConsumer(topic, id);
        currChatAdmin = new MessageConsumer(topic + "Admin", id);
        clientThreadPool.submit(() -> {
            while (currChat != null) {
                currChat.kafkaConsumer.poll(Duration.of(1, ChronoUnit.SECONDS)).forEach(mes -> SwingUtilities.invokeLater(()->chatArea.append(mes.value())));
            }
        });
    }

    private void destroyChatReader() {
        sendMessageToProducer(topic + "Admin", id);
        activeUsersField.setText("");
        chatArea.setText("");
        joinCreateChatButton.setText("join/create Chat");
        currChat = null;
        currChatAdmin = null;
        activeClients = new ConcurrentHashMap<>();
    }

    private void sendMessage(String topic, String id) {
        if (currChat != null) {
            sendMessageToProducer(topic, LocalDateTime.now().format(formatter) + " " + id + ": " + chatInput.getText());
            chatInput.setText("");
        }
    }

    private void sendMessageToProducer(String topic, String message) {
        MessageProducer.send(new ProducerRecord<>(topic, message + "\n"));
    }

    private void getFromAdminChannels(MessageConsumer consumer, ConcurrentHashMap<String, Integer> active) {
        consumer.kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS)).forEach(mes -> active.merge(mes.value(), 1, Integer::sum));
    }
}
