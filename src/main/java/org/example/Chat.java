package org.example;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.swing.*;
import java.awt.*;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

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

    private final ScheduledExecutorService clientThreadPool;
    private final DateTimeFormatter formatter;

    private final ConcurrentHashMap<String, Integer> activeChats;
    private ConcurrentHashMap<String, Integer> activeClients;

    private static final String adminChat = "SUPERSECRETADMINCHANNELPLEASEDONOTENTER";
    private String topic;
    private String id;

    private static final MessageConsumer generalAdminChat = new MessageConsumer(adminChat, "admin");
    private MessageConsumer currChatAdmin;
    private MessageConsumer currChat;
    private boolean loggedIn;
    private ScheduledFuture<?> readerTask;

    public Chat() throws HeadlessException {
        loggedIn = false;
        activeChats = new ConcurrentHashMap<>();
        clientThreadPool = Executors.newScheduledThreadPool(2);
        formatter = DateTimeFormatter.ofPattern("HH:mm:ss");

        joinCreateChatButton.addActionListener(e -> login());
        sendButton.addActionListener(e -> sendMessage(topic, id));
        chatInput.addKeyListener(new KeyAdapter() {
            @Override
            public void keyPressed(KeyEvent e) {
                if (e.getKeyCode() == KeyEvent.VK_ENTER) {
                    sendMessage(topic,id);
                }
            }
        });

        Timer timer = new Timer(10, (e) -> {
            if (currChatAdmin != null) {
                StringBuilder builder = new StringBuilder();
                getFromAdminChannels(currChatAdmin, activeClients);
                for (Map.Entry<String, Integer> entry : activeClients.entrySet()) {
                    if ((entry.getValue() & 1) == 1) {
                        builder.append(entry.getKey());
                    }
                }
                String activeUsers = builder.toString();
                SwingUtilities.invokeLater(() -> activeUsersField.setText(activeUsers));
            }
            getFromAdminChannels(generalAdminChat, activeChats);
            StringBuilder sets = new StringBuilder();
            activeChats.forEach((k, v) -> sets.append(k));
            SwingUtilities.invokeLater(() -> activeChatsField.setText(sets.toString()));
        });
        timer.start();

        this.add(panel1);
        this.pack();
        this.setResizable(false);
        this.setDefaultCloseOperation(EXIT_ON_CLOSE);
        this.setVisible(true);
    }

    private void login() {
        if (!loggedIn) {
            topic = ChatNameField.getText();
            id = userName.getText();
            if (!topic.isEmpty() && !id.isEmpty()) {
                createChatReader(topic, id);
                SwingUtilities.invokeLater(() -> joinCreateChatButton.setText("log out"));
                ChatNameField.setEnabled(false);
                sendMessageToProducer(topic + "Admin", id);
                loggedIn = !loggedIn;
            }
        } else {
            destroyChatReader();
            loggedIn = !loggedIn;
        }
    }

    private void createChatReader(String topic, String id) {
        activeClients = new ConcurrentHashMap<>();
        sendMessageToProducer(adminChat, topic);
        activeChats.putIfAbsent(topic+"\n", 1);
        currChat = new MessageConsumer(topic, id);
        currChatAdmin = new MessageConsumer(topic + "Admin", id);
        readerTask = clientThreadPool.scheduleAtFixedRate(() -> {
            if (currChat != null) {
                System.out.println("this is " + id + " reading from " + topic);
                currChat.kafkaConsumer.poll(Duration.of(1, ChronoUnit.SECONDS)).forEach(mes -> SwingUtilities.invokeLater(() -> chatArea.append(mes.value())));
            }else {
                System.out.println("this is: " + id + "signing out od: " + topic);
                readerTask.cancel(false);
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    private void destroyChatReader() {
        sendMessageToProducer(topic + "Admin", id);
        currChatAdmin = null;
        currChat = null;
        SwingUtilities.invokeLater(() -> activeUsersField.setText(""));
        SwingUtilities.invokeLater(() -> chatArea.setText(""));
        SwingUtilities.invokeLater(() -> joinCreateChatButton.setText("join/create Chat"));
        ChatNameField.setEnabled(true);
    }

    private void sendMessage(String topic, String id) {
        if (currChat != null) {
            sendMessageToProducer(topic, LocalDateTime.now().format(formatter) + " " + id + ": " + chatInput.getText());
            SwingUtilities.invokeLater(() -> chatInput.setText(""));
        }
    }

    private void sendMessageToProducer(String topic, String message) {
        MessageProducer.send(new ProducerRecord<>(topic, message + "\n"));
    }

    private void getFromAdminChannels(MessageConsumer consumer, ConcurrentHashMap<String, Integer> active) {
        consumer.kafkaConsumer.poll(Duration.of(10, ChronoUnit.MILLIS)).forEach(mes -> active.merge(mes.value(), 1, Integer::sum));
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(Chat::new);
    }
}
