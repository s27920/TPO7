package org.example;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.swing.*;
import java.awt.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

public class ChatApp extends JFrame {
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

    private String currentChat;
    private String currentUser;
    private final Set<String> activeUsers = new HashSet<>();
    private final Set<String> activeChats = new HashSet<>();

    public ChatApp() {
        setupUI();
        setupListeners();
    }

    private void setupUI() {
        JFrame frame = new JFrame("Chat App");
        frame.setContentPane(panel1);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(800, 600);
        frame.setVisible(true);
    }

    private void setupListeners() {
        sendButton.addActionListener(e -> sendMessage());
        joinCreateChatButton.addActionListener(e -> joinOrCreateChat());

        // Refresh active users and chats periodically
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::refreshActiveUsersAndChats, 0, 5, TimeUnit.SECONDS);
    }

    private void joinOrCreateChat() {
        currentChat = ChatNameField.getText();
        currentUser = userName.getText();
        if (!currentChat.isEmpty() && !currentUser.isEmpty()) {
            activeChats.add(currentChat);
            activeUsers.add(currentUser);
            MessageConsumer consumer = new MessageConsumer(currentChat, currentUser);
            new Thread(() -> {
                while (true) {
                    consumer.kafkaConsumer.poll(Duration.ofMillis(100)).forEach(record -> {
                        chatArea.append(record.value() + "\n");
                    });
                }
            }).start();
        }
    }

    private void sendMessage() {
        String message = chatInput.getText();
        if (!message.isEmpty() && currentChat != null && currentUser != null) {
            String formattedMessage = String.format("[%s] %s: %s",
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")),
                    currentUser, message);
            MessageProducer.send(new ProducerRecord<>(currentChat, formattedMessage));
            chatInput.setText("");
        }
    }

    private void refreshActiveUsersAndChats() {
        // Update the active users and chats text areas
        activeUsersField.setText(String.join("\n", activeUsers));
        activeChatsField.setText(String.join("\n", activeChats));
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(ChatApp::new);
    }
}
