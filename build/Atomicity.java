import java.sql.*;
import java.util.Scanner;

public class Atomicity {

    // Database Connection Details
    private static final String URL = "jdbc:mysql://localhost:3306/demo";
    private static final String USER = "root";
    private static final String PASSWORD = "sanjose99";

    // Call function to store
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Transfer from account ID: ");
        int fromAccount = scanner.nextInt();

        System.out.println("To receiver account ID: ");
        int toAccount = scanner.nextInt();
        System.out.println("Enter the amount to transfer: ");
        int amount = scanner.nextInt();

        System.out.println("Enter the minimum balance to be maintained: ");
        int minBalance = scanner.nextInt();

        transferMoney(fromAccount, toAccount, amount, minBalance);
        scanner.close();
    }

    // Execute functions to transfer money 1 to 2
    public static void transferMoney(int fromAccount, int toAccount, int amount, int minBalance) {
        Connection conn = null;
        try {
            // Establish connection
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
            conn.setAutoCommit(false);  // Start transaction

            // Fetch account balances
            int balance1 = getBalance(conn, fromAccount);
            int balance2 = getBalance(conn, toAccount);

            System.out.println("\nTransaction:");
            printAccountBalances();

            // Check if there are sufficient funds
            if (balance1 < amount) {
                System.out.println("Transaction Error: Insufficient funds in Account " + fromAccount);
                return;
            }

            // Check if minimum balance is maintained
            if ((balance1 - amount) < minBalance) {
                System.out.println("Transaction Error: Invalid minimum balance not maintained for Account " + fromAccount);
                return;
            }

            // Perform the transaction
            updateBalance(conn, fromAccount, balance1 - amount);
            updateBalance(conn, toAccount, balance2 + amount);

            // Commit the transaction
            conn.commit();
            System.out.println("Transaction Successful: " + amount + " transferred from Account " + fromAccount + " to Account " + toAccount);
        // Error catching
        } catch (SQLException e) {
            try {
                if (conn != null) {
                    conn.rollback();  // Rollback on failure
                    System.out.println("Transaction Rolled Back Due to Error!");
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        // Display database state
        printAccountBalances();
    }
    //check and return balance
    private static int getBalance(Connection conn, int accountId) throws SQLException {
        String query = "SELECT balance FROM accounts WHERE id = ?";
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setInt(1, accountId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("balance");
            }
        }
        return 0;
    }

    private static void updateBalance(Connection conn, int accountId, int newBalance) throws SQLException {
        String query = "UPDATE accounts SET balance = ? WHERE id = ?";
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setInt(1, newBalance);
            stmt.setInt(2, accountId);
            stmt.executeUpdate();
        }
    }

    private static void printAccountBalances() {
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM accounts")) {

            System.out.println("Updated Account Balances:");
            while (rs.next()) {
                System.out.println("Account " + rs.getInt("id") + ": " + rs.getString("name") + " - Balance: " + rs.getInt("balance"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

