import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;

public interface DBConnection extends Remote {
    String runConnect(String inputData, String inputPoints, String inputRollsPack) throws RemoteException, SQLException;
}
