import java.rmi.RemoteException;
import java.sql.SQLException;

public class RemoteDBConnect implements DBConnection {
//Объект для удаленного вызова
    @Override
    public String runConnect(String inputData,
                             String inputPoints,
                             String inputRollsPack) throws SQLException {

        DatabaseConnectNew.main(new String[]{inputData,inputPoints,inputRollsPack});
        return DatabaseConnectNew.returnRowCount;
    }
}
