import java.rmi.AlreadyBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class RemoteServerMain {

    //Уникальное имя удаленного объекта
    public static final String UNIQUE_BINDING_NAME = "server.DBConnect";
    public static Registry registry;

    public static void main(String[] args) throws RemoteException, AlreadyBoundException, InterruptedException {

//Создаем объект сервера
        final RemoteDBConnect server = new RemoteDBConnect();

//Создаем заглушку. Передаем наш объект для возможности удаленного вызова.
        Remote stub = UnicastRemoteObject.exportObject(server, 0);

//Создаем реестр удаленных объектов, определяем порт
        registry = LocateRegistry.createRegistry(1099);

//Регистрируем заглушку
        registry.bind(UNIQUE_BINDING_NAME, stub);

        Thread.sleep(Integer.MAX_VALUE);

    }
}
