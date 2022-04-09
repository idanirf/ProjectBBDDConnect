import java.sql.SQLException;

public class Main {
    public static void main(String[] args){
        DataBaseManager db = DataBaseManager.getInstance();

        try {
            db.open();
            var res = db.select("Select 'Hello word'");
            if (res.isPresent() && res.get().next()){
                System.out.println("OK!!");
            }

            res = db.select("select * from calificacion");
            if (res.isPresent()){
                var row = res.get();
                while(row.next()){
                    System.out.println(row.getString("Nota"));
                }
            }


            db.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
