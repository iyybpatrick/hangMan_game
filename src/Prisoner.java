/**
 * Created by YuebinYang on 9/20/17.
 */
public class Prisoner {

    private String gameStatus;  // alive | dead | free
    private String token;       // ID of instance of the game
    private int remaining;      // guess chance remaining
    private String state;       // ex:  h_ _s _ _oo_ _oy

    void setGameStatus(String gameStatus) {
        this.gameStatus = gameStatus;
    }
    void setToken(String token) {
        this.token = token;
    }

    void setRemaining(int remaining) {
        this.remaining = remaining;
    }

    void setState(String state) {
        this.state = state;
    }

    String getGameStatus(){
        return this.gameStatus;
    }
    String getToken() {
        return this.token;
    }
    String getState() {
        return this.state;
    }
    int getRemaining() {
        return remaining;
    }
}
