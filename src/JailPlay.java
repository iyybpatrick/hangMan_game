/**
 * Created by YuebinYang on 9/20/17.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.net.URL;
import java.io.InputStreamReader;
import java.util.regex.*;

//Parse state from hulu API
public class JailPlay {
    private static Prisoner prisoner;
    private static final String URL_PREFIX = "http://gallows.hulu.com/play?code=yuebin.patrick@gmail.com";

    public static Prisoner playGame(String s){
        try{
            String gameUrl = URL_PREFIX + s;
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    new URL(gameUrl).openStream()));
            String info = in.readLine();

            prisoner = getInfo(info);
        }catch(IOException e){
            System.err.println(e);
        }
        return prisoner;
    }

    public static Prisoner getInfo(String info) {
        Prisoner prisoner = new Prisoner();
        Pattern p = Pattern.compile("(ALIVE|DEAD|FREE)"); // get the status
        Matcher m = p.matcher(info);

        Pattern p1 = Pattern.compile("(\\d+)"); //get the token
        Matcher m1 = p1.matcher(info);

        Pattern p2 = Pattern.compile("(\\d)(,)"); //get remaining guesses
        Matcher m2 = p2.matcher(info);

        Pattern p3 = Pattern.compile("([A-Z_'\\s]+)(\"})"); //get state
        Matcher m3 = p3.matcher(info);

        if(m.find() && m1.find() && m2.find() && m3.find()){
            prisoner.setGameStatus(m.group());
            prisoner.setToken(m1.group());
            prisoner.setRemaining(Integer.parseInt(m2.group(1)));
            prisoner.setState(m3.group(1));
        } else {
            System.out.println("Parse error !");
        }
        return prisoner;
    }
    //start a new game
    public static Prisoner startGame(){
        return playGame("");
    }
    //continue until the prisoner is dead
    public static Prisoner makeGuess(Prisoner prisoner, char guess){
        return playGame(String.format("&token=%s&guess=%s", prisoner.getToken(), guess));
    }
}
