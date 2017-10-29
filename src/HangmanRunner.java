/**
 * Created by YuebinYang on 9/20/17.
 */
public class HangmanRunner {
    public static void main(String[] args){
        HangmanGuess brain = new HangmanGuess();
        Prisoner hostage = JailPlay.startGame();
        int numError = 0;
        int correctGuess = 0;
        while(hostage.getGameStatus().equals("ALIVE")){
            System.out.println("current state: " + hostage.getState());
            char guess = brain.makeGuess(hostage.getState()); //get a guess using AI
            Prisoner hostage_after = JailPlay.makeGuess(hostage, guess); //Update the state

            if(hostage_after.getRemaining() == 0 ||
                    hostage.getState().equals(hostage_after.getState())){

                numError++;
                brain.update(guess, false);
                System.out.println("Wrong guess : " + guess + " ," + "remains : " + hostage_after.getRemaining());
            }
            else {
                correctGuess++;
                brain.update(guess, true);
                System.out.println(guess + " is " + true);
            }
            hostage = hostage_after;
        }
        if(hostage.getGameStatus().equals("DEAD")){
            System.out.println(hostage.getState());
            System.out.println("No ! Dead !");
            System.out.println("Correct: " + correctGuess + ", Failure: 3.");
        }
        if(hostage.getGameStatus().equals("FREE")){
            System.out.println(hostage.getState());
            System.out.println("Congratulations ! You are free !");
            System.out.println("Correct: " + correctGuess + ", Failure: " + numError + ".");
        }
    }
}
