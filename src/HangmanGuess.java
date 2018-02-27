/**
 * Created by YuebinYang on 9/20/17.
 */
import java.io.IOException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.regex.*;

public class HangmanGuess {
    private Set<Character> correctChar = new HashSet<>();
    private Set<Character> incorrectChar = new HashSet<>();
    private Map<Integer, HashMap<String, Integer>> dicList = new HashMap<>();

    HangmanGuess() {
        try {
            Pattern integerPattern = Pattern.compile("\\d+");

            for (File f : new File("dictionary/").listFiles()) {
                BufferedReader reader = new BufferedReader(new FileReader(f));
                for (String str; (str = reader.readLine()) != null;) {
                    str = str.trim();
                    String[] pair = str.split(" ");

                    // if frequency is invalid
                    if (!integerPattern.matcher(pair[1]).matches()) continue;
                    // if word is invalid
                    if (pair[0] != null && pair[0].length() <= 2) {
                        if (pair[0].charAt(0) < 'a' || pair[0].charAt(0) > 'z') continue;
                    }

                    if (!dicList.containsKey(pair[0].length())) {
                        dicList.put(pair[0].length(), new HashMap<String, Integer>());
                    }
                    dicList.get(pair[0].length()).put(pair[0], Integer.parseInt(pair[1]));
                }
            }
        } catch (IOException e) {
            System.err.println(e);
            System.out.println("Dictionary initial error.");
        }
    }



    /**
     * make a guess based on dictionary and history guess log
     * @param state : state parsed from Json
     * @return      : char ready to guess
     */
    public char makeGuess(String state) {
        List<String> state_list = new ArrayList<>(Arrays.asList(state.split("[^A-Z_']+")));

        String guess_pattern = findGuessPattern(state_list);

        //if the length is 1, then most likely letters are 'a' and 'i'
        if(guess_pattern.length() == 1){
            return highest_frequency_guess();
        }
        List<String> candidateList = findCandidateList(guess_pattern);
        String targetWord = findGuessWord(candidateList);
        return findGuessLetter(targetWord);
    }

    /**
     * Find guess pattern from parsed list
     * @param state_list : parsed list
     * @return           : target pattern to guess
     */
    private String findGuessPattern(List<String> state_list) {
        int count = 0;
        String guessing = null;
        float globalKnownRatio = -1;
        int allunderScore = Integer.MAX_VALUE;

        for (String word : state_list) {
            int unknown = 0;
            int known = 0;
            boolean needGuess = false;
            for (char c : word.toCharArray()) {
                if (c == '_') {
                    needGuess = true;
                    unknown ++;
                } else {
                    known ++;
                }
            }
            // contains underscore in this word
            if (needGuess) {
                float knowRatio = known / unknown;
                if (knowRatio == globalKnownRatio) {
                    if(word.length() > guessing.length()) {
                        guessing = word;
                    }
                } else if (knowRatio > globalKnownRatio) {
                    globalKnownRatio = knowRatio;
                    guessing = word;
                } else {
                    if (globalKnownRatio == 0 && unknown < allunderScore) {
                        allunderScore = unknown;
                        guessing = word;
                    }
                }
            }
        } // for
        return guessing;
    }

    /**
     * Find candidate words for guessing
     * @param guessing : given guessing pattern
     * @return         : a list contains all the candidate words for guessing
     */
    private List<String> findCandidateList(String guessing) {

        StringBuilder excluding = new StringBuilder();
        for (Iterator<Character> ex = incorrectChar.iterator(); ex.hasNext();) {
            excluding.append(ex.next());
        }

        List<String> candidateList = new LinkedList<>();
        //guessing the one with least number of '_'
        String word = guessing.toLowerCase();
        Pattern regex = Pattern.compile(word.replace("_",
                (excluding.length() > 0) ? String.format("[a-z&&[^%s]]", excluding) : "[a-z]"));
        if (dicList.containsKey(word.length())) {
            for (String guess : dicList.get(word.length()).keySet()) {
                Matcher match = regex.matcher(guess);
                if (match.find()) {
                    candidateList.add(guess); // get a list of words that match the state
                }
            }
        }
        return candidateList;
    }

    /**
     * Find target word for guessing among candidate word list
     * @param candidateList : candidate word list
     * @return              : target word for guessing
     */
    private String findGuessWord(List<String> candidateList) {
        int frequencyCnt = 0;
        String targetWord = null;
        for (String possible : candidateList) {
            int thisCnt = dicList.get(possible.length()).get(possible);
            if (thisCnt > frequencyCnt) {
                frequencyCnt = thisCnt;
                targetWord = possible;
            }
        }
        return targetWord;
    }


    /**
     * Given target String, find letter with highest frequency in dictionary
     * @param targetWord : target word for guessing
     * @return : Most possible char to guess given target word
     */
    private char findGuessLetter(String targetWord) {
        if (targetWord == null) {
            return highest_frequency_guess();
        }

        char guessLetter = 'z';
        int letterFrequency = 0;
        boolean letter_not_found = true;
        for (int i = 0; i < targetWord.length(); i++) {
            char c = targetWord.charAt(i);
            if (correctChar.contains(c) || incorrectChar.contains(c)) {
                continue;
            }
            if (dicList.get(1).get(String.valueOf(c)) > letterFrequency) {
                letterFrequency = dicList.get(1).get(String.valueOf(c));
                guessLetter = c;
                letter_not_found = false;
            }
        }
        return letter_not_found ? highest_frequency_guess() : guessLetter;
    }

    /**
     * based on feedback from hulu API, update correct and incorrect set.
     * @param guess     : guessing char
     * @param success   : guessing correct or incorrect
     */
    public void update(char guess, boolean success) {
        if (success) {
            correctChar.add(guess);
        } else {
            incorrectChar.add(guess);
        }
    }

    /**
     * find guessing char with the highest frequency
     * @return : guessing char
     */
    public char highest_frequency_guess() {
        int letterFre = 0;
        char guessLetter = 'z';
        for (char c = 'a'; c <= 'z'; ++c) {
            if (!(correctChar.contains(c) || incorrectChar.contains(c))) {
                if (dicList.get(1).get(String.valueOf(c)) > letterFre) {
                    letterFre = dicList.get(1).get(String.valueOf(c));
                    guessLetter = c;
                }
            }
        }
        return guessLetter;
    }
}