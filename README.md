# Hangman game	
This is Hulu programming challenge, Hangman game.

# run
1) Please directly import this project to IDE IntelliJ IDEA.
2) Change URL_PREFIX to your Hangman web service.
3) Run HangmanRunner

# guessing Rules
## build dictionary 
1. Firstly I downloaded a frequency word list, which contains two element each line. First element is 'word', second is the frequency for this word.
2. Then I build a HashMap to store all the words. Key is the length of the word, value is a HashMap (key is the word, value is its frequency). In this way, I could find the most likely words given certain pattern.
## choose guessing pattern
1. If all the words are underscores pattern, choose the shortest one. ex(__ ___ ____), choose '__' as the guessing pattern. 
2. If some words contains letters that have been guessed out. Choose the one with highest known rate. (calculated by : known/unkown)
	1) __I_ _S C___ : choose '_S'
	2) __I_ _S COO_ : choose 'COO_'
	
3. If several words' known are the same, choose the longest one.
  1) _HI_ _S C___ : choose '_HI_'
  2) I __LL ____ T__ _F RE___EE_ : choose 'RE___EE_'
  
## build candidate word list
Choose all the candidate words that matches the pattern of this target pattern
## choose target char
Then choose target word with the highest frequency in the candidate word list. Then choose the target letter from with the highest frequency among all the missing words in this pattern.







