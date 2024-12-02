all: 
	killall -9 java
	clear
	javac *.java
	java DataServer 2005 &
	\n
	java DataServer 2002 2005