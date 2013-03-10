#1987 test
con<-pipe("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' 1987.csv",open="r")
system.time(RS.1987<-rstats(400L,con))
#table
#user  system elapsed 
#10.749   0.025  19.469 
#tapply
#user  system elapsed 
#7.901   0.024  14.950 
RS[,2]/RS[,1]
sqrt(RS[,3]/RS[,1])