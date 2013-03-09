# find global optimal B
checkTime<-sapply(c(1000,5000,10000,15000,20000,30000,40000,50000,100000),
                  function(B){
                    con<-pipe("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' 2008.csv","r")
                    system.time(rcount(B,con,"2008"))
                  }
)
options(scipen=999)
plot(c(1000,5000,10000,15000,20000,30000,40000,50000,100000),checkTime[3,],)

# find local optimal B
checkTime.local1<-sapply(seq(400,1200,by=400),
                  function(B){
                    con<-pipe("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' 2008.csv","r")
                    system.time(rcount(B,con,"2008"))
                  }
)
checkTime.local1


checkTime.local2<-sapply(seq(50,450,by=100),
                        function(B){
                          con<-pipe("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' 2008.csv","r")
                          system.time(rcount(B,con,"2008"))
                        }
)
plot(c(seq(50,450,by=100),seq(400,1200,by=400)),c(checkTime.local2[3,],checkTime.local1[3,]))

#global plot
plot(c(seq(50,450,by=100),seq(400,1200,by=400),c(1000,5000,10000,15000,20000,30000,40000,50000,100000)),c(checkTime.local2[3,],checkTime.local1[3,],checkTime[3,]))


######################1987.csv
checkTime<-sapply(c(500,5000,10000,20000,40000,60000,94158),
                  function(B){
                    con<-pipe("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' 1987.csv","r")
                    system.time(rcount(B,con,"2008"))
                  }
)
options(scipen=999)
plot(c(500,5000,10000,20000,40000,60000,94158),checkTime[3,],)