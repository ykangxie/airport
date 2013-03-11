options(scipen=999)
# find global optimal B
checkTime<-sapply(c(500,5000,10000,15000,20000,30000,40000,50000,100000),
                  function(B){
                    con<-pipe("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' 2008.csv","r")
                    system.time(rcount(B,con,"2008"))
                  }
)
plot(c(500,5000,10000,15000,20000,30000,40000,50000,100000),checkTime[3,],xlab="Block Size (lines)",ylab="Elapsed Time (seconds)",main="Global Optimal B")

# find local optimal B
checkTime.local<-sapply(seq(200,800,by=100),
                        function(B){
                          con<-pipe("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' 2008.csv","r")
                          system.time(rcount(B,con,"2008"))
                        }
)
plot(seq(200,800,by=100),checkTime.local[3,],xlab="Block Size (lines)",ylab="Elapsed Time (seconds)",main="Local Optimal B")

#plot
par(mfrow=c(1,2))
plot(seq(200,800,by=100),checkTime.local[3,],xlab="Block Size (lines)",ylab="Elapsed Time (seconds)",main="Local Optimal B")

