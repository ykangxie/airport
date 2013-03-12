setwd("./Desktop/airport")

library("RSQLite")
dr=dbDriver("SQLite")
con=dbConnect(dr,dbname = "airlineTable.db")
init_extensions(con)
#counts
system.time(rr<-dbSendQuery(con,"SELECT count(*) FROM delays WHERE Origin IN ('LAX', 'OAK', 'SFO', 'SMF') GROUP BY Origin;"))
system.time(fetch(rr,100))
#average
aa<-dbSendQuery(con,"SELECT avg(ArrDelay) FROM delays WHERE Origin IN ('LAX', 'OAK', 'SFO', 'SMF') GROUP BY Origin;")
fetch(aa,4)
#std.dev
std<-dbSendQuery(con,"SELECT stdev(ArrDelay) FROM delays WHERE Origin IN ('LAX', 'OAK', 'SFO', 'SMF') GROUP BY Origin;")
fetch(std,4)
RSQLite Extension 
install.packages("RSQLite.extfuns")
library("RSQLite.extfuns")

