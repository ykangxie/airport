##############################################
# Download and Unzip Files
##############################################
#/*get fileURl*/
#txt = readLines("http://eeyore.ucdavis.edu/stat242/data")
#ll = grep("csv.bz2", txt)   #line.number containing "csv.bz2"
#files = gsub(".*([0-9]{4}.(csv).bz2).*", "\\1", txt[ll]) #file names
#fileUrl<-sprintf("http://eeyore.ucdavis.edu/stat242/data/%s", files) #fileUrl

#/*download bz2 files*/
#destFile<-sprintf("./Desktop/airport/%s",files)
#mapply(function(url,output){download.file(url,destfile=output,method="curl")},fileUrl,destFile)

#/*unzip bz2 files*/
#cmd<-sprintf("cd ./Desktop/airport; bunzip2 -d %s", files)
#sapply(cmd,system)

##############################################
# PART I. Tools Exploration
##############################################
setwd("./Desktop/airport")
files = gsub(".*([0-9]{4}.csv).*", "\\1", list.files()[grep(".*([0-9]{4}.csv).*", list.files())])
year = gsub(".*([0-9]{4}).csv.*", "\\1", files[grep(".*([0-9]{4}.csv).*", files)])

# Tools Exploration- system.time(counts) for 2008.csv 
year="2008"

#---------------------
#(I) Shell Scripting
#---------------------
## 1.1 /grep & wc -l/ [multiple passes]
system.time(SHcounts.wc<-system("for airport in LAX OAK SFO SMF; do cut -f 17 -d , 2008.csv | grep $airport | wc -l; done",intern=TRUE))
#user  system elapsed 
#67.933   0.833  55.612 

## 1.2 /egrep & (sort + uniq -c)/ [one pass]
system.time(SHcounts.uniq<-system("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' 2008.csv | cut -f 17 -d , | sort | uniq -c",intern=TRUE))
#user  system elapsed 
#62.077   0.242  61.230

#--------------------------
#(II) R Streaming from csv
#--------------------------
source("./code/funs/rcount.R")
con<-file("2008.csv","r")
system.time(rcount(400L,"2008.csv")) #268.294 1.229 269.525 
system.time(rcount(12000L,"2008.csv")) #255.774 0.816 256.571 

#----------------------------
#(III) R Streaming from Unix
#----------------------------
source("./funs/rcount.R")
con<-pipe("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' 2008.csv","r")
system.time(RSHcount<-rcount(400L,con))
#user  system elapsed 
#19.476   0.061  57.478 

#----------------------------
#(IV) SQLite
#----------------------------
library("RSQLite")
dr=dbDriver("SQLite")
con=dbConnect(dr,dbname = "airlineTable.db")
system.time(rr<-dbSendQuery(con,"SELECT count(*) FROM delays WHERE Origin IN ('LAX', 'OAK', 'SFO', 'SMF') GROUP BY Origin;"))
system.time(fetch(rr,100))
#counts
fetch(rr,100)  
# user  system elapsed 
# 0.557   0.016   0.576
library("RSQLite.extfuns")
con=dbConnect(dr,dbname = "airlineTable.db")
init_extensions(con)
#average
aa<-dbSendQuery(con,"SELECT avg(ArrDelay) FROM delays WHERE Origin IN ('LAX', 'OAK', 'SFO', 'SMF') GROUP BY Origin;")
fetch(aa,4)
#std.dev
std<-dbSendQuery(con,"SELECT stdev(ArrDelay) FROM delays WHERE Origin IN ('LAX', 'OAK', 'SFO', 'SMF') GROUP BY Origin;")
fetch(std,4)
##############################################
# PART II. Means and Std.Dev
##############################################
#rstats() 
source("./code/funs/rstats.R")

##############################################
# Test: 2008.csv
##############################################
# i. rstats()
con<-pipe("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' 2008.csv","r")
system.time(RS.2008<-rstats(400L,con))
#user  system elapsed 
#34.124   0.099  62.476 
RS.2008[,2]/RS.2008[,1]
sqrt(RS.2008[,3]/RS.2008[,1])

# ii. awk validation
system.time(cmean<-system("egrep '([0-9]|NA),LAX,[A-Z]' 2008.csv | cut -f 15 -d , | awk 'BEGIN {s=0; c=0}; {s=s+$1;c=c+1}; END {print c,s,s/c}'",intern=TRUE))
#> counts mean (LAX 2008.csv)
#[1] "181706 5.28919"
#[1] "52886 2.05015"
#[1] "116306 10.5643"
#[1] "44990 3.32407"
#user  system elapsed for each airport
#41.994   0.185  41.596 

##############################################
# All Years: 1987-2012.csv
##############################################
Sys.setlocale(locale="C")

# i. Each Year (array)
year = gsub(".*([0-9]{4}).csv.*", "\\1", files[grep(".*([0-9]{4}.csv).*", files)])
cmds<-sprintf("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' %s.csv",year)
system.time(RS.ALL<-lapply(cmds,function(cmd){con<-pipe(cmd,"r")
                                        tmp<-rstats(400L,con)
                                        close(con)
                                        tmp}))
#user   system  elapsed  #1824.276   10.011 1285.353 
names(RS.ALL)<-year  
RS.ALL.ARRAY<-array(unlist(RS.ALL), dim = c(nrow(RS.ALL[[1]]), ncol(RS.ALL[[1]]), length(RS.ALL))) #reconstruction: (4 x 3 x 22) 
rownames(RS.ALL.ARRAY)<-rownames(RS.ALL[[1]])
colnames(RS.ALL.ARRAY)<-colnames(RS.ALL[[1]])
dimnames(RS.ALL.ARRAY)[[3]]<-year
#counts
apply(RS.ALL.ARRAY,c(3),function(x){x[,1]})
#means
apply(RS.ALL.ARRAY,c(3),function(x){x[,2]/x[,1]})
#standard deviation
apply(RS.ALL.ARRAY,c(3),function(x){sqrt(x[,3])/x[,1]})

# ii. All Year (matrix)
TOTAL<-apply(RS.ALL.ARRAY,c(1,2),sum)
TOTAL[,1]#counts
TOTAL[,2]/TOTAL[,1]  #mean
sqrt(TOTAL[,3]/TOTAL[,1]) #std.dev
TOTAL.STATS<-structure(data.frame(cbind(TOTAL[,1],TOTAL[,2]/TOTAL[,1],sqrt(TOTAL[,3]/TOTAL[,1]))),names=c("counts", "means", "std.dev"))

# iii. All Years (awk-validation)
# notes: the awk validation is a quick validation which has a larger n because of the inclusion of NA ArrDelay observations
system.time(cmean2<-system("egrep '([0-9]|NA),LAX,[A-Z]' [12]*.csv | cut -f 15 -d , | awk 'BEGIN {s=0; c=0}; {s=s+$1;c=c+1}; END {print c,s,s/c}'",intern=TRUE))
# counts mean (LAX [12]*.csv)
#LAX 3947365 23325561 5.90915"
#OAK 1134520 5635468 4.96727
#SFO 2605579 20433082 7.84205
#SMF 790378 4178518 5.28673