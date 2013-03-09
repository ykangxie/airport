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
# PART I. Counts
##############################################
files = gsub(".*([0-9]{4}.csv).*", "\\1", list.files()[grep(".*([0-9]{4}.csv).*", list.files())])
year = gsub(".*([0-9]{4}).csv.*", "\\1", files[grep(".*([0-9]{4}.csv).*", files)])

################################################
# Exploration- system.time(counts) for 2008.csv
################################################
setwd("./Desktop/airport")

dept=c("LAX","OAK","SFO","SMF")
year="2008"

#(I) Shell Scripting
#1.1 /grep & wc -l/
system.time(SHcounts.wc<-system("for airport in LAX OAK SFO SMF; do cut -f 17 -d , 2008.csv | grep $airport | wc -l; done",intern=TRUE))
system.time(SHcounts.wc<-structure(as.numeric(SHcounts.wc),names=c("LAX","OAK","SFO","SMF")))

#user  system elapsed 
#67.317   0.867  55.346 

#1.2 /egrep & (sort + uniq -c)/
system.time(SHcounts.uniq<-system("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' 2008.csv | cut -f 17 -d , | sort | uniq -c",intern=TRUE))

#user  system elapsed 
#61.342   0.185  60.458 

#(II) R Streaming 
rstreaming<-function(B,csvfile="2008.csv"){
  B=as.integer(B)
  con=file(csvfile,"r")
  Rcounts<-structure(c(data=rep(0,4)),names=c("LAX","OAK","SFO","SMF"))
  while(TRUE){
    txt=readLines(con,n=B)
    if (length(txt)==0)
      break
    temp=sapply(strsplit(txt,","),"[[",17)
    update<-as.numeric(table(temp)[names(Rcounts)])
    update[is.na(update)]<-0
    Rcounts<-Rcounts+update
  }
  Rcounts
}

system.time(rstreaming(400L,"2008.csv")) #261.543   1.146 262.663
system.time(rstreaming(12000L,"2008.csv")) #257.718   0.922 258.620 


#(III) R Streaming from Unix
con<-pipe("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' 2008.csv","r")
rcount<-function(B,con,year){
  B=as.integer(B)
  Rcounts<-structure(c(data=rep(0,4)),names=c("LAX","OAK","SFO","SMF"))
  while(TRUE){
    txt=readLines(con,n=B)
    if (length(txt)==0)
      break
    temp=sapply(strsplit(txt,","),"[[",17)
    update<-as.numeric(table(temp)[names(Rcounts)])
    update[is.na(update)]<-0
    Rcounts<-Rcounts+update
  }
  Rcounts
}
system.time(RSHcount<-rcount(400L,con,"2008"))
#user  system elapsed 
#19.476   0.061  57.478 

##############################################
# (II) Mean and Std.Dev for 2008.csv
##############################################
con<-pipe("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' 2008.csv","r")
rstats<-function(B,con,year){
  B=as.integer(B)
  #set up counters
  Rcounts<-structure(c(data=rep(0,4)),names=c("LAX","OAK","SFO","SMF"))
  Rsum<-structure(c(data=rep(0,4)),names=c("LAX","OAK","SFO","SMF"))
  Rsumvar<-structure(c(data=rep(0,4)),names=c("LAX","OAK","SFO","SMF"))
  #reading blocks
  while(TRUE){
    txt=readLines(con,n=B)
    if (length(txt)==0)
      break
    #ArrDelay & Airport
    ArrDelay=as.numeric(sapply(strsplit(txt,","),"[[",15))
    Origin=as.factor(sapply(strsplit(txt,","),"[[",17))
    #dropNA
    dropNA<-!is.na(ArrDelay)
    ArrDelay<-ArrDelay[dropNA]
    Origin<-Origin[dropNA]
    #1.1 Rcounts
    update<-as.numeric(table(Origin)[names(Rcounts)])
    update[is.na(update)]<-0
    Rcounts<-Rcounts+update
    #1.2 Rsum
    update<-tapply(ArrDelay,Origin,sum)[names(Rsum)]
    update[is.na(update)]<-0
    Rsum<-Rsum+update
    #1.3 Rvar
    update<-tapply(ArrDelay,Origin,function(x){var(x)*length(x)})[names(Rsumvar)]
    update[is.na(update)]<-0
    Rsumvar<-Rsumvar+update   
  }
  cbind(Rcounts,Rsum,Rsumvar)
}

system.time(RS.ALL<-rstats(400L,con))
#user  system elapsed 
#34.124   0.099  62.476 
RS[,2]/RS[,1]
sqrt(RS[,3]/RS[,1])

# awk validation
system.time(cmean<-system("egrep '([0-9]|NA),LAX,[A-Z]' 2008.csv | cut -f 15 -d , | awk 'BEGIN {s=0; c=0}; {s=s+$1;c=c+1}; END {print c,s/c}'",intern=TRUE))
#> counts mean (LAX 2008.csv)
#[1] "181706 5.28919"
#[1] "52886 2.05015"
#[1] "116306 10.5643"
#[1] "44990 3.32407"
#user  system elapsed for each airport
#41.994   0.185  41.596 
#43.088   0.216  43.191 
system.time(cmean2<-system("egrep '([0-9]|NA),LAX,[A-Z]' [12]*.csv | cut -f 15 -d , | awk 'BEGIN {s=0; c=0}; {s=s+$1;c=c+1}; END {print c,s/c}'",intern=TRUE))
# counts mean (LAX [12]*.csv)
#[1] "3947365 5.90915"


##############################################
# (II) Mean and Std.Dev for All Years
##############################################
Sys.setlocale(locale="C")
#ALL YEAR
year = gsub(".*([0-9]{4}).csv.*", "\\1", files[grep(".*([0-9]{4}.csv).*", files)])
cmds<-sprintf("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' %s.csv",year)
#temp<-lapply(cmds,system,intern=TRUE)

RS.ALL.ARRAY<-lapply(cmds,function(cmd){con<-pipe(cmd,"r")
                                        tmp<-rstats(400L,con)
                                        close(con)
                                        tmp},
                     simplify=True)
names(RS.ALL.ARRAY)<-year

con<-pipe("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' [12]*.csv",open="r")
system.time(RS.ALL<-rstats(400L,con))
#user   system  elapsed 
#763.809    2.189 1332.127 

#1987
con<-pipe("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' 1987.csv",open="r")
system.time(RS.1987<-rstats(400L,con))
#table
#user  system elapsed 
#10.749   0.025  19.469 
#tapply
#user  system elapsed 
#8.725   0.040  15.688 
RS[,2]/RS[,1]
sqrt(RS[,3]/RS[,1])