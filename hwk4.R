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
#1.1 /grep & wc -l/ [multiple passes]
system.time(SHcounts.wc<-system("for airport in LAX OAK SFO SMF; do cut -f 17 -d , 2008.csv | grep $airport | wc -l; done",intern=TRUE))
system.time(SHcounts.wc<-structure(as.numeric(SHcounts.wc),names=c("LAX","OAK","SFO","SMF")))

#user  system elapsed 
#67.317   0.867  55.346 

#1.2 /egrep & (sort + uniq -c)/ [one pass]
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
# Test: Mean and Std.Dev for 2008.csv
##############################################
con<-pipe("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' 2008.csv","r")
#rstats 
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
    update<-as.numeric(table(temp)[names(Rcounts)])
    #update<-tapply(ArrDelay,Origin,length)[names(Rcounts)] #table() is faster than tapply(length)
    update[is.na(update)]<-0
    Rcounts<-Rcounts+as.numeric(update)
    #1.2 Rsum
    update<-tapply(ArrDelay,Origin,sum)[names(Rsum)]
    update[is.na(update)]<-0
    Rsum<-Rsum+as.numeric(update)
    #1.3 Rvar
    update<-tapply(ArrDelay,Origin,function(x){var(x)*length(x)})[names(Rsumvar)]
    update[is.na(update)]<-0
    Rsumvar<-Rsumvar+as.numeric(update) 
  }
  cbind(Rcounts,Rsum,Rsumvar)
}

# i. rstats()
system.time(RS.ALL<-rstats(400L,con))
#user  system elapsed 
#34.124   0.099  62.476 
RS[,2]/RS[,1]
sqrt(RS[,3]/RS[,1])

# ii. awk validation
system.time(cmean<-system("egrep '([0-9]|NA),LAX,[A-Z]' 2008.csv | cut -f 15 -d , | awk 'BEGIN {s=0; c=0}; {s=s+$1;c=c+1}; END {print c,s/c}'",intern=TRUE))
#> counts mean (LAX 2008.csv)
#[1] "181706 5.28919"
#[1] "52886 2.05015"
#[1] "116306 10.5643"
#[1] "44990 3.32407"
#user  system elapsed for each airport
#41.994   0.185  41.596 

##############################################
# (II) Mean and Std.Dev for All Years
##############################################
Sys.setlocale(locale="C")
source('rstats.R')

# i. Each Year (array)
year = gsub(".*([0-9]{4}).csv.*", "\\1", files[grep(".*([0-9]{4}.csv).*", files)])
cmds<-sprintf("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' %s.csv",year)
system.time(RS.ALL<-lapply(cmds,function(cmd){con<-pipe(cmd,"r")
                                        tmp<-rstats(400L,con)
                                        close(con)
                                        tmp}))
names(RS.ALL)<-year
#user   system  elapsed 
#1824.276   10.011 1285.353 
RS.ALL.ARRAY<-array(unlist(RS.ALL), dim = c(nrow(RS.ALL[[1]]), ncol(RS.ALL[[1]]), length(RS.ALL))) #(4 x 3 x 22)
rownames(RS.ALL.ARRAY)<-rownames(RS.ALL[[1]])
colnames(RS.ALL.ARRAY)<-colnames(RS.ALL[[1]])
dimnames(RS.ALL.ARRAY)[[3]]<-year

RS<-apply(RS.ALL.ARRAY,c(1,2),sum)

RS[,1]#counts
RS[,2]/RS[,1]  #mean
sqrt(RS[,3]/RS[,1]) #std.dev

# ii. All Years (array-validation)
con<-pipe("egrep '([0-9]|NA),(LAX|OAK|SFO|SMF),[A-Z]' [12]*.csv",open="r")
system.time(RS.ALL.Validation<-rstats(400L,con))
#user   system  elapsed 
#671.574    2.686 1351.976 

RS.ALL.Validation[,1]#counts
RS.ALL.Validation[,2]/RS.ALL.Validation[,1]  #mean
sqrt(RS.ALL.Validation[,3]/RS.ALL.Validation[,1]) #std.dev

# iii. All Years (awk-validation)
system.time(cmean2<-system("egrep '([0-9]|NA),LAX,[A-Z]' [12]*.csv | cut -f 15 -d , | awk 'BEGIN {s=0; c=0}; {s=s+$1;c=c+1}; END {print c,s/c}'",intern=TRUE))
# counts mean (LAX [12]*.csv)
#[1] "3947365 5.90915"
#The awk validation is a quick validation which has a larger n without dropping NA ArrDelay observations