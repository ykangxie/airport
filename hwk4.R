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
# (I) Counts
##############################################
files = gsub(".*([0-9]{4}.csv).*", "\\1", list.files()[grep(".*([0-9]{4}.csv).*", list.files())])
year = gsub(".*([0-9]{4}).csv.*", "\\1", files[grep(".*([0-9]{4}.csv).*", files)])
year = as.character(1987:2008)

#############################
# TIME: counts for 2008.csv
#############################
setwd("./Desktop/airport")

#(1) Shell Scrpiting
year="2008"
shcount<-function(year){
  cmd<-sprintf("for airport in LAX OAK SFO SMF; do cut -f 17 -d , %s.csv | grep $airport | wc -l; done",year)
  temp<-lapply(cmd,function(command) system(command,intern=TRUE))
  SHcounts<-lapply(temp,function(x)structure(as.numeric(x),names=c("LAX","OAK","SFO","SMF")));
  names(SHcounts)<-year
  SHcounts
}
system.time(SHcounts2008<-shcount("2008"))
Time.SHcounts<-.Last.value

#notes:for each year.csv, we count the number of flights leaving each of the airports

#(2) R Streaming
rcount<-function(B,csvfile){
  B=as.integer(B)
  con=file(csvfile,"r")
  LAX=0;OAK=0;SFO=0;SMF=0;
  while(TRUE){
    txt=readLines(con,n=B)
    if (length(txt)==0)
      break
    temp=sapply(strsplit(txt,","),"[[",17) 
    LAX = LAX + length(temp[(temp=="LAX")])
    OAK = OAK + length(temp[(temp=="OAK")])
    SFO = SFO + length(temp[(temp=="SFO")])
    SMF = SMF + length(temp[(temp=="SMF")])
  }
  Rcounts<-cbind(LAX,OAK,SFO,SMF)
  row.names(Rcounts)<-year
  Rcounts
}

# find global optimal B
checkTime<-sapply(c(1000,5000,10000,50000,100000),
                  function(B){
                    csvfile="2008.csv"
                    system.time(rcount(B,csvfile))
                    }
                  )
options(scipen=999)
plot(c(1000,5000,10000,50000,100000),checkTime[3,],)

# find local optimal B around 100000
checkTime.local<-sapply(c(8000,12000,14000,16000,18000),
                  function(B){
                    csvfile="2008.csv"
                    system.time(rcount(B,csvfile))
                  }
)
plot(c(5000,8000,12000,14000,16000,18000,10000),c(268.263,checkTime.local[,3],258.497))

#8000
system.time(rcount(8000,"2008.csv"))
#user  system elapsed 
#261.613   1.010 262.640

#12000
system.time(rcount(12000,"2008.csv"))
#user  system elapsed 
#255.807   0.799 256.567 

#20000
system.time(rcount(20000,"2008.csv"))
#user  system elapsed 
#264.036   0.712 264.747 

plot(c(5000,seq(8000,20000,by=2000),c(22000,24000,26000),c(28000,30000,32000)),c(268.263,262.640,258.497,256.567,checkTime.local[3,],264.747,checkTime.local2[3,],checkTime.local3[3,]),xlab="B lines",ylab=("elapsed.time (secs)"))

#ii. Rprof the best "B"
Sys.setlocale(locale="C")
Rprof("Rstream.out")
rcount(5000,2008.csv)
Rprof(NULL)
summaryRprof("Rstream.out")

#(3) Rprof Compare
summaryRprof("Shell.out")
summaryRprof("Rstream.out")

##############################################
# (II) Mean and Std.Dev
##############################################
#system("egrep '(LAX|SFO|OAK|SMF)' [12]*.csv > CalFlights")
#system("tail -f Counts")
year = as.character(1987:2008)

#Shell
cat 2008.csv | cut -f 17 -d , | egrep '(LAX|OAK|SFO|SMF)' | sort | uniq | wc -l
cmean2<-system("egrep [0-9],LAX 2008.csv | cut -f 15 -d , | awk 'BEGIN {s=0; c=0}; {s=s+$1;c=c+1}; END {print c,s/c}'",intern=TRUE)
#> cmean
#[1] "181706 5.28919"

#R
con=pipe(cut -f 17 d , | grep LAX|SFO|SMF|OAK| )
