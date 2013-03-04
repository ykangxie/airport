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
files = gsub(".*([0-9]{4}.csv).*", "\\1", list.files()[grep("csv", list.files())])
year = gsub(".*([0-9]{4}).csv.*", "\\1", files[grep("csv", files)])
year = as.character(1987:2008)

#############################
# TIME: counts for 2008.csv
#############################
setwd("./Desktop/airport")

#(1) Shell Scrpiting
Rprof("Shell.out")
year="2008"
cmd<-sprintf("for airport in LAX OAK SFO SMF; do cut -f 17 -d , %s.csv | grep $airport | wc -l; done",year)
temp<-lapply(cmd,function(command) system(command,intern=TRUE))
SHcounts<-lapply(temp,function(x)structure(as.numeric(x),names=c("LAX","OAK","SFO","SMF")))
names(SHcounts)<-year
Rprof(NULL)
#notes:for each year.csv, we count the number of flights leaving each of the airports

#(2) R Streaming
Sys.setlocale(locale="C")

Rprof("Rstream1.out")
B=1L
con=file("2008.csv","r")
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
Rprof(NULL)
summaryRprof("Rstream1.out")


#(3) Rprof Compare
summaryRprof("Shell.out")
summaryRprof("Rstream.out")
