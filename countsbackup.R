#(1) Shell Scrpiting
Rprof("Shell.out")
cmd<-sprintf("for airport in LAX OAK SFO SMF; do cut -f 17 -d , %s.csv | grep $airport | wc -l; done",year)
temp<-lapply(cmd,function(command) system(command,intern=TRUE))
counts<-lapply(temp,structure(as.numeric(x),names=c("LAX","OAK","SFO","SMF")))
names(counts)<-year
Rprof(NULL)
#notes:for each year.csv, we count the number of flights leaving each of the airports


#(2) R Streaming
Rprof("Rstream.out")
con=file("2008.csv","r")
LAX=0;OAK=0;SFO=0;SMF=0;
while(TRUE){
  txt=readLines(con,n=1000)
  if (length(txt)==0)
    break
  temp=sapply(strsplit(txt,","),"[[",17) 
  LAX = LAX + length(temp[(temp=="LAX")])
  OAK = OAK + length(temp[(temp=="OAK")])
  SFO = SFO + length(temp[(temp=="SFO")])
  SMF = SMF + length(temp[(temp=="SMF")])
}


Rprof(NULL)
summaryRprof("Rstream.out")


############
#let me write R streaming
####################
Sys.setlocale(locale="C")

# open a connection  // we will loop over the files later
filename="2008.csv"
#filename="temp.out"
con=file(filename,"r")

#set blocks  // we will try different B and measure the system.time later 
B=100000L

#steaming the data
LAX=0;OAK=0;SFO=0;SMF=0;
while(TRUE){
  txt=readLines(con,n=1000)
  if (length(txt)==0)
    break
  temp=sapply(strsplit(txt,","),"[[",17) 
  LAX = LAX + length(temp[(temp=="LAX")])
  OAK = OAK + length(temp[(temp=="OAK")])
  SFO = SFO + length(temp[(temp=="SFO")])
  SMF = SMF + length(temp[(temp=="SMF")])
}


txt = readLines(con,n=B)[-1]
temp<-sapply(strsplit(txt,","),"[[",17) 
############################################
output<-c()
for (i in as.numeric(year)){
  con=file(paste(i,".csv",sep=""))
  output[i]<-sapply(readLines(con,n=1),function(txt)strsplit(txt,","))
  names(counts)<-year
}



#find optimal B for blocks reading
