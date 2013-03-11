#rcount()
rcount<-function(B,con){
  B=as.integer(B)
  #set up counter
  Rcounts<-structure(c(data=rep(0,4)),names=c("LAX","OAK","SFO","SMF"))
  #read in blocks
  while(TRUE){
    txt=readLines(con,n=B)
    if (length(txt)==0)
      break
    #select Origin
    temp=sapply(strsplit(txt,","),"[[",17)
    #tables for counts
    update<-as.numeric(table(temp)[names(Rcounts)])
    update[is.na(update)]<-0
    #update counters
    Rcounts<-Rcounts+update
  }
  Rcounts
}