# Working directory
setwd("E:/DMS_WorkDir1/")

# Either define a set of datasets and fractions to analyze:
#set <- read.csv("Set34.csv",stringsAsFactors = FALSE)
#dataset <- set$DatasetName
#fraction <- set$Fraction

# Or define a single dataset to analyze:
dataset <- "TEDDY_DISCOVERY_SET_34_23_20Oct15_Frodo_15-08-15"
fraction <- 23

# When def.baseline is TRUE, use default.baseline, for example Baseline-1-10-SMAQC.csv
# When def.baseline is FALSE, associate a csv file with alt.baseline
def.baseline <- FALSE

# The csv file associated with alt.baseline should have a header row then one row per dataset
# Those datasets will be used to establish new baseline data
# Header row: DatasetName,Fraction
# Example data row: TEDDY_DISCOVERY_SET_01_23_07May15_Frodo_15-03-07,1
alt.baseline <- "Example_Custom_Baseline_File.csv"

# Subfolder (below the working directory) with _ReporterIons.txt files
MASIC <- "./"

# Data file with SMAQC data
# Should have a header row: DatasetName,Fraction,Dataset_ID,P_2C,MS1_2B,RT_MS_Q1,RT_MS_Q4,RT_MSMS_Q1,RT_MSMS_Q4,Date
# Then one row per dataset: TEDDY_DISCOVERY_SET_01_01_07May15_Frodo_15-03-07,1,428201,455,1304800,0.29,0.3,0.28,0.3,7-May-15
# Must have the SMAQC data for both the dataset of interest and all of the baseline datasets
SMAQC <- "SMAQC_Data.csv"

# Must pre-install the packages using:
# install.packages(c("pcaPP", "robust", "e1071", "reshape", "ggplot2", "XML", "mvoutlier", "zoo"))
#
# Or install individually, for example:
# install.packages("pcaPP");
# install.packages("e1071");

# Load libraries
library(pcaPP)
library(robust)
library(e1071)
library(reshape)
library(ggplot2)
library(XML)
library(mvoutlier)
library(zoo)

QC_ART <- function(dataset,fraction,def.baseline,alt.baseline,MASIC,SMAQC){
  default.baseline <- "Baseline-1-10-SMAQC.csv"
  
  #####################################
  ####Libraries########################
  #####################################
  # library(pcaPP)
  # library(robust)
  # library(e1071)
  # library(reshape)
  # library(ggplot2)
  # library(XML)
  # library(mvoutlier)
  # library(zoo)
  
  
  ###################################
  ###Data Import and Refinement Step#
  ###################################
  
  # MASIC files
  load_data <- function(dataset){
    files <- list.files(path=MASIC,pattern=paste(dataset,"_ReporterIons",sep=""))
    files <- lapply(files, function(x) paste(MASIC,x,sep=""))
    lapply(files,read.csv,header=TRUE,fill=TRUE,row.names=NULL,sep="\t")
  }
  RIres <- lapply(dataset,function(x) load_data(x))

  fn = function(x){
    data.frame(Dataset_ID = x$Dataset[1],PIMZ_skew=skewness(x$ParentIonMZ),BPMZ_skew=skewness(x$BasePeakMZ),
               I119_skew=skewness(x$Ion_119),I121_median=median(x$Ion_121),I121_skew=skewness(x$Ion_121),
               WAPIC_skew=skewness(x$Weighted.Avg.Pct.Intensity.Correction),
               MissingIons=sum(c(x$Ion_113,x$Ion_114,x$Ion_115,x$Ion_116,x$Ion_117,x$Ion_118,x$Ion_119,x$Ion_121)==0)
               /(length(x$Ion_113)+length(x$Ion_114)+length(x$Ion_115)+length(x$Ion_116)+length(x$Ion_117)
                 +length(x$Ion_118)+length(x$Ion_119)+length(x$Ion_121)))
  }
  
  temp=lapply(RIres,function(x) fn(x[[1]]))
  RI_final=do.call(rbind,temp)
  
  SMAQC.var <- read.csv(SMAQC)
  
  if(def.baseline){
    def.names <- read.csv(default.baseline,stringsAsFactors=FALSE)$DatasetName
  }else{
    def.names <- read.csv(alt.baseline,stringsAsFactors=FALSE)$DatasetName
  }

  SMAQC.var$chromatog <- (((SMAQC.var$RT_MS_Q1-mean(subset(SMAQC.var$RT_MS_Q1,SMAQC.var$DatasetName%in%def.names)))-
                             (SMAQC.var$RT_MS_Q4-mean(subset(SMAQC.var$RT_MS_Q4,SMAQC.var$DatasetName%in%def.names))))
                          +((SMAQC.var$RT_MSMS_Q1-mean(subset(SMAQC.var$RT_MSMS_Q1,SMAQC.var$DatasetName%in%def.names)))-
                              (SMAQC.var$RT_MSMS_Q4-mean(subset(SMAQC.var$RT_MSMS_Q4,SMAQC.var$DatasetName%in%def.names)))))

  SMAQC.dat <- subset(SMAQC.var,SMAQC.var$DatasetName%in%dataset)
  Names <- c("DatasetName","Dataset_ID","P_2C","MS1_2B","chromatog")
  new.data <- merge(RI_final,SMAQC.dat[,which(colnames(SMAQC.var)%in%Names)],by="Dataset_ID")
  new.data <- new.data[order(match(new.data$DatasetName,dataset)),]
  new.data$Fraction <- fraction
  
  
  if(def.baseline){
    baseline.data <- read.csv(default.baseline)
    baseline.data$chromatog <- subset(SMAQC.var$chromatog,SMAQC.var$DatasetName%in%baseline.data$DatasetName)
  }else{
    alternate <- read.csv(alt.baseline,stringsAsFactors = FALSE)
    alt.names <- lapply(alternate$DatasetName,function(x) load_data(x))
    temp.alt <- lapply(alt.names,function(x) fn(x[[1]]))
    alt.RI <- do.call(rbind,temp.alt)
    alt.SMAQC2 <- SMAQC.var[which(SMAQC.var$DatasetName%in%alternate$DatasetName),]
    alt.SMAQC <- subset(SMAQC.var,SMAQC.var$DatasetName%in%alternate$DatasetName)
    
    NamesForMerge <- c("DatasetName","Dataset_ID","P_2C","MS1_2B","RT_MS_Q1","RT_MS_Q4","RT_MSMS_Q1","RT_MSMS_Q4","chromatog")
    baseline.data <- merge(alt.RI,alt.SMAQC[,which(colnames(alt.SMAQC)%in%NamesForMerge )],by="Dataset_ID")
    baseline.data <- merge(baseline.data,alternate,by="DatasetName")
  }
  
  if(!def.baseline) {
    # Save a data file of the new baseline data
    write.table(baseline.data, "Baseline-New.csv", sep=",", row.names=FALSE, col.names=TRUE, na="") 
  }
  
  base.names <- c("DatasetName","Fraction","PIMZ_skew","BPMZ_skew","I119_skew","I121_median","I121_skew","WAPIC_skew",
                       "MissingIons","P_2C","MS1_2B","chromatog")
  
  all.data <- rbind(baseline.data[,which(colnames(baseline.data)%in%base.names)],
                    new.data[,which(colnames(new.data)%in%base.names)])

  all.data$chromatog <- unlist(lapply(all.data$chromatog,function(x) x+rnorm(1,mean=0,sd=.0001)))
  
  all.data$P_2C = as.numeric(as.character(all.data$P_2C))
  all.data$MS1_2B = as.numeric(gsub(",","",all.data$MS1_2B))
  
  #############################################
  ########### Statistics ######################
  #############################################
  
  # Set variables - pot_out shows potential outliers as determined by program, dist shows mahalanobis distance
  distances = matrix(nrow=length(fraction),ncol=1)
  rownames(distances) <- rownames(all.data)[1:length(fraction)]
  
  for(q in 1:length(fraction)){
    # Subset data to include only the relevant fraction for all sets
    Frac = subset(all.data,all.data$Fraction==fraction[q])
    stat.names <- base.names[-c(1,2)]
    Frac2 = Frac[,which(colnames(Frac)%in%stat.names)]
    # Perform multivariate outlier detection
    x.out = sign2(Frac2,makeplot = FALSE)
    rownames(distances)[q] = dataset[q]
    distances[q,1] = x.out$x.dist[nrow(Frac)]
  }
  return(distances)
}
  


distances <- QC_ART(dataset=dataset,fraction=fraction,def.baseline=def.baseline,alt.baseline=alt.baseline,MASIC=MASIC,SMAQC=SMAQC)

# Save the results to a tab-separated file
write.table(distances, "QC_ART_Results.txt", sep="\t", row.names=TRUE, col.names=FALSE, na="", quote=FALSE) 

