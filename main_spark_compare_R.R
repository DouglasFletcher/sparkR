

# ==========================================
# purpose: run r random forest regression on 
# give me credit dataset - sparkR compare R
# source: givemecredit
# https://www.kaggle.com/c/GiveMeSomeCredit
# random forest method
# Douglas Fletcher
# ==========================================

# global vars
if(!exists("DIRLOC")){
	DIRLOC <- getwd()
}

# global vars
TRAINING <- paste0(DIRLOC,"/in/cstraining_kaggle.csv")

# read sparkR setup
source(paste0(DIRLOC,"/sparkRSetup.R"))
options(scipen=999)

# libraries
library(randomForest)
library(foreach)
library(doMC)
library(SparkR)
library(magrittr)

# system tasks
sparkR.session(master="local[*]"
	, appName="appName"
	, sparkConfig = list(
		spark.driver.memory = "2g"
	)
)

# functions
source(paste0(DIRLOC,"/dataprep_model_functions.R"))

#===================
# prepare input data
#===================
prepSparkDataset <- function(DATAIN){
	# ==============================
	# purpose: prep model data spark
	# source: douglas fletcher
	# @params: 
	# 	input: DATAIN type: string
	# ==============================
	# read data
	print("reading rawdata...")
	indata <- getDataRSpark(DATAIN)
	# transform
	print("creating modelset...")
	# supress NA durch Umwandlung erzeugt
	trainTrans <- xFormDataSparkR(indata$training)
	# Exclude all rows with NAs
	print("subsetting modelset...")
	trainTransNonNAs <- na.omit(trainTrans)
	# return
	return(trainTransNonNAs)
}


#================
# training models
#================

sparkRRandomForest <- function(trainTransNonNAs){
	# =========================================
	# purpose: run random forest R
	# source: douglas fletcher
	# @params: 
	# 	input: trainTransNonNAs type: dataframe
	# =========================================
	# __init random forest input
	#NTREES <- rep(200, 16)
	NTREES <- rep(200, 3)
	# train model
	print("training model...")
	output <- buildRFSparkModel(trainTransNonNAs, NTREES)
	return(output)
}



#=====
# main
#=====

#__ run SparkR methods
ptm <- proc.time()
testsTransSparkNonNAs <- prepSparkDataset(TRAINING)
print("timing prepSparkDataset: ")
print(proc.time() - ptm)

print(names(testsTransSparkNonNAs))

ptm <- proc.time()
rSparkRandOutput <- sparkRRandomForest(testsTransSparkNonNAs)
print("timing sparkR randomForest: ")
print(proc.time() - ptm)
