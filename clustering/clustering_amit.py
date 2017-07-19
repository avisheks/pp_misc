import sys


import matplotlib
# the following lines avoid the error:
# 	"RuntimeError: Invalid DISPLAY variable"
matplotlib.use('Agg') # Force matplotlib to not use any Xwindows backend
import matplotlib.pyplot as plt

import argparse as argp
import time

import pyspark as pysp


#===============================================================================
def cmd_parser():
	# Instantiate the parser
	parser = argp.ArgumentParser(description='Clustering options')
	
	# Required positional argument
	parser.add_argument('-q', '--qlas-input', type=str, help='QLAS query input path', required=True)
	parser.add_argument('-o', '--output-path', type=str, help='Output path', required=True)
	# Optional positional argument
	parser.add_argument('-m', '--min-clusters', type=int, help='Max number of clusters (default=100)', default=10)
	parser.add_argument('-k', '--max-clusters', type=int, help='Max number of clusters (default=100)', default=10)
	parser.add_argument('-p', '--out-partitions', type=int, help='Output file partitions (default=1)', default=1)
	parser.add_argument('-s', '--sampling-ratio', type=float, help='Sampling ratio (default=1)', default=1)
	
	return parser


#===============================================================================
def mllib_kmeans(inputData, minClusters, maxClusters):
	# --------------------------------------------------------------------------
	from numpy import array
	from math import sqrt
	from pyspark.mllib.clustering import KMeans
	
	# --------------------------------------------------------------------------
	# def conv2libsvm_mllib(input):
	# 	# query = input[1][0]
	# 	# vector = input[1][1].split(',')
	# 	# res = str(query) + '\t' + ' '.join(vector)
	# 	# return res
	# 	query = input.split('\t')[0]
	# 	vector = input[1].split(',')
	# 	#vector = array([float(i) for i in input.split('\t')[1].split(',')])
	# 	res = str(query) + '\t' + ' '.join(vector)
	# 	return vector
	  
	# --------------------------------------------------------------------------
	def error(point):
		center = model.centers[model.predict(point)]
		return sqrt(sum([x**2 for x in (point - center)]))
	 
	# --------------------------------------------------------------------------
	# inputData.cache()
	parsedData = inputData.map(lambda x: array([float(i) for i in x[1].split('\t')[3].split(',')]))
	parsedData.cache()
	print 'parsedData: ' + str(parsedData.count())
	#parsedData = data
	#parsedData.repartition(100).saveAsTextFile('/tmp/avishek2/output/qlas_cat_w2v' + '_parsedData')
	
	modelCosts = []
	for k in xrange(minClusters,maxClusters+1):
		model = KMeans.train(parsedData, k, maxIterations=10, initializationMode="random")
		wssse = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
		print("k:" + str(k) + "\tWithin Set Sum of Squared Errors = " + str(wssse))
		modelCosts.append(wssse)
	  
	finalK = modelCosts.index(min(modelCosts)) + minClusters
	model = KMeans.train(parsedData, finalK, maxIterations=10, initializationMode="random")
	#preds = model.predict(parsedData)
	
	# preds = parsedData.map(lambda x: (','.join(map(str,x.tolist())),model.predict(x)))
	preds = inputData.map(lambda x: (x[0],x[1], model.predict(array([float(i) for i in x[1].split('\t')[3].split(',')]))))
	#preds.saveAsTextFile('/tmp/avishek2/output/qlas_cat_w2v' + '_preds')
	preds.cache()
	print 'preds: ' + str(parsedData.count())
	
	# preds = preds.map(lambda x: (x[0], x[1]))
	# #output = data.map(lambda x: (x.split('\t')[0],x.split('\t')[1])).join(preds)
	# output = inputData\
	# 	.map(lambda x: (x.split('\t')[3].encode('UTF8'),x.encode('UTF8')))\
	# 	.leftOuterJoin(preds)
	#
	# print inputData.take(1)
	# print preds.take(1)
	# print output.take(1)
	# 	#.map(lambda x: x[1][0].encode('UTF8') + '\t' + str(x[1][1]))
	# 	##.map(lambda x: x[1][0].encode('UTF8') + '\t' + str(x[1][1]) + '\t' + x[0].encode('UTF8'))
	# #output.take(1)
	#
	# print 'output: ' + str(output.count())
	
	return preds


#==============================================================================
'''  
def mlpip_kmeans(inputData):
	# --------------------------------------------------------------------------
	from pyspark.ml.clustering import KMeans

	# --------------------------------------------------------------------------
	def conv2libsvm_ml(input):
		query = input[1][0]
		vector = input[1][1].split(',')
		idx = [i+1 for i in xrange(len(vector))]
		res = str(query) + '\t' + ' '.join(map(lambda x: str(x[0])+':'+str(x[1]),zip(idx,vector)))
		return res
	
	# Loads data.
	#dataset = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")
	dataset = spark.read.format("libsvm").load('kmeans_data')
	
	# Trains a k-means model.
	maxClusters = 10
	modelCosts = []
	for k in xrange(2,maxClusters+1):
	  kmeans = KMeans().setK(k).setSeed(1)
	  model = kmeans.fit(dataset)
	  wssse = model.computeCost(dataset)
	  print("k:" + str(k) + "\tWithin Set Sum of Squared Errors = " + str(wssse))
	  modelCosts.append(wssse)
	  
	finalK = modelCosts.index(min(modelCosts)) + 2
	kmeans = KMeans().setK(finalK).setSeed(1)
	model = kmeans.fit(dataset)
	transformed = model.transform(dataset).select("features", "prediction")
	
	# data = dataset.rdd
	# pred = transformed.rdd.zipWithIndex()
	# output = data.map(lambda x: (x[0],x[1:])).join(pred.map(lambda x: (x[1],x[0])))
	# output.saveAsTextFile('output')
	
	final_data = dataset.join(transformed, dataset.features == transformed.features).drop(transformed.features)
'''

#========================================================================
if __name__ == "__main__":

	sc = pysp.SparkContext(appName="kmeans_clustering")
	sc.setLogLevel("ERROR")

	### Read parameters
	parser = cmd_parser()
	args = parser.parse_args()
	
	qlas_input = args.qlas_input
	#w2v_input = args.w2v_input
	output = args.output_path
	minK = max(args.min_clusters,2)
	maxK = min(args.max_clusters,100)
	partitions = args.out_partitions
	sampling_ratio = max(0.1,min(args.sampling_ratio,1))


	# # # Read data
	# # qlasData = sc\
	# # 	.textFile(qlas_input) \
	# # 	.map(lambda x: x.encode('UTF8'))
	# # qlas_filt = qlasData\
	# # 	.filter(lambda x: float(x.split()[0]) > 0.7)\
	# # 	.map(lambda x: x.split()[1].replace('1~','').replace('_',' '))
	# # qlas_proj = qlas_filt.map(lambda x: (x,x))
	# #
	# #
	# # # --------------------------------------------------------------------------
	# # import re
	# # w2vData = sc \
	# # 	.textFile(w2v_input) \
	# # 	.map(lambda x: x.encode('UTF8'))
	# # w2vData_q_only = w2vData\
	# # 	.filter(lambda x: (not re.match(r'^adid_',x.split()[0])
     # #                        and not re.match(r'^slc_',x.split()[0])) )
	# # w2vData_q_only_kv = w2vData_q_only\
	# # 	.map(lambda x: (x.split()[0],x.split()[1]))
	# # joined = qlas_proj\
	# # 	.leftOuterJoin(w2vData_q_only_kv)\
	# # 	.map(lambda x:x[1])\
	# # 	.map(lambda x: str(x[0]) + '\t' + str(x[1]))
	# # joined.repartition(100).saveAsTextFile(output+'_joined')
	#
	#
	# joined = sc\
	# 	.textFile(output+'_joined/part*')
	# joined_training = joined\
	# 	.filter(lambda x: x.split('\t')[1] != 'None')
	# # joined_training.repartition(100).saveAsTextFile(output + '_joined_training')
	# joined_testing = joined \
	# 	.filter(lambda x: x.split('\t')[1] == 'None')
	#
	# # joined_testing.repartition(100).saveAsTextFile(output + '_joined_testing')
	# # joined_training = sc\
	# # 	.textFile('/tmp/avishek2/output/qlas_cat_w2v_joined_training/part*')
	
	joined = sc\
		.textFile(qlas_input)
	if (sampling_ratio < 1):
		print 'Sampling...'
		joined = joined.sample(False, sampling_ratio, int(round(time.time()*1000)))
	joined = joined\
		.zipWithIndex()\
		.map(lambda x: (x[1],x[0]))
	joined.saveAsTextFile(output + '_joined')
	# joined_training = joined\
	# 	.filter(lambda x: x.split('\t')[3] != 'None')
	# joined_training.saveAsTextFile(output + '_joined_training')
	# joined_testing = joined \
	# 	.filter(lambda x: x.split('\t')[1] == 'None')
	# joined_testing.saveAsTextFile(output + '_joined_testing')

	# joined_training = joined \
	# 	.filter(lambda x: True if len(x.split('\t')) == 4 else False)
	# joined_training = joined \
	# 	.map(lambda x: x.split('\t')[4])
	# joined_training.saveAsTextFile(output + '_joined_tmp')


	joined_training = joined \
		.filter(lambda x: True if x[1].split('\t')[3] != '' else False)
	print 'joined_training: ' + str(joined_training.count())
	joined_training.saveAsTextFile(output + '_joined_training')
	joined_testing = joined \
		.filter(lambda x: True if x[1].split('\t')[3] == '' else False)
	print 'joined_testing: ' + str(joined_testing.count())
	joined_testing.saveAsTextFile(output + '_joined_testing')


	# # joined_training = sc\
	# # 	.textFile('/tmp/avishek2/output/qlas_cat_w2v_joined_training/part*')
	#
	# #libsvmdata = joined.map(lambda x: conv2libsvm(x))
	# #libsvmdata_w_idx = libsvmdata.zipWithIndex().map(lambda x: str(x[1]+1) + '\t' +  str(x[0]))
	# #libsvmdata_w_idx.map(lambda x: x.split('\t')[0] + ' ' + x.split('\t')[2]).repartition(1).saveAsTextFile('kmeans_data')
	#
	# # joined_training.cache()
	# training_data = joined_training\
	# 	.map(lambda x: x.split('\t')[1] + '\t' + x.split('\t')[3])
	training_w_clusterid = mllib_kmeans(joined_training, minK, maxK)
	training_w_clusterid = training_w_clusterid \
		.map(lambda x: x[1].encode('UTF8') + '\t' + str(x[2]))
	training_w_clusterid.saveAsTextFile(output + '_training_w_clusterid')
	print 'training_w_clusterid: ' + str(training_w_clusterid.count())
	# .map(lambda x: str(x[0]) + '\t' + str(x[1]) + '\t' + str(x[2])) \


	# joined_testing = sc \
	# 	.textFile('/tmp/avishek2/output/qlas_cat_w2v_joined_testing/part*')
	#joined_testing.cache()
	testing = joined_testing\
		.map(lambda x: x[1].encode('UTF8') + '-1' + '\t' + '-1') #\
		#.map(lambda x: x.encode('UTF8'))
	testing.saveAsTextFile(output + '_testing')
	print 'testing: ' + str(testing.count())


	# # --------------------------------------------------------------------------
	out = training_w_clusterid.union(testing)
	outPartitioned = out.repartition(partitions)
	outPartitioned \
		.saveAsTextFile(output)