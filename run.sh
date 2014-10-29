#!/bin/bash
export _JAVA_OPTIONS=-Xmx60G
jar='/export/home/mraza/Rwanda_MonthlyCOG/target/scala-2.10/monthlycog_2.10-0.1.jar'
file_suffix='-decrypted.txt'
hadoop_outdir_prefix='/user/mraza/Rwanda_Out/MobilityFiles/'
hadoop_outdir_suffix='*/part-*'

output_path='/export/home/mraza/Rwanda_Output/FixedCOG/'
output_suffix='_MonthlyCOG'

exec_obj_name='MainMonthlyCOG'

for month in  0606
do
   	echo "Trying jar $jar file $month$file_suffix ";
	
	spark-submit  --class $exec_obj_name --master yarn-client $jar $month$file_suffix --verbose;
   	echo "Executing export hadoop fs -cat $hadoop_outdir_prefix$month$hadoop_outdir_suffix>>$output_path$month$output_suffix "

	hadoop fs -cat $hadoop_outdir_prefix$month$hadoop_outdir_suffix>>$output_path$month$output_suffix;
done
