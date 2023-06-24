# !/bin/bash
echo '使用方法 sh sync.sh "ip1 ip2"'
echo "开始执行 $1 hadoop节点"
for ip in $1
do
	echo "开始拷贝依赖包到$ip 上"
	scp iceberg-spark-runtime-3.2_2.12-1.2.1.jar root@$ip:/usr/local/service/iceberg/
	echo "开始到$ip 上备份原有iceberg包"
	echo ”开始远程执行指令...“
    	ssh root@$ip "mv /usr/local/service/iceberg/iceberg-spark3-runtime-0.11.0.jar1 /usr/local/service/iceberg/iceberg-spark3-runtime-0.11.0.jar_bak"
  echo ”完成...“
done