mvn clean package
cp  ./fork-example.jar ./fork-workflow/lib/
hadoop fs -rmr /fork/fork-workflow
hadoop fs -copyFromLocal fork-workflow /fork
