$JAVA_HOME/bin/java -Dsandbox=false -jar evosuite-1.2.0.jar \
  -class "org.apache.bookkeeper.$1" \
  -projectCP "target/test-classes:target/classes:$(mvn dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q)"
